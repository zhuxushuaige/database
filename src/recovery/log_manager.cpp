/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include "log_manager.h"

namespace {
constexpr int kDefaultGroupCommitMaxWaitUs = 100;
constexpr int kMaxConfiguredGroupCommitWaitUs = 10000;
constexpr size_t kDefaultGroupCommitTargetWaiters = 8;
constexpr size_t kMaxConfiguredGroupCommitTargetWaiters = 1024;

inline void spin_pause() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

int GroupCommitMaxWaitUs() {
    static const int wait_us = [] {
        const char *raw = std::getenv("RMDB_GROUP_COMMIT_WAIT_US");
        if (raw == nullptr || raw[0] == '\0') {
            return kDefaultGroupCommitMaxWaitUs;
        }
        char *end = nullptr;
        long parsed = std::strtol(raw, &end, 10);
        if (end == raw || *end != '\0' || parsed < 0 || parsed > kMaxConfiguredGroupCommitWaitUs) {
            return kDefaultGroupCommitMaxWaitUs;
        }
        return static_cast<int>(parsed);
    }();
    return wait_us;
}

size_t GroupCommitTargetWaiters() {
    static const size_t target_waiters = [] {
        const char *raw = std::getenv("RMDB_GROUP_COMMIT_TARGET_WAITERS");
        if (raw == nullptr || raw[0] == '\0') {
            return kDefaultGroupCommitTargetWaiters;
        }
        char *end = nullptr;
        unsigned long parsed = std::strtoul(raw, &end, 10);
        if (end == raw || *end != '\0' || parsed == 0 || parsed > kMaxConfiguredGroupCommitTargetWaiters) {
            return kDefaultGroupCommitTargetWaiters;
        }
        return static_cast<size_t>(parsed);
    }();
    return target_waiters;
}

bool GroupCommitUsesAdaptivePolicy() {
    static const bool adaptive = [] {
        const char *explicit_policy = std::getenv("RMDB_GROUP_COMMIT_ADAPTIVE");
        if (explicit_policy != nullptr && explicit_policy[0] != '\0') {
            return std::strcmp(explicit_policy, "0") != 0;
        }
        // Explicit legacy knobs retain their exact fixed-policy meaning.
        return std::getenv("RMDB_GROUP_COMMIT_WAIT_US") == nullptr &&
               std::getenv("RMDB_GROUP_COMMIT_TARGET_WAITERS") == nullptr;
    }();
    return adaptive;
}
}

LogManager::LogManager(DiskManager *disk_manager) : disk_manager_(disk_manager) {
    persist_lsn_.store(INVALID_LSN, std::memory_order_relaxed);
    writer_thread_ = std::thread([this] { writer_loop(); });
}

LogManager::~LogManager() {
    try {
        flush_log_to_disk();
    } catch (...) {
        // WAL 写线程一旦失败便不可恢复，前台调用者已经收到该异常；析构仍须继续回收线程。
    }
    {
        std::lock_guard<std::mutex> lock(latch_);
        stop_writer_ = true;
        immediate_flush_requested_ = true;
    }
    writer_cv_.notify_one();
    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
}

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号（即该记录在日志文件中的字节偏移量）
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    std::unique_lock<std::mutex> lock(latch_);
    rethrow_writer_error_unlocked();
    if (log_record->log_tot_len_ > LOG_BUFFER_SIZE) {
        throw InternalError("WAL record exceeds log buffer size");
    }
    while (active_buffer_->is_full(log_record->log_tot_len_)) {
        immediate_flush_requested_ = true;
        writer_cv_.notify_one();
        flush_cv_.wait(lock, [&] {
            return !active_buffer_->is_full(log_record->log_tot_len_) || writer_error_ != nullptr;
        });
        rethrow_writer_error_unlocked();
    }

    // LSN 就是记录在当前 WAL 文件中的起始字节偏移，既用于顺序读取，也用于按事务链回溯。
    log_record->lsn_ = active_start_offset_ + active_buffer_->offset_;
    log_record->serialize(active_buffer_->buffer_ + active_buffer_->offset_);
    active_buffer_->offset_ += log_record->log_tot_len_;
    next_log_offset_ = active_start_offset_ + active_buffer_->offset_;
    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    lsn_t target_lsn = INVALID_LSN;
    {
        std::lock_guard<std::mutex> lock(latch_);
        target_lsn = next_log_offset_ - 1;
    }
    request_flush_and_wait(target_lsn, false);
}

void LogManager::flush_log_to_disk_until(lsn_t target_lsn) {
    request_flush_and_wait(target_lsn, false);
}

void LogManager::flush_log_to_disk_until_group(lsn_t target_lsn) {
    request_flush_and_wait(target_lsn, true);
}

void LogManager::request_flush_and_wait(lsn_t target_lsn, bool group_commit) {
    // 快速路径：写线程完成落盘后 persist_lsn_ 原子可见；先无锁自旋一小段，
    // 避免每次 flush 完成时 notify_all 惊动所有等待者去抢同一把 latch_。
    if (target_lsn != INVALID_LSN && target_lsn > persist_lsn_.load(std::memory_order_acquire)) {
        constexpr int kFlushSpinIterations = 400;  // ~40-80μs
        for (int i = 0; i < kFlushSpinIterations &&
                            target_lsn > persist_lsn_.load(std::memory_order_acquire);
             ++i) {
            spin_pause();
        }
    }
    std::unique_lock<std::mutex> lock(latch_);
    rethrow_writer_error_unlocked();
    if (target_lsn == INVALID_LSN || target_lsn <= persist_lsn_.load(std::memory_order_acquire)) {
        return;
    }
    // WAL 重置前已经保证全部数据页持久化，但页头仍可能保留上一代日志的 LSN。
    // 换出该页时若请求的偏移超出当前代末尾，应把它视作已经满足；继续等待会让写线程
    // 永远追赶一个当前 WAL 不可能到达的 requested_lsn_。
    if (target_lsn >= next_log_offset_) {
        return;
    }
    requested_lsn_ = std::max(requested_lsn_, target_lsn);
    if (group_commit) {
        ++group_waiter_count_;
    } else {
        immediate_flush_requested_ = true;
    }
    writer_cv_.notify_one();
    flush_cv_.wait(lock, [&] {
        return target_lsn <= persist_lsn_.load(std::memory_order_acquire) || writer_error_ != nullptr;
    });
    if (group_commit) {
        --group_waiter_count_;
    }
    rethrow_writer_error_unlocked();
}

void LogManager::writer_loop() noexcept {
    std::unique_lock<std::mutex> lock(latch_);
    while (true) {
        writer_cv_.wait(lock, [&] {
            return stop_writer_ || immediate_flush_requested_ ||
                   (requested_lsn_ != INVALID_LSN &&
                    requested_lsn_ > persist_lsn_.load(std::memory_order_acquire));
        });
        if (writer_error_ != nullptr) {
            return;
        }
        if (stop_writer_ && active_buffer_->offset_ == 0) {
            return;
        }
        if (active_buffer_->offset_ == 0) {
            if (stop_writer_) {
                return;
            }
            immediate_flush_requested_ = false;
            continue;
        }

        if (!stop_writer_ && !immediate_flush_requested_) {
            int wait_us = GroupCommitMaxWaitUs();
            size_t target_waiters = GroupCommitTargetWaiters();
            if (GroupCommitUsesAdaptivePolicy()) {
                // Wait at most roughly half of the observed fsync cost. Fast
                // storage favors latency; slower storage gives concurrent
                // committers a wider coalescing window. The previous group
                // size supplies a stable target and avoids reacting to one
                // transient waiter sample.
                wait_us = static_cast<int>(std::clamp<rmdb::u64>(adaptive_sync_ns_ / 2000, 20, 200));
                target_waiters = std::clamp<size_t>(adaptive_last_group_size_, 2, 16);
            }
            if (wait_us > 0 && group_waiter_count_ < target_waiters) {
                const auto deadline =
                    std::chrono::steady_clock::now() + std::chrono::microseconds(wait_us);
                writer_cv_.wait_until(lock, deadline, [&] {
                    return stop_writer_ || immediate_flush_requested_ ||
                           group_waiter_count_ >= target_waiters;
                });
            }
        }

        std::swap(active_buffer_, flush_buffer_);
        flush_start_offset_ = active_start_offset_;
        flush_size_ = flush_buffer_->offset_;
        active_start_offset_ = flush_start_offset_ + flush_size_;
        flush_in_progress_ = true;
        immediate_flush_requested_ = false;

        char *flush_data = flush_buffer_->buffer_;
        const int flush_size = flush_size_;
        const lsn_t flush_start = flush_start_offset_;
        const size_t flush_group_size = std::max<size_t>(1, group_waiter_count_);
        lock.unlock();
        const auto sync_begin = std::chrono::steady_clock::now();
        try {
            disk_manager_->write_log(flush_data, static_cast<size_t>(flush_size), flush_start);
            disk_manager_->sync_log();
        } catch (...) {
            lock.lock();
            writer_error_ = std::current_exception();
            flush_in_progress_ = false;
            flush_cv_.notify_all();
            return;
        }
        const auto sync_end = std::chrono::steady_clock::now();
        lock.lock();

        if (GroupCommitUsesAdaptivePolicy()) {
            const auto observed_ns = static_cast<rmdb::u64>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_begin).count());
            adaptive_sync_ns_ = (adaptive_sync_ns_ * 7 + observed_ns) / 8;
            adaptive_last_group_size_ = (adaptive_last_group_size_ * 3 + flush_group_size + 3) / 4;
        }

        persist_lsn_.store(flush_start + flush_size - 1, std::memory_order_release);
        flush_buffer_->reset();
        flush_size_ = 0;
        flush_in_progress_ = false;
        if (requested_lsn_ <= persist_lsn_.load(std::memory_order_relaxed)) {
            requested_lsn_ = INVALID_LSN;
        }
        flush_cv_.notify_all();
    }
}

void LogManager::rethrow_writer_error_unlocked() const {
    if (writer_error_ != nullptr) {
        std::rethrow_exception(writer_error_);
    }
}

lsn_t LogManager::get_log_file_offset() {
    std::lock_guard<std::mutex> lock(latch_);
    return next_log_offset_;
}

lsn_t LogManager::get_persist_lsn() {
    std::lock_guard<std::mutex> lock(latch_);
    return persist_lsn_.load(std::memory_order_relaxed);
}

void LogManager::reset_log_file_offset(lsn_t log_file_offset) {
    std::unique_lock<std::mutex> lock(latch_);
    rethrow_writer_error_unlocked();
    while (flush_in_progress_) {
        flush_cv_.wait(lock);
    }
    active_buffer_->reset();
    flush_buffer_->reset();
    active_start_offset_ = log_file_offset;
    next_log_offset_ = log_file_offset;
    persist_lsn_.store(log_file_offset > 0 ? log_file_offset - 1 : INVALID_LSN, std::memory_order_relaxed);
    requested_lsn_ = INVALID_LSN;
    immediate_flush_requested_ = false;
}
