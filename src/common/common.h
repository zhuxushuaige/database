/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"
#include "common/exception.h"

// ===================================================================
// types.h — 基础类型别名
// ===================================================================

namespace rmdb {

using i8 = std::int8_t;
using u8 = std::uint8_t;
using i16 = std::int16_t;
using u16 = std::uint16_t;
using i32 = std::int32_t;
using u32 = std::uint32_t;
using i64 = std::int64_t;
using u64 = std::uint64_t;

using isize = std::ptrdiff_t;
using usize = std::size_t;

static_assert(sizeof(i8) == 1);
static_assert(sizeof(i16) == 2);
static_assert(sizeof(i32) == 4);
static_assert(sizeof(i64) == 8);

// ===================================================================
// binary.h — 非对齐内存读写
// ===================================================================

// ===================================================================
// scope_exit.h — 作用域退出清理
// ===================================================================

template <typename Fn>
class ScopeExit {
   public:
    explicit ScopeExit(Fn fn) noexcept(std::is_nothrow_move_constructible_v<Fn>) : fn_(std::move(fn)) {}

    ScopeExit(const ScopeExit &) = delete;
    ScopeExit &operator=(const ScopeExit &) = delete;
    ScopeExit &operator=(ScopeExit &&) = delete;

    ScopeExit(ScopeExit &&other) noexcept(std::is_nothrow_move_constructible_v<Fn>)
        : fn_(std::move(other.fn_)), active_(std::exchange(other.active_, false)) {}

    ~ScopeExit() noexcept {
        if (active_) {
            std::invoke(fn_);
        }
    }

    void release() noexcept { active_ = false; }

   private:
    Fn fn_;
    bool active_{true};
};

template <typename Fn>
[[nodiscard]] auto make_scope_exit(Fn &&fn) {
    return ScopeExit<std::decay_t<Fn>>(std::forward<Fn>(fn));
}

// ===================================================================
// checked_atomic_counter.h — 原子计数器
// ===================================================================

namespace atomic_counter {

template <typename T>
inline void Increment(std::atomic<T> &counter, T amount = 1,
                      std::memory_order order = std::memory_order_relaxed) {
    counter.fetch_add(amount, order);
}

template <typename T>
inline void DecrementPositive(std::atomic<T> &counter, const char *name, T amount = 1,
                              std::memory_order success_order = std::memory_order_relaxed,
                              std::memory_order failure_order = std::memory_order_relaxed) {
    if (amount == 0) {
        return;
    }
    T current = counter.load(std::memory_order_relaxed);
    while (true) {
        if (current < amount) {
            throw InternalError(std::string(name) + " underflow");
        }
        if (counter.compare_exchange_weak(current, current - amount, success_order, failure_order)) {
            return;
        }
    }
}

}  // namespace atomic_counter

// ===================================================================
// lru_cache_store.h — 泛型 LRU 缓存
// ===================================================================

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class LruCacheStore {
   public:
    explicit LruCacheStore(size_t capacity) : capacity_(capacity) {}

    template <typename Validator>
    std::optional<Value> Lookup(const Key &key, Validator &&validator) {
        std::lock_guard<std::mutex> guard(latch_);
        auto iter = entries_.find(key);
        if (iter == entries_.end()) {
            return std::nullopt;
        }
        auto list_iter = iter->second;
        if (!validator(list_iter->value)) {
            lru_.erase(list_iter);
            entries_.erase(iter);
            return std::nullopt;
        }
        lru_.splice(lru_.begin(), lru_, list_iter);
        return lru_.front().value;
    }

    void Store(Key key, Value value) {
        std::lock_guard<std::mutex> guard(latch_);
        auto existing = entries_.find(key);
        if (existing != entries_.end()) {
            lru_.erase(existing->second);
            entries_.erase(existing);
        }
        lru_.push_front(Entry{std::move(key), std::move(value)});
        entries_[lru_.front().key] = lru_.begin();
        while (entries_.size() > capacity_) {
            auto last = std::prev(lru_.end());
            entries_.erase(last->key);
            lru_.pop_back();
        }
    }

    void Clear() {
        std::lock_guard<std::mutex> guard(latch_);
        entries_.clear();
        lru_.clear();
    }

    size_t SizeForTest() const {
        std::lock_guard<std::mutex> guard(latch_);
        return entries_.size();
    }

   private:
    struct Entry {
        Key key;
        Value value;
    };

    size_t capacity_;
    mutable std::mutex latch_;
    std::list<Entry> lru_;
    std::unordered_map<Key, typename std::list<Entry>::iterator, Hash> entries_;
};

// ===================================================================
// workload_feedback.h — 全局 epoch / 反馈生命周期
// ===================================================================

struct WorkloadFeedbackEpochSnapshot {
    rmdb::u64 data_epoch{0};
    rmdb::u64 feedback_generation{0};
};

class WorkloadFeedbackLifecycle {
   public:
    static WorkloadFeedbackLifecycle &Instance() {
        static WorkloadFeedbackLifecycle lifecycle;
        return lifecycle;
    }

    WorkloadFeedbackEpochSnapshot Snapshot() const {
        return WorkloadFeedbackEpochSnapshot{
            data_epoch_.load(std::memory_order_acquire),
            feedback_generation_.load(std::memory_order_acquire),
        };
    }

    void AdvanceDataGeneration() {
        data_epoch_.fetch_add(1, std::memory_order_acq_rel);
        feedback_generation_.fetch_add(1, std::memory_order_acq_rel);
    }

   private:
    WorkloadFeedbackLifecycle() = default;

    std::atomic<rmdb::u64> data_epoch_{1};
    std::atomic<rmdb::u64> feedback_generation_{1};
};

inline rmdb::u64 workload_data_epoch() {
    return WorkloadFeedbackLifecycle::Instance().Snapshot().data_epoch;
}

inline rmdb::u64 workload_feedback_generation() {
    return WorkloadFeedbackLifecycle::Instance().Snapshot().feedback_generation;
}

inline void advance_workload_data_generation() {
    WorkloadFeedbackLifecycle::Instance().AdvanceDataGeneration();
}

template <typename Key, typename Payload, typename Hash = std::hash<Key>>
class SynchronizedFeedbackRegistry {
   public:
    using StateMap = std::unordered_map<Key, Payload, Hash>;

    template <typename Fn>
    auto Read(Fn &&fn) const {
        std::lock_guard<std::mutex> guard(mutex_);
        return std::forward<Fn>(fn)(states_);
    }

    template <typename Fn>
    auto Update(Fn &&fn) {
        std::lock_guard<std::mutex> guard(mutex_);
        return std::forward<Fn>(fn)(states_);
    }

    StateMap Snapshot() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return states_;
    }

    void Clear() {
        std::lock_guard<std::mutex> guard(mutex_);
        states_.clear();
    }

   private:
    mutable std::mutex mutex_;
    StateMap states_;
};

// ===================================================================
// catalog_epoch.h — catalog epoch 封装
// ===================================================================

inline rmdb::u64 catalog_data_epoch() {
    return workload_data_epoch();
}

inline rmdb::u64 catalog_feedback_generation() {
    return workload_feedback_generation();
}

inline void advance_catalog_data_epoch() {
    advance_workload_data_generation();
}

// ===================================================================
// perf_stats.h — 性能统计
// ===================================================================

class RuntimePerfStats {
   public:
    RuntimePerfStats(std::string path, size_t buffer_pool_pages)
        : path_(std::move(path)), buffer_pool_pages_(buffer_pool_pages) {}

    void RecordBufferPoolHit() { buffer_pool_hits_.fetch_add(1, std::memory_order_relaxed); }
    void RecordBufferPoolMiss() { buffer_pool_misses_.fetch_add(1, std::memory_order_relaxed); }
    void RecordBufferPoolEviction() { buffer_pool_evictions_.fetch_add(1, std::memory_order_relaxed); }
    void RecordDirtyFlush() { dirty_flushes_.fetch_add(1, std::memory_order_relaxed); }
    void RecordPinFailure() { pin_failures_.fetch_add(1, std::memory_order_relaxed); }

    void RecordDiskRead(size_t bytes, bool wal) {
        disk_read_ops_.fetch_add(1, std::memory_order_relaxed);
        disk_read_bytes_.fetch_add(bytes, std::memory_order_relaxed);
        if (wal) {
            wal_read_ops_.fetch_add(1, std::memory_order_relaxed);
            wal_read_bytes_.fetch_add(bytes, std::memory_order_relaxed);
        }
    }

    void RecordDiskWrite(size_t bytes, bool wal) {
        disk_write_ops_.fetch_add(1, std::memory_order_relaxed);
        disk_write_bytes_.fetch_add(bytes, std::memory_order_relaxed);
        if (wal) {
            wal_write_ops_.fetch_add(1, std::memory_order_relaxed);
            wal_write_bytes_.fetch_add(bytes, std::memory_order_relaxed);
        }
    }

    void RecordWalReset(rmdb::u64 pause_ns, rmdb::u64 dirty_pages, rmdb::u64 discarded_bytes) {
        wal_reset_count_.fetch_add(1, std::memory_order_relaxed);
        wal_reset_dirty_pages_.fetch_add(dirty_pages, std::memory_order_relaxed);
        wal_reset_discarded_bytes_.fetch_add(discarded_bytes, std::memory_order_relaxed);
        rmdb::u64 observed = wal_reset_max_pause_ns_.load(std::memory_order_relaxed);
        while (pause_ns > observed &&
               !wal_reset_max_pause_ns_.compare_exchange_weak(
                   observed, pause_ns, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }

    bool Write() const {
        std::ofstream output(path_, std::ios::trunc);
        if (!output.is_open()) {
            return false;
        }
        output << "{\n"
               << "  \"buffer_pool_pages\": " << buffer_pool_pages_ << ",\n"
               << "  \"buffer_pool_hits\": " << Load(buffer_pool_hits_) << ",\n"
               << "  \"buffer_pool_misses\": " << Load(buffer_pool_misses_) << ",\n"
               << "  \"buffer_pool_evictions\": " << Load(buffer_pool_evictions_) << ",\n"
               << "  \"dirty_flushes\": " << Load(dirty_flushes_) << ",\n"
               << "  \"pin_failures\": " << Load(pin_failures_) << ",\n"
               << "  \"disk_read_ops\": " << Load(disk_read_ops_) << ",\n"
               << "  \"disk_read_bytes\": " << Load(disk_read_bytes_) << ",\n"
               << "  \"disk_write_ops\": " << Load(disk_write_ops_) << ",\n"
               << "  \"disk_write_bytes\": " << Load(disk_write_bytes_) << ",\n"
               << "  \"wal_read_ops\": " << Load(wal_read_ops_) << ",\n"
               << "  \"wal_read_bytes\": " << Load(wal_read_bytes_) << ",\n"
               << "  \"wal_write_ops\": " << Load(wal_write_ops_) << ",\n"
               << "  \"wal_write_bytes\": " << Load(wal_write_bytes_) << ",\n"
               << "  \"wal_reset_count\": " << Load(wal_reset_count_) << ",\n"
               << "  \"wal_reset_max_pause_ns\": " << Load(wal_reset_max_pause_ns_) << ",\n"
               << "  \"wal_reset_dirty_pages\": " << Load(wal_reset_dirty_pages_) << ",\n"
               << "  \"wal_reset_discarded_bytes\": " << Load(wal_reset_discarded_bytes_) << "\n"
               << "}\n";
        output.flush();
        return output.good();
    }

   private:
    static rmdb::u64 Load(const std::atomic<rmdb::u64> &counter) {
        return counter.load(std::memory_order_relaxed);
    }

    std::string path_;
    size_t buffer_pool_pages_;
    std::atomic<rmdb::u64> buffer_pool_hits_{0};
    std::atomic<rmdb::u64> buffer_pool_misses_{0};
    std::atomic<rmdb::u64> buffer_pool_evictions_{0};
    std::atomic<rmdb::u64> dirty_flushes_{0};
    std::atomic<rmdb::u64> pin_failures_{0};
    std::atomic<rmdb::u64> disk_read_ops_{0};
    std::atomic<rmdb::u64> disk_read_bytes_{0};
    std::atomic<rmdb::u64> disk_write_ops_{0};
    std::atomic<rmdb::u64> disk_write_bytes_{0};
    std::atomic<rmdb::u64> wal_read_ops_{0};
    std::atomic<rmdb::u64> wal_read_bytes_{0};
    std::atomic<rmdb::u64> wal_write_ops_{0};
    std::atomic<rmdb::u64> wal_write_bytes_{0};
    std::atomic<rmdb::u64> wal_reset_count_{0};
    std::atomic<rmdb::u64> wal_reset_max_pause_ns_{0};
    std::atomic<rmdb::u64> wal_reset_dirty_pages_{0};
    std::atomic<rmdb::u64> wal_reset_discarded_bytes_{0};
};

// ===================================================================
// runtime_feedback.h — 运行时反馈
// ===================================================================

constexpr rmdb::u32 kInvalidRuntimeNodeId = std::numeric_limits<rmdb::u32>::max();

enum class RuntimeNodeKind : rmdb::u8 {
    kOther = 0,
    kSeqScan,
    kIndexScan,
    kJoin,
    kCountIndex,
    kMinMaxIndex,
    kAggregate,
    kSort,
    kLimit,
    kDml,
};

struct RuntimeScanFeedback {
    std::atomic<rmdb::u64> executions{0};
    std::atomic<rmdb::u64> rows_scanned{0};
    std::atomic<rmdb::u64> rows_visible{0};
    std::atomic<rmdb::u64> rows_output{0};
    std::atomic<rmdb::u64> index_entries{0};
    std::atomic<rmdb::u64> heap_fetches{0};
    std::atomic<rmdb::u64> index_only_rows{0};
    std::atomic<rmdb::u64> elapsed_ns{0};
};

struct RuntimeJoinFeedback {
    std::atomic<rmdb::u64> executions{0};
    std::atomic<rmdb::u64> left_rows{0};
    std::atomic<rmdb::u64> right_rows{0};
    std::atomic<rmdb::u64> candidate_pairs{0};
    std::atomic<rmdb::u64> rows_output{0};
    std::atomic<rmdb::u64> elapsed_ns{0};
};

constexpr rmdb::u64 kScanFeedbackMinExecutions = 8;
constexpr rmdb::u64 kScanFeedbackWindowExecutions = 32;
constexpr double kScanFeedbackHighTableRatio = 0.30;
constexpr rmdb::u64 kScanFeedbackSmallTablePages = 8;

struct RuntimeScanFeedbackSnapshot {
    rmdb::u64 executions = 0;
    rmdb::u64 rows_scanned = 0;
    rmdb::u64 rows_visible = 0;
    rmdb::u64 rows_output = 0;
    rmdb::u64 index_entries = 0;
    rmdb::u64 heap_fetches = 0;
    rmdb::u64 index_only_rows = 0;
    rmdb::u64 elapsed_ns = 0;
};

inline RuntimeScanFeedbackSnapshot scan_feedback_delta(const RuntimeScanFeedbackSnapshot &current,
                                                        const RuntimeScanFeedbackSnapshot &baseline) {
    auto subtract = [](rmdb::u64 value, rmdb::u64 base) { return value >= base ? value - base : value; };
    return RuntimeScanFeedbackSnapshot{
        subtract(current.executions, baseline.executions),
        subtract(current.rows_scanned, baseline.rows_scanned),
        subtract(current.rows_visible, baseline.rows_visible),
        subtract(current.rows_output, baseline.rows_output),
        subtract(current.index_entries, baseline.index_entries),
        subtract(current.heap_fetches, baseline.heap_fetches),
        subtract(current.index_only_rows, baseline.index_only_rows),
        subtract(current.elapsed_ns, baseline.elapsed_ns),
    };
}

struct RuntimeScanPolicyState {
    std::mutex mutex;
    RuntimeScanFeedbackSnapshot baseline;
    bool has_decision{false};
    bool prefer_seq_scan{false};
};

struct RuntimeNodeFeedback {
    RuntimeNodeKind kind = RuntimeNodeKind::kOther;
    rmdb::u32 node_id = kInvalidRuntimeNodeId;
    rmdb::u64 generation = 0;
    RuntimeScanFeedback scan;
    RuntimeJoinFeedback join;
    RuntimeScanPolicyState scan_policy;

    RuntimeNodeFeedback(RuntimeNodeKind kind_, rmdb::u32 node_id_, rmdb::u64 generation_ = 0)
        : kind(kind_), node_id(node_id_), generation(generation_) {}
};

struct RuntimeFeedbackStore {
    explicit RuntimeFeedbackStore(rmdb::u64 generation_ = workload_feedback_generation())
        : generation(generation_) {}

    rmdb::u64 generation = 0;
    std::vector<std::shared_ptr<RuntimeNodeFeedback>> nodes;

    std::shared_ptr<RuntimeNodeFeedback> get(rmdb::u32 node_id) const {
        if (node_id >= nodes.size()) {
            return nullptr;
        }
        return nodes[node_id];
    }

    std::shared_ptr<RuntimeNodeFeedback> ensure(rmdb::u32 node_id, RuntimeNodeKind kind) {
        if (nodes.size() <= node_id) {
            nodes.resize(static_cast<size_t>(node_id) + 1);
        }
        if (nodes[node_id] == nullptr) {
            nodes[node_id] = std::make_shared<RuntimeNodeFeedback>(kind, node_id, generation);
        }
        return nodes[node_id];
    }
};

inline void record_scan_feedback(const std::shared_ptr<RuntimeNodeFeedback> &feedback,
                                 rmdb::u64 rows_scanned,
                                 rmdb::u64 rows_visible,
                                 rmdb::u64 rows_output,
                                 rmdb::u64 index_entries,
                                 rmdb::u64 heap_fetches,
                                 rmdb::u64 index_only_rows,
                                 rmdb::u64 elapsed_ns) {
    if (feedback == nullptr) {
        return;
    }
    feedback->scan.executions.fetch_add(1, std::memory_order_relaxed);
    feedback->scan.rows_scanned.fetch_add(rows_scanned, std::memory_order_relaxed);
    feedback->scan.rows_visible.fetch_add(rows_visible, std::memory_order_relaxed);
    feedback->scan.rows_output.fetch_add(rows_output, std::memory_order_relaxed);
    feedback->scan.index_entries.fetch_add(index_entries, std::memory_order_relaxed);
    feedback->scan.heap_fetches.fetch_add(heap_fetches, std::memory_order_relaxed);
    feedback->scan.index_only_rows.fetch_add(index_only_rows, std::memory_order_relaxed);
    feedback->scan.elapsed_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
}

inline void record_join_feedback(const std::shared_ptr<RuntimeNodeFeedback> &feedback,
                                 rmdb::u64 left_rows,
                                 rmdb::u64 right_rows,
                                 rmdb::u64 candidate_pairs,
                                 rmdb::u64 rows_output,
                                 rmdb::u64 elapsed_ns) {
    if (feedback == nullptr || feedback->kind != RuntimeNodeKind::kJoin) {
        return;
    }
    feedback->join.executions.fetch_add(1, std::memory_order_relaxed);
    feedback->join.left_rows.fetch_add(left_rows, std::memory_order_relaxed);
    feedback->join.right_rows.fetch_add(right_rows, std::memory_order_relaxed);
    feedback->join.candidate_pairs.fetch_add(candidate_pairs, std::memory_order_relaxed);
    feedback->join.rows_output.fetch_add(rows_output, std::memory_order_relaxed);
    feedback->join.elapsed_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
}

class RuntimeJoinFeedbackCollector {
   public:
    RuntimeJoinFeedbackCollector() = default;
    explicit RuntimeJoinFeedbackCollector(std::shared_ptr<RuntimeNodeFeedback> feedback) {
        bind(std::move(feedback));
    }
    ~RuntimeJoinFeedbackCollector() { flush(); }

    RuntimeJoinFeedbackCollector(const RuntimeJoinFeedbackCollector &) = delete;
    RuntimeJoinFeedbackCollector &operator=(const RuntimeJoinFeedbackCollector &) = delete;

    void bind(std::shared_ptr<RuntimeNodeFeedback> feedback) {
        flush();
        feedback_ = std::move(feedback);
    }

    void begin() {
        flush();
        counters_ = {};
        active_ = feedback_ != nullptr && feedback_->kind == RuntimeNodeKind::kJoin;
        flushed_ = false;
        if (active_) {
            started_at_ = std::chrono::steady_clock::now();
        }
    }

    void add_left_rows(rmdb::u64 count = 1) {
        if (active_) counters_.left_rows += count;
    }
    void add_right_rows(rmdb::u64 count = 1) {
        if (active_) counters_.right_rows += count;
    }
    void add_candidate_pairs(rmdb::u64 count = 1) {
        if (active_) counters_.candidate_pairs += count;
    }
    void add_rows_output(rmdb::u64 count = 1) {
        if (active_) counters_.rows_output += count;
    }

    void flush() {
        if (!active_ || flushed_) {
            return;
        }
        flushed_ = true;
        auto elapsed_ns = static_cast<rmdb::u64>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at_)
                .count());
        record_join_feedback(feedback_, counters_.left_rows, counters_.right_rows,
                             counters_.candidate_pairs, counters_.rows_output, elapsed_ns);
    }

   private:
    struct Counters {
        rmdb::u64 left_rows{0};
        rmdb::u64 right_rows{0};
        rmdb::u64 candidate_pairs{0};
        rmdb::u64 rows_output{0};
    };

    std::shared_ptr<RuntimeNodeFeedback> feedback_;
    Counters counters_;
    std::chrono::steady_clock::time_point started_at_;
    bool active_{false};
    bool flushed_{true};
};

class RuntimeScanFeedbackCollector {
   public:
    RuntimeScanFeedbackCollector() = default;

    RuntimeScanFeedbackCollector(std::shared_ptr<RuntimeNodeFeedback> feedback,
                                 RuntimeNodeKind expected_kind) {
        bind(std::move(feedback), expected_kind);
    }

    ~RuntimeScanFeedbackCollector() { flush(); }

    RuntimeScanFeedbackCollector(const RuntimeScanFeedbackCollector &) = delete;
    RuntimeScanFeedbackCollector &operator=(const RuntimeScanFeedbackCollector &) = delete;

    void bind(std::shared_ptr<RuntimeNodeFeedback> feedback, RuntimeNodeKind expected_kind) {
        flush();
        feedback_ = std::move(feedback);
        expected_kind_ = expected_kind;
    }

    void begin() {
        flush();
        counters_ = {};
        active_ = feedback_ != nullptr && feedback_->kind == expected_kind_;
        flushed_ = false;
        if (active_) {
            started_at_ = std::chrono::steady_clock::now();
        }
    }

    void add_rows_scanned(rmdb::u64 count = 1) {
        if (active_) counters_.rows_scanned += count;
    }
    void add_rows_visible(rmdb::u64 count = 1) {
        if (active_) counters_.rows_visible += count;
    }
    void add_rows_output(rmdb::u64 count = 1) {
        if (active_) counters_.rows_output += count;
    }
    void add_index_entries(rmdb::u64 count = 1) {
        if (active_) counters_.index_entries += count;
    }
    void add_heap_fetches(rmdb::u64 count = 1) {
        if (active_) counters_.heap_fetches += count;
    }
    void add_index_only_rows(rmdb::u64 count = 1) {
        if (active_) counters_.index_only_rows += count;
    }

    void flush() {
        if (!active_ || flushed_) {
            return;
        }
        flushed_ = true;
        auto elapsed_ns = static_cast<rmdb::u64>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at_)
                .count());
        record_scan_feedback(feedback_, counters_.rows_scanned, counters_.rows_visible,
                             counters_.rows_output, counters_.index_entries, counters_.heap_fetches,
                             counters_.index_only_rows, elapsed_ns);
    }

   private:
    struct Counters {
        rmdb::u64 rows_scanned{0};
        rmdb::u64 rows_visible{0};
        rmdb::u64 rows_output{0};
        rmdb::u64 index_entries{0};
        rmdb::u64 heap_fetches{0};
        rmdb::u64 index_only_rows{0};
    };

    std::shared_ptr<RuntimeNodeFeedback> feedback_;
    RuntimeNodeKind expected_kind_{RuntimeNodeKind::kOther};
    Counters counters_;
    std::chrono::steady_clock::time_point started_at_;
    bool active_{false};
    bool flushed_{true};
};

inline RuntimeScanFeedbackSnapshot load_scan_feedback_snapshot(
    const std::shared_ptr<RuntimeNodeFeedback> &feedback) {
    RuntimeScanFeedbackSnapshot snapshot;
    if (feedback == nullptr) {
        return snapshot;
    }
    snapshot.executions = feedback->scan.executions.load(std::memory_order_relaxed);
    snapshot.rows_scanned = feedback->scan.rows_scanned.load(std::memory_order_relaxed);
    snapshot.rows_visible = feedback->scan.rows_visible.load(std::memory_order_relaxed);
    snapshot.rows_output = feedback->scan.rows_output.load(std::memory_order_relaxed);
    snapshot.index_entries = feedback->scan.index_entries.load(std::memory_order_relaxed);
    snapshot.heap_fetches = feedback->scan.heap_fetches.load(std::memory_order_relaxed);
    snapshot.index_only_rows = feedback->scan.index_only_rows.load(std::memory_order_relaxed);
    snapshot.elapsed_ns = feedback->scan.elapsed_ns.load(std::memory_order_relaxed);
    return snapshot;
}

}  // namespace rmdb

// ===================================================================
// 原始 common.h 内容 — TabCol, Value, Condition, SetClause
// ===================================================================

struct TabCol {
    std::string tab_name;
    std::string col_name;
    std::string output_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        float float_val;  // float value
    };
    std::string str_val;  // string value

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        write_raw_data(len);
    }

    void init_raw(int len, std::shared_ptr<RmRecord> raw_buffer) {
        assert(raw == nullptr);
        raw = std::move(raw_buffer);
        raw->Resize(len);
        write_raw_data(len);
    }

private:
    void write_raw_data(int len) {
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            rmdb::store_unaligned<int>(raw->data, int_val);
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            rmdb::store_unaligned<float>(raw->data, float_val);
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
};

enum class SetOp { ASSIGN, ADD, SUB, MUL, DIV };

struct SetArithmeticStep {
    SetOp op{SetOp::ASSIGN};
    Value rhs;
};

struct SetClause {
    TabCol lhs;
    SetOp op{SetOp::ASSIGN};
    bool rhs_is_col{false};
    TabCol rhs_col;
    bool rhs_has_val{false};
    Value rhs;
    bool reversed_operands{false};  // true for e.g. SET col = 10 - col
    std::vector<SetArithmeticStep> extra_steps;
};
