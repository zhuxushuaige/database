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

#include <atomic>
#include <condition_variable>
#include <array>
#include <exception>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <iostream>
#include "log_defs.h"
#include "common/config.h"
#include "common/common.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型。显式值属于持久化日志格式，不可重排。 */
enum class LogType : rmdb::i32 {
    kUpdate = 0,
    kInsert = 1,
    kDelete = 2,
    kBegin = 3,
    kCommit = 4,
    kAbort = 5,
    kCheckpoint = 6,
};

inline constexpr std::array<std::string_view, 7> kLogTypeNames{{
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "CHECKPOINT",
}};

static_assert(kLogTypeNames.size() == static_cast<rmdb::usize>(LogType::kCheckpoint) + 1);

inline constexpr std::string_view log_type_name(LogType type) noexcept {
    const auto index = static_cast<rmdb::usize>(type);
    return index < kLogTypeNames.size() ? kLogTypeNames[index] : "UNKNOWN";
}

static_assert(sizeof(LogType) == sizeof(rmdb::i32));

class LogRecord {
public:
    virtual ~LogRecord() = default;

    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    rmdb::u32 log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(rmdb::u32));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        memcpy(&log_type_, src + OFFSET_LOG_TYPE, sizeof(LogType));
        memcpy(&lsn_, src + OFFSET_LSN, sizeof(lsn_t));
        memcpy(&log_tot_len_, src + OFFSET_LOG_TOT_LEN, sizeof(rmdb::u32));
        memcpy(&log_tid_, src + OFFSET_LOG_TID, sizeof(txn_id_t));
        memcpy(&prev_lsn_, src + OFFSET_PREV_LSN, sizeof(lsn_t));
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << log_type_name(log_type_) << "\n";
        std::cout << "Print Log Record:\n";
        std::cout << "log_type_: " << log_type_name(log_type_) << "\n";
        std::cout << "lsn: " << lsn_ << "\n";
        std::cout << "log_tot_len: " << log_tot_len_ << "\n";
        std::cout << "log_tid: " << log_tid_ << "\n";
        std::cout << "prev_lsn: " << prev_lsn_ << "\n";
    }
};

class BeginLogRecord: public LogRecord {
public:
    BeginLogRecord() {
        log_type_ = LogType::kBegin;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
        log_tid_ = txn_id;
    }
    // 序列化Begin日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Begin日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);   
    }
    virtual void format_print() override {
        std::cout << "log type in son_function: " << log_type_name(log_type_) << "\n";
        LogRecord::format_print();
    }
};

class CommitLogRecord: public LogRecord {
public:
    CommitLogRecord() {
        log_type_ = LogType::kCommit;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() {
        log_tid_ = txn_id;
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    void format_print() override {
        printf("commit record\n");
        LogRecord::format_print();
    }
};

class AbortLogRecord: public LogRecord {
public:
    AbortLogRecord() {
        log_type_ = LogType::kAbort;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    AbortLogRecord(txn_id_t txn_id) : AbortLogRecord() {
        log_tid_ = txn_id;
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    void format_print() override {
        printf("abort record\n");
        LogRecord::format_print();
    }
};

struct CheckpointTxnInfo {
    txn_id_t txn_id_{INVALID_TXN_ID};
    lsn_t last_lsn_{INVALID_LSN};
};

class CheckpointLogRecord: public LogRecord {
public:
    CheckpointLogRecord() {
        log_type_ = LogType::kCheckpoint;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        active_txn_count_ = 0;
    }
    CheckpointLogRecord(const std::vector<CheckpointTxnInfo>& active_txns) : CheckpointLogRecord() {
        active_txn_count_ = static_cast<int>(active_txns.size());
        active_txns_ = active_txns;
        log_tot_len_ += sizeof(int);
        if (active_txn_count_ > 0) {
            log_tot_len_ += active_txn_count_ * (sizeof(txn_id_t) + sizeof(lsn_t));
        }
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &active_txn_count_, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < active_txn_count_; ++i) {
            memcpy(dest + offset, &active_txns_[i].txn_id_, sizeof(txn_id_t));
            offset += sizeof(txn_id_t);
            memcpy(dest + offset, &active_txns_[i].last_lsn_, sizeof(lsn_t));
            offset += sizeof(lsn_t);
        }
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        memcpy(&active_txn_count_, src + offset, sizeof(int));
        offset += sizeof(int);
        active_txns_.resize(active_txn_count_);
        for (int i = 0; i < active_txn_count_; ++i) {
            memcpy(&active_txns_[i].txn_id_, src + offset, sizeof(txn_id_t));
            offset += sizeof(txn_id_t);
            memcpy(&active_txns_[i].last_lsn_, src + offset, sizeof(lsn_t));
            offset += sizeof(lsn_t);
        }
    }
    void format_print() override {
        printf("checkpoint record\n");
        LogRecord::format_print();
        printf("active_txn_count: %d\n", active_txn_count_);
    }
    int active_txn_count_;
    std::vector<CheckpointTxnInfo> active_txns_;   // 检查点时刻活跃事务及其最近日志位置
};

class InsertLogRecord: public LogRecord {
public:
    InsertLogRecord() {
        log_type_ = LogType::kInsert;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    InsertLogRecord(txn_id_t txn_id, const RmRecord& insert_value, const Rid& rid, const std::string& table_name)
        : InsertLogRecord() {
        log_tid_ = txn_id;
        insert_value_view_ = &insert_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += insert_value.size;
        log_tot_len_ += sizeof(Rid);
        table_name_ = table_name;
        log_tot_len_ += sizeof(size_t) + table_name_.size();
    }

    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        const RmRecord &insert_value = insert_value_view_ == nullptr ? insert_value_ : *insert_value_view_;
        memcpy(dest + offset, &insert_value.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, insert_value.data, insert_value.size);
        offset += insert_value.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = table_name_.size();
        memcpy(dest + offset, &table_name_size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        insert_value_view_ = nullptr;
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        int record_size = 0;
        memcpy(&record_size, src + offset, sizeof(int));
        offset += sizeof(int);
        insert_value_.ResizeAndCopy(src + offset, record_size);
        offset += record_size;
        memcpy(&rid_, src + offset, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = 0;
        memcpy(&table_name_size, src + offset, sizeof(size_t));
        offset += sizeof(size_t);
        table_name_.assign(src + offset, table_name_size);
    }
    void format_print() override {
        printf("insert record\n");
        LogRecord::format_print();
        const RmRecord &insert_value = insert_value_view_ == nullptr ? insert_value_ : *insert_value_view_;
        printf("insert_value: %s\n", insert_value.data);
        printf("insert rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_.c_str());
    }

    RmRecord insert_value_;     // 插入的记录
    const RmRecord *insert_value_view_{nullptr};  // add_log_to_buffer同步序列化期间借用
    Rid rid_;                   // 记录插入的位置
    std::string table_name_;    // 插入记录的表名称
};

class DeleteLogRecord: public LogRecord {
public:
    DeleteLogRecord() {
        log_type_ = LogType::kDelete;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    DeleteLogRecord(txn_id_t txn_id, const RmRecord& old_record, const Rid& rid, const std::string& table_name)
        : DeleteLogRecord() {
        log_tid_ = txn_id;
        old_record_view_ = &old_record;
        rid_ = rid;
        log_tot_len_ += sizeof(int) + old_record.size;
        log_tot_len_ += sizeof(Rid);
        table_name_ = table_name;
        log_tot_len_ += sizeof(size_t) + table_name_.size();
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        const RmRecord &old_record = old_record_view_ == nullptr ? old_record_ : *old_record_view_;
        memcpy(dest + offset, &old_record.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, old_record.data, old_record.size);
        offset += old_record.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = table_name_.size();
        memcpy(dest + offset, &table_name_size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size);
    }
    void deserialize(const char* src) override {
        old_record_view_ = nullptr;
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        int record_size = 0;
        memcpy(&record_size, src + offset, sizeof(int));
        offset += sizeof(int);
        old_record_.ResizeAndCopy(src + offset, record_size);
        offset += record_size;
        memcpy(&rid_, src + offset, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = 0;
        memcpy(&table_name_size, src + offset, sizeof(size_t));
        offset += sizeof(size_t);
        table_name_.assign(src + offset, table_name_size);
    }
    void format_print() override {
        printf("delete record\n");
        LogRecord::format_print();
        printf("delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_.c_str());
    }
    RmRecord old_record_;       // 被删除的记录（用于undo）
    const RmRecord *old_record_view_{nullptr};  // add_log_to_buffer同步序列化期间借用
    Rid rid_;                   // 记录的位置
    std::string table_name_;
};

class UpdateLogRecord: public LogRecord {
public:
    UpdateLogRecord() {
        log_type_ = LogType::kUpdate;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    UpdateLogRecord(txn_id_t txn_id, const RmRecord& old_record, const RmRecord& new_record, const Rid& rid,
                    const std::string& table_name)
        : UpdateLogRecord() {
        log_tid_ = txn_id;
        old_record_view_ = &old_record;
        new_record_view_ = &new_record;
        rid_ = rid;
        log_tot_len_ += sizeof(int) + old_record.size;
        log_tot_len_ += sizeof(int) + new_record.size;
        log_tot_len_ += sizeof(Rid);
        table_name_ = table_name;
        log_tot_len_ += sizeof(size_t) + table_name_.size();
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        const RmRecord &old_record = old_record_view_ == nullptr ? old_record_ : *old_record_view_;
        const RmRecord &new_record = new_record_view_ == nullptr ? new_record_ : *new_record_view_;
        memcpy(dest + offset, &old_record.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, old_record.data, old_record.size);
        offset += old_record.size;
        memcpy(dest + offset, &new_record.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, new_record.data, new_record.size);
        offset += new_record.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = table_name_.size();
        memcpy(dest + offset, &table_name_size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size);
    }
    void deserialize(const char* src) override {
        old_record_view_ = nullptr;
        new_record_view_ = nullptr;
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        int old_record_size = 0;
        memcpy(&old_record_size, src + offset, sizeof(int));
        offset += sizeof(int);
        old_record_.ResizeAndCopy(src + offset, old_record_size);
        offset += old_record_size;
        int new_record_size = 0;
        memcpy(&new_record_size, src + offset, sizeof(int));
        offset += sizeof(int);
        new_record_.ResizeAndCopy(src + offset, new_record_size);
        offset += new_record_size;
        memcpy(&rid_, src + offset, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = 0;
        memcpy(&table_name_size, src + offset, sizeof(size_t));
        offset += sizeof(size_t);
        table_name_.assign(src + offset, table_name_size);
    }
    void format_print() override {
        printf("update record\n");
        LogRecord::format_print();
        printf("update rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_.c_str());
    }
    RmRecord old_record_;       // 更新前的记录（用于undo）
    RmRecord new_record_;       // 更新后的记录（用于redo）
    const RmRecord *old_record_view_{nullptr};  // add_log_to_buffer同步序列化期间借用
    const RmRecord *new_record_view_{nullptr};
    Rid rid_;                   // 记录的位置
    std::string table_name_;
};

/* 单个日志缓冲区；LogManager 使用两个实例轮换接收前台写入和后台刷盘。 */

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    [[nodiscard]] bool is_full(int append_size) const { return offset_ + append_size > LOG_BUFFER_SIZE; }

    void reset() { offset_ = 0; }

    char buffer_[LOG_BUFFER_SIZE + 1];
    int offset_;  // 下一条日志在缓冲区内的写入偏移
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
/**
 * Buffered WAL manager with a dedicated writer and group-commit support.
 *
 * Foreground transactions wait for persist_lsn_ to cover their target LSN.
 * The writer publishes persist_lsn_ only after write_log() and sync_log()
 * complete, so a successful COMMIT response has auditable stable storage.
 */
class LogManager {
public:
    explicit LogManager(DiskManager* disk_manager);
    ~LogManager();

    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk();
    void flush_log_to_disk_until(lsn_t target_lsn);
    void flush_log_to_disk_until_group(lsn_t target_lsn);
    lsn_t get_log_file_offset();
    lsn_t get_persist_lsn();
    void reset_log_file_offset(lsn_t log_file_offset);

private:
    void request_flush_and_wait(lsn_t target_lsn, bool group_commit);
    void writer_loop() noexcept;
    void rethrow_writer_error_unlocked() const;

    std::mutex latch_;  // 保护双缓冲切换及以下刷盘状态
    std::condition_variable flush_cv_;
    std::condition_variable writer_cv_;
    LogBuffer log_buffers_[2];  // active buffer 接收新日志，flush buffer 负责当前写盘
    LogBuffer *active_buffer_{&log_buffers_[0]};
    LogBuffer *flush_buffer_{&log_buffers_[1]};
    std::atomic<lsn_t> persist_lsn_{INVALID_LSN};  // 已持久化的最后一个字节偏移；原子可见，等待者可无锁自旋
    lsn_t active_start_offset_{0};  // active buffer 首字节对应的 WAL 文件偏移
    lsn_t next_log_offset_{0};      // 下一条日志记录的 WAL 文件偏移
    lsn_t flush_start_offset_{0};
    int flush_size_{0};
    bool flush_in_progress_{false};
    bool immediate_flush_requested_{false};
    bool stop_writer_{false};
    size_t group_waiter_count_{0};
    size_t adaptive_last_group_size_{8};
    rmdb::u64 adaptive_sync_ns_{100000};
    lsn_t requested_lsn_{INVALID_LSN};
    std::exception_ptr writer_error_;
    DiskManager* disk_manager_;
    std::thread writer_thread_;
}; 
