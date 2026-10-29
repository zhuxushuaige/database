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
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unordered_set>

#include "common/common.h"
#include "transaction/txn_defs.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

/** 表示此tuple的前一个版本的链接 */
struct UndoLink {
    /* 之前的版本可以在其中的事务中找到 */
    txn_id_t prev_txn_{INVALID_TXN_ID};
    /* 在 `prev_txn_` 中前一个版本的日志索引 */
    int prev_log_idx_{0};

    friend auto operator==(const UndoLink& a, const UndoLink& b) {
        return a.prev_txn_ == b.prev_txn_ && a.prev_log_idx_ == b.prev_log_idx_;
    }

    friend auto operator!=(const UndoLink& a, const UndoLink& b) {
        return !(a == b);
    }

    /* 是否实际指向一条撤销日志。 */
    bool IsValid() const {
        return prev_txn_ != INVALID_TXN_ID;
    }
};

struct UndoLog {
    /* 此日志是否为删除标记 */
    bool is_deleted_{false};
    /* 此撤销日志修改的字段 */
    std::vector<bool> modified_fields_;
    /* 修改后的字段 */
    std::vector<Value> tuple_;
    /* 旧元组的完整字节镜像；shared_ptr 允许版本链读者跨执行器生命周期持有。 */
    std::shared_ptr<RmRecord> tuple_image_{nullptr};
    /* 此撤销日志的时间戳 */
    timestamp_t ts_{INVALID_TS};
    /* 撤销日志的前一个版本 */
    UndoLink prev_version_{};
};

class Transaction {
public:
    struct TupleWriteSummary {
        std::shared_ptr<const std::string> tab_name;
        Rid rid;
        bool is_deleted{false};
        bool publish_version{false};
    };
    struct PredicateRead {
        std::string tab_name;
        std::vector<ColMeta> cols;
        std::vector<Condition> conds;
    };
    struct SerializableWriteInfo {
        std::string tab_name;
        Rid rid;
        std::vector<ColMeta> cols;
        std::shared_ptr<RmRecord> old_record;
        std::shared_ptr<RmRecord> new_record;
    };
    explicit Transaction(txn_id_t txn_id,
                         IsolationLevel isolation_level = IsolationLevel::SNAPSHOT_ISOLATION)
        : isolation_level_(isolation_level), txn_id_(txn_id) {}

    ~Transaction() = default;

    inline txn_id_t get_transaction_id() const {
        return txn_id_;
    }
    inline void set_transaction_id(txn_id_t txn_id) {
        txn_id_ = txn_id;
    }

    inline std::thread::id get_thread_id() const {
        return thread_id_;
    }

    inline void set_txn_mode(bool txn_mode) {
        txn_mode_ = txn_mode;
    }
    inline bool get_txn_mode() const {
        return txn_mode_;
    }

    inline void set_lightweight_read_only(bool value) {
        lightweight_read_only_ = value;
    }
    inline bool is_lightweight_read_only() const {
        return lightweight_read_only_;
    }

    inline void set_start_ts(timestamp_t start_ts) {
        start_ts_ = start_ts;
    }
    inline timestamp_t get_start_ts() const {
        return start_ts_;
    }

    inline IsolationLevel get_isolation_level() const {
        return isolation_level_;
    }
    inline void set_isolation_level(IsolationLevel isolation_level) {
        isolation_level_ = isolation_level;
    }

    inline bool watermark_registered() const {
        return watermark_registered_;
    }
    inline void set_watermark_registered(bool registered) {
        watermark_registered_ = registered;
    }

    inline bool gc_ready() const {
        return gc_ready_;
    }
    inline void set_gc_ready(bool ready) {
        gc_ready_ = ready;
    }

    inline bool admission_active() const {
        return admission_active_;
    }
    inline void set_admission_active(bool active) {
        admission_active_ = active;
    }

    inline bool ssi_metadata_released() const {
        return ssi_metadata_released_.load(std::memory_order_acquire);
    }
    inline void set_ssi_metadata_released(bool released) {
        ssi_metadata_released_.store(released, std::memory_order_release);
    }

    inline TransactionState get_state() const {
        return state_.load(std::memory_order_acquire);
    }
    inline void set_state(TransactionState state) {
        state_.store(state, std::memory_order_release);
    }

    inline lsn_t get_prev_lsn() const {
        return prev_lsn_;
    }
    inline void set_prev_lsn(lsn_t prev_lsn) {
        prev_lsn_ = prev_lsn;
    }

    inline std::vector<WriteRecord>& get_write_set() {
        return write_set_;
    }
    inline const std::vector<WriteRecord>& get_write_set() const {
        return write_set_;
    }

    inline void emplace_write_record(WType type, const std::string &tab_name, const Rid &rid) {
        auto table = InternTableName(tab_name);
        write_set_.emplace_back(type, table, rid);
        UpdateWriteSummary(table, rid, type);
    }

    inline void emplace_write_record(WType type, const std::string &tab_name, const Rid &rid,
                                     const RmRecord &record, bool index_keys_changed = true) {
        auto table = InternTableName(tab_name);
        write_set_.emplace_back(type, table, rid, record, index_keys_changed);
        UpdateWriteSummary(table, rid, type);
    }

    inline void emplace_write_record(WType type, const std::string &tab_name, const Rid &rid,
                                     std::shared_ptr<RmRecord> record, bool index_keys_changed = true) {
        auto table = InternTableName(tab_name);
        write_set_.emplace_back(type, table, rid, std::move(record), index_keys_changed);
        UpdateWriteSummary(table, rid, type);
    }

    inline const std::vector<TupleWriteSummary> &get_write_summary() const { return write_summary_; }

    inline const std::vector<lock_data_key_t>& get_lock_set() const {
        return lock_set_;
    }

    inline void add_lock(lock_data_key_t lock_key) {
        lock_set_.push_back(lock_key);
    }

    inline void remove_lock(lock_data_key_t lock_key) {
        lock_set_.erase(std::remove(lock_set_.begin(), lock_set_.end(), lock_key), lock_set_.end());
    }

    inline void clear_locks() {
        lock_set_.clear();
    }

    inline const std::vector<lock_data_key_t>& get_write_intents() const {
        return write_intents_;
    }
    inline void add_write_intent(lock_data_key_t key) {
        write_intents_.push_back(key);
    }
    inline void clear_write_intents() {
        write_intents_.clear();
    }

    inline void add_read_record(const std::string& tab_name, Rid rid) {
        read_records_[tab_name].push_back(rid);
    }

    inline const std::unordered_map<std::string, std::vector<Rid>>& get_read_records() const {
        return read_records_;
    }

    inline void add_predicate_read(std::string tab_name,
                                   std::vector<ColMeta> cols,
                                   std::vector<Condition> conds) {
        predicate_reads_.push_back({std::move(tab_name), std::move(cols), std::move(conds)});
    }

    inline const std::vector<PredicateRead>& get_predicate_reads() const {
        return predicate_reads_;
    }

    inline void upsert_serializable_write(std::string tab_name,
                                          Rid rid,
                                          std::vector<ColMeta> cols,
                                          std::shared_ptr<RmRecord> old_record,
                                          std::shared_ptr<RmRecord> new_record) {
        serializable_writes_[serializable_write_key(tab_name, rid)] = {std::move(tab_name),
                                                                       rid,
                                                                       std::move(cols),
                                                                       std::move(old_record),
                                                                       std::move(new_record)};
    }

    inline const std::unordered_map<std::string, SerializableWriteInfo>& get_serializable_writes()
        const {
        return serializable_writes_;
    }

    inline void add_rw_dependency(txn_id_t txn_id) {
        rw_dependencies_.insert(txn_id);
    }

    inline const std::unordered_set<txn_id_t>& get_rw_dependencies() const {
        return rw_dependencies_;
    }

    inline void remove_rw_dependency(txn_id_t txn_id) {
        rw_dependencies_.erase(txn_id);
    }

    inline void clear_serializable_state() {
        read_records_.clear();
        predicate_reads_.clear();
        rw_dependencies_.clear();
        serializable_writes_.clear();
    }

    inline timestamp_t get_read_ts() const {
        return read_ts_;
    }
    inline void set_read_ts(timestamp_t read_ts) {
        read_ts_ = read_ts;
    }
    inline timestamp_t get_commit_ts() const {
        return commit_ts_;
    }
    inline void set_commit_ts(timestamp_t commit_ts) {
        commit_ts_ = commit_ts;
    }

    inline void clear_write_records() {
        write_set_.clear();
        write_summary_.clear();
        interned_table_names_.clear();
    }

    /** 修改现有的撤销日志 */
    inline auto ModifyUndoLog(int log_idx, UndoLog new_log) {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_[log_idx] = std::move(new_log);
    }

    /** @return 此事务中撤销日志的索引 */
    inline auto AppendUndoLog(UndoLog log) -> UndoLink {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_.emplace_back(std::move(log));
        return {txn_id_, static_cast<int>(undo_logs_.size() - 1)};
    }
    inline auto GetUndoLog(size_t log_id) -> UndoLog {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_[log_id];
    }

    /** @return 撤销日志的数量 */
    inline auto GetUndoLogNum() -> size_t {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_.size();
    }

    inline std::vector<UndoLog> TakeUndoLogs() {
        std::scoped_lock<std::mutex> lck(latch_);
        std::vector<UndoLog> logs;
        logs.swap(undo_logs_);
        return logs;
    }

    inline void RetainUndoReference() {
        undo_ref_count_.fetch_add(1, std::memory_order_acq_rel);
    }

    inline void ReleaseUndoReference() {
        rmdb::i64 current = undo_ref_count_.load(std::memory_order_acquire);
        while (true) {
            if (current <= 0) {
                throw InternalError("Transaction undo reference count underflow");
            }
            if (undo_ref_count_.compare_exchange_weak(
                    current, current - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return;
            }
        }
    }

    inline rmdb::i64 GetUndoReferenceCount() const {
        return undo_ref_count_.load(std::memory_order_acquire);
    }

private:
    std::shared_ptr<const std::string> InternTableName(const std::string &tab_name) {
        for (const auto &name : interned_table_names_) {
            if (*name == tab_name) return name;
        }
        auto name = std::make_shared<const std::string>(tab_name);
        interned_table_names_.push_back(name);
        return name;
    }

    void UpdateWriteSummary(const std::shared_ptr<const std::string> &tab_name, const Rid &rid, WType type) {
        // OLTP transactions normally touch only a few dozen tuples. A linear
        // scan over contiguous summaries is cheaper than allocating hash
        // nodes and chasing buckets for every insert/update/delete.
        for (auto &entry : write_summary_) {
            if (entry.tab_name.get() == tab_name.get() && entry.rid == rid) {
                entry.is_deleted = type == WType::DELETE_TUPLE;
                entry.publish_version =
                    entry.publish_version || type != WType::INSERT_TUPLE;
                return;
            }
        }
        write_summary_.push_back(TupleWriteSummary{
            tab_name, rid, type == WType::DELETE_TUPLE, type != WType::INSERT_TUPLE});
    }

    static std::string serializable_write_key(const std::string& tab_name, const Rid& rid) {
        return tab_name + "#" + std::to_string(rid.page_no) + ":" + std::to_string(rid.slot_no);
    }

    bool txn_mode_{false};  // false 表示单条 SQL 的隐式事务，true 表示由 BEGIN 显式开启
    bool lightweight_read_only_{false};
    std::atomic<TransactionState> state_{
        TransactionState::DEFAULT};   // 可被 checkpoint/SSI/GC 并发观察
    IsolationLevel isolation_level_;  // 事务的隔离级别，默认隔离级别为快照隔离
    bool watermark_registered_{false};
    bool gc_ready_{false};
    bool admission_active_{false};
    std::atomic<bool> ssi_metadata_released_{true};
    std::thread::id thread_id_{std::this_thread::get_id()};  // 创建事务的线程
    lsn_t prev_lsn_{INVALID_LSN};       // 当前事务最后一条 WAL 的 LSN，用于故障恢复
    txn_id_t txn_id_;                   // 事务的ID，唯一标识符
    timestamp_t start_ts_{INVALID_TS};  // 事务开始时间戳；begin() 成功前保持无效值

    std::vector<WriteRecord> write_set_;          // 事务包含的所有写操作，首次写入时才分配
    std::vector<std::shared_ptr<const std::string>> interned_table_names_;
    std::vector<TupleWriteSummary> write_summary_;
    std::vector<lock_data_key_t> lock_set_;       // 事务申请的所有锁，首次加锁时才分配
    std::vector<lock_data_key_t> write_intents_;  // snapshot 建立前获取的热点写准入令牌

    std::atomic<timestamp_t> read_ts_{0};
    /** 提交时间戳 */
    std::atomic<timestamp_t> commit_ts_{INVALID_TS};
    std::unordered_map<std::string, std::vector<Rid>> read_records_;
    std::vector<PredicateRead> predicate_reads_;
    std::unordered_set<txn_id_t> rw_dependencies_;
    std::unordered_map<std::string, SerializableWriteInfo> serializable_writes_;
    /** 仍指向本事务 undo 日志的页头链接和跨事务链接数量。 */
    std::atomic<rmdb::i64> undo_ref_count_{0};
    /**
     * @brief 存储撤销日志。
     * 其他撤销日志/表堆将存储 (txn_id, index)
     * 对，因此只能向此vector中追加内容或就地更新内容，而不能删除任何内容。
     */
    std::vector<UndoLog> undo_logs_;
    /** 用于访问事务级撤销日志的锁。 */
    std::mutex latch_;
};
