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

#include <array>
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "transaction/transaction.h"

class LockManager {
    enum class LockMode { SHARED, EXLUCSIVE };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, S, X };

    /* 事务的加锁申请 */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode)
            : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;   // 申请加锁的事务ID
        LockMode lock_mode_;    // 事务申请加锁的类型
        bool granted_;          // 该事务是否已经被赋予锁
        std::condition_variable cv_;
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        std::list<LockRequest> request_queue_;  // 加锁队列
        txn_id_t fast_exclusive_owner_ = INVALID_TXN_ID;
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;   // 加锁队列的锁模式
    };

public:
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    // Acquire logical write-admission tokens before a transaction fixes its SI snapshot.
    // SI/SER conflicts use the same bounded wait as record locks so a stalled owner cannot
    // prevent the wire layer from returning a retryable transaction abort indefinitely.
    void canonicalize_write_intents(std::vector<lock_data_key_t> *keys) const;
    bool acquire_write_intents(Transaction *txn,
                               const std::vector<lock_data_key_t> &keys,
                               bool already_canonical = false);
    void release_write_intents(Transaction *txn);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

    bool unlock_all(Transaction *txn);

private:
    static constexpr size_t LOCK_SHARD_COUNT = 128;

    class alignas(64) LockTableShard {
    public:
        std::mutex latch_;
        std::unordered_map<lock_data_key_t, LockRequestQueue> lock_table_;
    };

    class alignas(64) IntentShard {
    public:
        std::mutex latch_;
        std::condition_variable cv_;
        std::unordered_map<lock_data_key_t, txn_id_t> owners_;
    };

    GroupLockMode recompute_group_lock_mode(const LockRequestQueue &queue) const;
    bool can_grant_request(const LockRequestQueue &queue,
                           std::list<LockRequest>::const_iterator request_iter) const;
    void notify_grantable_waiters(LockRequestQueue &queue) const;
    static size_t shard_index(lock_data_key_t lock_key);
    LockTableShard &shard_for(lock_data_key_t lock_key);
    bool set_wait_edges_and_detect_cycle(txn_id_t waiting_txn, const std::vector<txn_id_t> &blockers);
    void clear_wait_edges(txn_id_t txn_id);
    void remove_txn_from_wait_graph(txn_id_t txn_id);

    std::array<LockTableShard, LOCK_SHARD_COUNT> lock_table_shards_;
    std::array<IntentShard, LOCK_SHARD_COUNT> intent_shards_;
    std::mutex wait_graph_latch_;
    std::unordered_map<txn_id_t, std::vector<txn_id_t>> wait_edges_;
};
