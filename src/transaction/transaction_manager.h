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
#include <chrono>
#include <array>
#include <condition_variable>
#include <deque>
#include <unordered_map>
#include <optional>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "transaction.h"
#include "common/common.h"
#include "common/config.h"

#include "watermark.h"
#include "recovery/log_manager.h"
#include "concurrency/lock_manager.h"
#include "system/sm_manager.h"
#include "common/exception.h"
#include "common/common.h"



#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

// 语句级 checkpoint 闸门:替代每语句一次的 shared_mutex。
//
// 读者(语句执行)走原子计数快路径,日常零锁;写者(WAL reset / SQL
// CHECKPOINT)置位 writer 标志后等待读者归零。正确性依赖"先递增、后复查"
// 的顺序:递增若发生在 writer 置位之前,writer 的 readers==0 等待会观察到
// 该增量并等待其退出;若置位已发生,读者撤销增量并转入持锁等待。
class StatementCheckpointGate {
   public:
    class ReadGuard {
       public:
        ReadGuard() = default;
        explicit ReadGuard(StatementCheckpointGate *gate) : gate_(gate) {}
        ReadGuard(const ReadGuard &) = delete;
        ReadGuard &operator=(const ReadGuard &) = delete;
        ReadGuard(ReadGuard &&other) noexcept : gate_(other.gate_) { other.gate_ = nullptr; }
        ReadGuard &operator=(ReadGuard &&other) noexcept {
            if (this != &other) {
                Release();
                gate_ = other.gate_;
                other.gate_ = nullptr;
            }
            return *this;
        }
        ~ReadGuard() { Release(); }

       private:
        void Release() {
            if (gate_ != nullptr) {
                gate_->ReadExit();
                gate_ = nullptr;
            }
        }
        StatementCheckpointGate *gate_ = nullptr;
    };

    class WriteGuard {
       public:
        WriteGuard() = default;
        explicit WriteGuard(StatementCheckpointGate *gate) : gate_(gate) {}
        WriteGuard(const WriteGuard &) = delete;
        WriteGuard &operator=(const WriteGuard &) = delete;
        WriteGuard(WriteGuard &&other) noexcept : gate_(other.gate_) { other.gate_ = nullptr; }
        WriteGuard &operator=(WriteGuard &&other) noexcept {
            if (this != &other) {
                Release();
                gate_ = other.gate_;
                other.gate_ = nullptr;
            }
            return *this;
        }
        ~WriteGuard() { Release(); }

       private:
        void Release() {
            if (gate_ != nullptr) {
                gate_->WriteExit();
                gate_ = nullptr;
            }
        }
        StatementCheckpointGate *gate_ = nullptr;
    };

    ReadGuard ReadEnter() {
        for (;;) {
            readers_.fetch_add(1, std::memory_order_acq_rel);
            if (!writer_.load(std::memory_order_acquire)) {
                return ReadGuard(this);
            }
            ReadExit();
            WaitForWriterClear();
        }
    }

    WriteGuard WriteEnter() {
        writer_.store(true, std::memory_order_release);
        std::unique_lock<std::mutex> lock(wait_mutex_);
        wait_cv_.wait(lock, [&] { return readers_.load(std::memory_order_acquire) == 0; });
        return WriteGuard(this);
    }

   private:
    void ReadExit() {
        // 最后一个读者退出且写者在等待时才通知,避免每次退出都唤醒。
        if (readers_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            if (writer_.load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lock(wait_mutex_);
                wait_cv_.notify_all();
            }
        }
    }

    void WriteExit() {
        writer_.store(false, std::memory_order_release);
        std::lock_guard<std::mutex> lock(wait_mutex_);
        wait_cv_.notify_all();
    }

    void WaitForWriterClear() {
        std::unique_lock<std::mutex> lock(wait_mutex_);
        wait_cv_.wait(lock, [&] { return !writer_.load(std::memory_order_acquire); });
    }

    std::atomic<std::uint32_t> readers_{0};
    std::atomic<bool> writer_{false};
    std::mutex wait_mutex_;
    std::condition_variable wait_cv_;
};



#include <array>
#include <atomic>
#include <cassert>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>


/// 版本链中的第一个撤销链接，将表堆元组链接到撤销日志。
struct VersionUndoLink {
    /** 版本链中的下一个版本。 */
    UndoLink prev_;
    bool in_progress_{false};

    friend auto operator==(const VersionUndoLink &a, const VersionUndoLink &b) {
        return a.prev_ == b.prev_ && a.in_progress_ == b.in_progress_;
    }

    friend auto operator!=(const VersionUndoLink &a, const VersionUndoLink &b) { return !(a == b); }

    inline static std::optional<VersionUndoLink> FromOptionalUndoLink(std::optional<UndoLink> undo_link) {
        if (undo_link.has_value()) {
            return VersionUndoLink{*undo_link};
        }
        return std::nullopt;
    }
};
struct TransactionPageVersionInfo {
    std::shared_mutex mutex_;
    std::atomic<int> active_meta_count_{0};
    std::atomic<int> uncommitted_meta_count_{0};
    std::atomic<int> deleted_count_{0};
    std::atomic<int> version_link_count_{0};
    std::atomic<timestamp_t> max_committed_meta_ts_{0};
    std::atomic<rmdb::u64> visibility_epoch_{1};
    std::atomic<rmdb::u32> dirty_slot_count_{0};
    rmdb::u32 slot_count_{0};
    rmdb::u32 slot_word_count_{0};
    std::unique_ptr<std::atomic<rmdb::u64>[]> dirty_slot_words_;
    rmdb::u32 dirty_slot_word_count_{0};
    std::unique_ptr<rmdb::u64[]> tuple_meta_valid_words_;
    std::unique_ptr<TupleMeta[]> tuple_meta_values_;
    std::unique_ptr<rmdb::u64[]> version_valid_words_;
    std::unique_ptr<VersionUndoLink[]> version_values_;

    void InitDirtySlots(rmdb::u32 slot_count) {
        if (slot_count == 0 || dirty_slot_words_ != nullptr) {
            return;
        }
        rmdb::u32 word_count = (slot_count + 63) / 64;
        auto words = std::make_unique<std::atomic<rmdb::u64>[]>(word_count);
        for (rmdb::u32 i = 0; i < word_count; ++i) {
            words[i].store(0, std::memory_order_relaxed);
        }
        slot_count_ = slot_count;
        slot_word_count_ = word_count;
        dirty_slot_word_count_ = word_count;
        dirty_slot_words_ = std::move(words);
    }

    bool CanTrackSlot(slot_offset_t slot_no) const {
        return slot_no < slot_count_;
    }

    static rmdb::u64 SlotBit(slot_offset_t slot_no) {
        return rmdb::u64{1} << (slot_no & 63);
    }

    void EnsureTupleMetaStorage() {
        if (tuple_meta_values_ != nullptr) {
            return;
        }
        tuple_meta_valid_words_ = std::make_unique<rmdb::u64[]>(slot_word_count_);
        tuple_meta_values_ = std::make_unique<TupleMeta[]>(slot_count_);
    }

    bool HasTupleMeta(slot_offset_t slot_no) const {
        if (!CanTrackSlot(slot_no) || tuple_meta_valid_words_ == nullptr) {
            return false;
        }
        return (tuple_meta_valid_words_[slot_no / 64] & SlotBit(slot_no)) != 0;
    }

    const TupleMeta *GetTupleMeta(slot_offset_t slot_no) const {
        return HasTupleMeta(slot_no) ? &tuple_meta_values_[slot_no] : nullptr;
    }

    TupleMeta *GetMutableTupleMeta(slot_offset_t slot_no) {
        return HasTupleMeta(slot_no) ? &tuple_meta_values_[slot_no] : nullptr;
    }

    bool SetTupleMeta(slot_offset_t slot_no, const TupleMeta &meta) {
        assert(CanTrackSlot(slot_no));
        EnsureTupleMetaStorage();
        rmdb::u64 &word = tuple_meta_valid_words_[slot_no / 64];
        rmdb::u64 bit = SlotBit(slot_no);
        bool existed = (word & bit) != 0;
        tuple_meta_values_[slot_no] = meta;
        word |= bit;
        return existed;
    }

    bool ClearTupleMeta(slot_offset_t slot_no) {
        if (!HasTupleMeta(slot_no)) {
            return false;
        }
        tuple_meta_valid_words_[slot_no / 64] &= ~SlotBit(slot_no);
        return true;
    }

    void ReleaseTupleMetaStorageIfEmpty() {
        if (active_meta_count_.load(std::memory_order_relaxed) == 0) {
            tuple_meta_values_.reset();
            tuple_meta_valid_words_.reset();
        }
    }

    void EnsureVersionStorage() {
        if (version_values_ != nullptr) {
            return;
        }
        version_valid_words_ = std::make_unique<rmdb::u64[]>(slot_word_count_);
        version_values_ = std::make_unique<VersionUndoLink[]>(slot_count_);
    }

    bool HasVersion(slot_offset_t slot_no) const {
        if (!CanTrackSlot(slot_no) || version_valid_words_ == nullptr) {
            return false;
        }
        return (version_valid_words_[slot_no / 64] & SlotBit(slot_no)) != 0;
    }

    const VersionUndoLink *GetVersion(slot_offset_t slot_no) const {
        return HasVersion(slot_no) ? &version_values_[slot_no] : nullptr;
    }

    VersionUndoLink *GetMutableVersion(slot_offset_t slot_no) {
        return HasVersion(slot_no) ? &version_values_[slot_no] : nullptr;
    }

    bool SetVersion(slot_offset_t slot_no, const VersionUndoLink &version) {
        assert(CanTrackSlot(slot_no));
        EnsureVersionStorage();
        rmdb::u64 &word = version_valid_words_[slot_no / 64];
        rmdb::u64 bit = SlotBit(slot_no);
        bool existed = (word & bit) != 0;
        version_values_[slot_no] = version;
        word |= bit;
        return existed;
    }

    bool ClearVersion(slot_offset_t slot_no) {
        if (!HasVersion(slot_no)) {
            return false;
        }
        version_valid_words_[slot_no / 64] &= ~SlotBit(slot_no);
        return true;
    }

    void ReleaseVersionStorageIfEmpty() {
        if (version_link_count_.load(std::memory_order_relaxed) == 0) {
            version_values_.reset();
            version_valid_words_.reset();
        }
    }

    bool CanTrackDirtySlot(slot_offset_t slot_no) const {
        rmdb::u32 word_idx = static_cast<rmdb::u32>(slot_no / 64);
        return dirty_slot_words_ != nullptr && word_idx < dirty_slot_word_count_;
    }

    bool MarkDirtySlot(slot_offset_t slot_no) {
        rmdb::u32 word_idx = static_cast<rmdb::u32>(slot_no / 64);
        if (!CanTrackDirtySlot(slot_no)) {
            return false;
        }
        rmdb::u64 bit = 1ull << (slot_no & 63);
        rmdb::u64 old = dirty_slot_words_[word_idx].fetch_or(bit, std::memory_order_acq_rel);
        if ((old & bit) != 0) {
            return false;
        }
        rmdb::atomic_counter::Increment(dirty_slot_count_, rmdb::u32{1}, std::memory_order_acq_rel);
        return true;
    }

    bool ClearDirtySlot(slot_offset_t slot_no) {
        rmdb::u32 word_idx = static_cast<rmdb::u32>(slot_no / 64);
        if (!CanTrackDirtySlot(slot_no)) {
            return false;
        }
        rmdb::u64 bit = 1ull << (slot_no & 63);
        rmdb::u64 old = dirty_slot_words_[word_idx].fetch_and(~bit, std::memory_order_acq_rel);
        if ((old & bit) == 0) {
            return false;
        }
        rmdb::atomic_counter::DecrementPositive(dirty_slot_count_, "Dirty page slot count", rmdb::u32{1},
                                                std::memory_order_acq_rel, std::memory_order_acquire);
        return true;
    }

    bool IsSlotClean(slot_offset_t slot_no) const {
        rmdb::u32 word_idx = static_cast<rmdb::u32>(slot_no / 64);
        if (dirty_slot_words_ == nullptr || word_idx >= dirty_slot_word_count_) {
            return false;
        }
        rmdb::u64 bit = 1ull << (slot_no & 63);
        return (dirty_slot_words_[word_idx].load(std::memory_order_acquire) & bit) == 0;
    }

    rmdb::u32 DirtySlotCount() const {
        return dirty_slot_count_.load(std::memory_order_acquire);
    }
};

struct TransactionTableVersionInfo {
    static constexpr page_id_t kExactDirtyPageLimit = 1 << 20;
    static constexpr size_t kExactDirtyPageWordCount = static_cast<size_t>(kExactDirtyPageLimit) / 64;
    static constexpr size_t kDensePageChunkBits = 10;
    static constexpr size_t kDensePageChunkSize = 1 << kDensePageChunkBits;
    static constexpr size_t kDensePageChunkMask = kDensePageChunkSize - 1;
    static constexpr size_t kDensePageChunkCount =
        static_cast<size_t>(kExactDirtyPageLimit) / kDensePageChunkSize;

    struct PageInfoChunk {
        std::array<std::atomic<TransactionPageVersionInfo *>, kDensePageChunkSize> pages_{};

        PageInfoChunk() {
            for (auto &page : pages_) {
                page.store(nullptr, std::memory_order_relaxed);
            }
        }
    };

    TransactionTableVersionInfo() {
        for (auto &word : dirty_page_filter_) {
            word.store(0, std::memory_order_relaxed);
        }
        for (auto &word : exact_dirty_pages_) {
            word.store(0, std::memory_order_relaxed);
        }
        for (auto &chunk : page_info_chunks_) {
            chunk.store(nullptr, std::memory_order_relaxed);
        }
    }

    std::shared_mutex mutex_;
    std::atomic<rmdb::u64> page_map_epoch_{0};
    std::atomic<rmdb::u32> dirty_page_count_{0};
    std::atomic<rmdb::u64> dirty_slot_total_{0};
    std::array<std::atomic<rmdb::u64>, 1024> dirty_page_filter_{};
    std::array<std::atomic<rmdb::u64>, kExactDirtyPageWordCount> exact_dirty_pages_{};
    std::array<std::atomic<PageInfoChunk *>, kDensePageChunkCount> page_info_chunks_{};
    std::vector<std::unique_ptr<PageInfoChunk>> page_info_chunk_owners_;
    std::unordered_map<page_id_t, std::shared_ptr<TransactionPageVersionInfo>> pages_;

    bool maybe_has_page_version_info(page_id_t page_no) const {
        rmdb::u64 hash = static_cast<rmdb::u64>(static_cast<rmdb::u32>(page_no)) * 11400714819323198485ull;
        size_t bucket = static_cast<size_t>(hash & (dirty_page_filter_.size() - 1));
        rmdb::u64 bit = 1ull << ((hash >> 10) & 63);
        return (dirty_page_filter_[bucket].load(std::memory_order_acquire) & bit) != 0;
    }

    void mark_page_version_info(page_id_t page_no) {
        rmdb::u64 hash = static_cast<rmdb::u64>(static_cast<rmdb::u32>(page_no)) * 11400714819323198485ull;
        size_t bucket = static_cast<size_t>(hash & (dirty_page_filter_.size() - 1));
        rmdb::u64 bit = 1ull << ((hash >> 10) & 63);
        dirty_page_filter_[bucket].fetch_or(bit, std::memory_order_release);
    }

    bool tracks_exact_dirty_page(page_id_t page_no) const {
        return page_no >= 0 && page_no < kExactDirtyPageLimit;
    }

    bool is_page_dirty_exact(page_id_t page_no) const {
        if (!tracks_exact_dirty_page(page_no)) {
            return true;
        }
        size_t word_idx = static_cast<size_t>(page_no) / 64;
        rmdb::u64 bit = 1ull << (static_cast<rmdb::u32>(page_no) & 63);
        return (exact_dirty_pages_[word_idx].load(std::memory_order_acquire) & bit) != 0;
    }

    bool mark_page_dirty_exact(page_id_t page_no) {
        if (!tracks_exact_dirty_page(page_no)) {
            return false;
        }
        size_t word_idx = static_cast<size_t>(page_no) / 64;
        rmdb::u64 bit = 1ull << (static_cast<rmdb::u32>(page_no) & 63);
        rmdb::u64 old = exact_dirty_pages_[word_idx].fetch_or(bit, std::memory_order_acq_rel);
        if ((old & bit) != 0) {
            return false;
        }
        rmdb::atomic_counter::Increment(dirty_page_count_, rmdb::u32{1}, std::memory_order_acq_rel);
        return true;
    }

    bool clear_page_dirty_exact(page_id_t page_no) {
        if (!tracks_exact_dirty_page(page_no)) {
            return false;
        }
        size_t word_idx = static_cast<size_t>(page_no) / 64;
        rmdb::u64 bit = 1ull << (static_cast<rmdb::u32>(page_no) & 63);
        rmdb::u64 old = exact_dirty_pages_[word_idx].fetch_and(~bit, std::memory_order_acq_rel);
        if ((old & bit) == 0) {
            return false;
        }
        rmdb::atomic_counter::DecrementPositive(dirty_page_count_, "Dirty table page count", rmdb::u32{1},
                                                std::memory_order_acq_rel, std::memory_order_acquire);
        return true;
    }

    TransactionPageVersionInfo *lookup_page_info_dense(page_id_t page_no) const {
        if (!tracks_exact_dirty_page(page_no)) {
            return nullptr;
        }
        size_t chunk_idx = static_cast<size_t>(page_no) >> kDensePageChunkBits;
        size_t chunk_offset = static_cast<size_t>(page_no) & kDensePageChunkMask;
        auto *chunk = page_info_chunks_[chunk_idx].load(std::memory_order_acquire);
        if (chunk == nullptr) {
            return nullptr;
        }
        return chunk->pages_[chunk_offset].load(std::memory_order_acquire);
    }

    void publish_page_info_dense(page_id_t page_no, TransactionPageVersionInfo *page_info) {
        if (!tracks_exact_dirty_page(page_no) || page_info == nullptr) {
            return;
        }
        size_t chunk_idx = static_cast<size_t>(page_no) >> kDensePageChunkBits;
        size_t chunk_offset = static_cast<size_t>(page_no) & kDensePageChunkMask;
        auto *chunk = page_info_chunks_[chunk_idx].load(std::memory_order_acquire);
        if (chunk == nullptr) {
            auto owner = std::make_unique<PageInfoChunk>();
            chunk = owner.get();
            page_info_chunk_owners_.push_back(std::move(owner));
            page_info_chunks_[chunk_idx].store(chunk, std::memory_order_release);
        }
        chunk->pages_[chunk_offset].store(page_info, std::memory_order_release);
    }

    rmdb::u32 DirtyPageCount() const {
        return dirty_page_count_.load(std::memory_order_acquire);
    }

    rmdb::u64 DirtySlotTotal() const {
        return dirty_slot_total_.load(std::memory_order_acquire);
    }
};




#include <atomic>
#include <memory>

namespace rmdb::transaction_page_state {

using rmdb::atomic_counter::DecrementPositive;
using rmdb::atomic_counter::Increment;

inline void AddActiveMeta(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    Increment(page_info->active_meta_count_);
}

inline void RemoveActiveMeta(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    DecrementPositive(page_info->active_meta_count_, "Active tuple metadata count");
}

inline void AddUncommittedMeta(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    Increment(page_info->uncommitted_meta_count_);
}

inline void RemoveUncommittedMeta(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    DecrementPositive(page_info->uncommitted_meta_count_, "Uncommitted tuple metadata count");
}

inline void AddDeletedMeta(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    Increment(page_info->deleted_count_);
}

inline void RemoveDeletedMeta(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    DecrementPositive(page_info->deleted_count_, "Deleted tuple metadata count");
}

inline void AddVersionLink(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    Increment(page_info->version_link_count_);
}

inline void RemoveVersionLink(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    DecrementPositive(page_info->version_link_count_, "Version link count");
}

inline void AdvanceMaxCommittedMetaTs(const std::shared_ptr<TransactionPageVersionInfo> &page_info,
                                      timestamp_t timestamp) {
    timestamp_t observed = page_info->max_committed_meta_ts_.load(std::memory_order_relaxed);
    while (timestamp > observed &&
           !page_info->max_committed_meta_ts_.compare_exchange_weak(
               observed, timestamp, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

inline void BumpVisibilityEpoch(const std::shared_ptr<TransactionPageVersionInfo> &page_info) {
    if (page_info != nullptr) {
        page_info->visibility_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
}

inline void MarkDirtySlot(const std::shared_ptr<TransactionTableVersionInfo> &table_info,
                          const std::shared_ptr<TransactionPageVersionInfo> &page_info, const Rid &rid) {
    if (page_info == nullptr || rid.slot_no < 0) {
        return;
    }
    slot_offset_t slot = static_cast<slot_offset_t>(rid.slot_no);
    if (!page_info->CanTrackDirtySlot(slot)) {
        if (table_info != nullptr) {
            table_info->mark_page_dirty_exact(rid.page_no);
        }
        return;
    }
    bool newly_dirty = page_info->MarkDirtySlot(slot);
    if (newly_dirty && table_info != nullptr) {
        Increment(table_info->dirty_slot_total_, rmdb::u64{1}, std::memory_order_acq_rel);
    }
    if (newly_dirty && page_info->DirtySlotCount() == 1 && table_info != nullptr) {
        table_info->mark_page_dirty_exact(rid.page_no);
    }
}

inline void ClearDirtySlotIfEmpty(const std::shared_ptr<TransactionTableVersionInfo> &table_info,
                                  const std::shared_ptr<TransactionPageVersionInfo> &page_info, const Rid &rid) {
    if (page_info == nullptr || rid.slot_no < 0) {
        return;
    }
    slot_offset_t slot = static_cast<slot_offset_t>(rid.slot_no);
    if (!page_info->CanTrackDirtySlot(slot)) {
        return;
    }
    if (!page_info->HasTupleMeta(slot) && !page_info->HasVersion(slot)) {
        bool cleared = page_info->ClearDirtySlot(slot);
        if (cleared && table_info != nullptr) {
            DecrementPositive(table_info->dirty_slot_total_, "Dirty slot count", rmdb::u64{1},
                              std::memory_order_acq_rel, std::memory_order_acquire);
        }
        if (cleared && page_info->DirtySlotCount() == 0 && table_info != nullptr) {
            table_info->clear_page_dirty_exact(rid.page_no);
        }
    }
}

}  // namespace rmdb::transaction_page_state




#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

struct TransactionVersionState {
    using TableMap = std::unordered_map<std::string, std::shared_ptr<TransactionTableVersionInfo>>;

    mutable std::shared_mutex mutex;
    TableMap tables;
    std::atomic<rmdb::u64> epoch{1};
    inline static std::atomic<rmdb::u64> next_cache_id{1};
    const rmdb::u64 cache_id{next_cache_id.fetch_add(1, std::memory_order_relaxed)};
};



#include "record/rm_defs.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

struct TransactionRetiredTuple {
    std::string tab_name;
    Rid rid;
};

struct TransactionRetiredBatch {
    timestamp_t commit_ts{INVALID_TS};
    std::vector<TransactionRetiredTuple> tuples;
};

struct TransactionGcState {
    std::mutex run_mutex;
    std::mutex queue_mutex;
    std::deque<TransactionRetiredBatch> retired_batches;
    std::atomic<size_t> retired_tuple_count{0};
    std::deque<txn_id_t> finished_transactions;
    std::chrono::steady_clock::time_point last_gc_trigger{};
};



#include "watermark.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <shared_mutex>

struct TransactionLifecycleState {
    std::atomic<txn_id_t> next_txn_id{0};
    std::atomic<timestamp_t> next_timestamp{0};
    std::mutex commit_publish_mutex;
    std::condition_variable commit_publish_cv;
    std::atomic<rmdb::u64> next_commit_ticket{0};
    std::atomic<rmdb::u64> next_publish_ticket{0};  // 发布序号；锁内推进，等待者可无锁自旋观察
    StatementCheckpointGate checkpoint_gate;
    mutable std::mutex admission_mutex;
    std::condition_variable admission_cv;
    // 日常路径(无 WAL reset 阻塞)下 active 计数与 blocked 标志均为原子操作,
    // 不触碰 mutex/cv;仅在 drain 阻塞期间才走持锁慢路径。
    std::atomic<bool> admission_blocked{false};
    std::atomic<size_t> active_transaction_count{0};
    std::atomic<timestamp_t> last_commit_ts{0};
    Watermark running_txns{0};
};



#include <array>
#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "defs.h"

namespace rmdb {

class SnapshotIndexHistoryStore {
   public:
    void Record(const std::string &table_name, rmdb::u64 index_id,
                const Rid &rid, timestamp_t retire_ts);
    std::vector<Rid> Lookup(const std::string &table_name, rmdb::u64 index_id,
                            timestamp_t read_ts);
    void Purge(timestamp_t watermark);
    void EraseIndex(const std::string &table_name, rmdb::u64 index_id);
    void EraseTable(const std::string &table_name);
    void Clear();
    size_t EntryCountForTest();

   private:
    struct Key {
        std::string table_name;
        rmdb::u64 index_id{0};

        bool operator==(const Key &other) const {
            return table_name == other.table_name && index_id == other.index_id;
        }
    };

    struct KeyHash {
        size_t operator()(const Key &key) const;
    };

    struct Entry {
        timestamp_t retire_ts{INVALID_TS};
        Rid rid{};
    };

    struct Shard {
        std::mutex mutex;
        std::unordered_map<Key, std::vector<Entry>, KeyHash> entries;
    };

    static constexpr size_t kShardCount = 32;

    size_t shard_index(const Key &key) const;
    std::array<Shard, kShardCount> shards_;
};

}  // namespace rmdb




#include <array>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <utility>

class TransactionRegistry {
   public:
    using Map = std::unordered_map<txn_id_t, Transaction *>;
    static constexpr size_t kShardCount = 32;

    class AllView {
       public:
        Transaction *Find(txn_id_t txn_id) const { return registry_->FindUnlocked(txn_id); }

        bool Erase(txn_id_t txn_id) {
            Map &map = registry_->shards_[registry_->ShardIndex(txn_id)].transactions;
            return map.erase(txn_id) != 0;
        }

        size_t Size() const {
            size_t size = 0;
            for (const auto &shard : registry_->shards_) {
                size += shard.transactions.size();
            }
            return size;
        }

        template <typename Fn>
        void ForEach(Fn &&fn) const {
            for (const auto &shard : registry_->shards_) {
                for (const auto &entry : shard.transactions) {
                    fn(entry.first, entry.second);
                }
            }
        }

       private:
        friend class TransactionRegistry;
        explicit AllView(TransactionRegistry *registry) : registry_(registry) {}
        TransactionRegistry *registry_;
    };

    TransactionRegistry() = default;
    TransactionRegistry(const TransactionRegistry &) = delete;
    TransactionRegistry &operator=(const TransactionRegistry &) = delete;

    ~TransactionRegistry() {
        std::unique_lock<std::shared_mutex> all_lock(all_latch_);
        for (auto &shard : shards_) {
            for (auto &entry : shard.transactions) {
                delete entry.second;
            }
        }
    }

    void Insert(Transaction *txn) {
        if (txn == nullptr) {
            throw InternalError("Cannot register a null transaction");
        }
        std::shared_lock<std::shared_mutex> all_lock(all_latch_);
        auto &shard = shards_[ShardIndex(txn->get_transaction_id())];
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        auto [iter, inserted] = shard.transactions.emplace(txn->get_transaction_id(), txn);
        if (!inserted && iter->second != txn) {
            throw InternalError("Transaction id is already registered");
        }
        iter->second = txn;
    }

    Transaction *GetThreadOwned(txn_id_t txn_id) const {
        if (txn_id == INVALID_TXN_ID) {
            return nullptr;
        }
        std::shared_lock<std::shared_mutex> all_lock(all_latch_);
        const auto &shard = shards_[ShardIndex(txn_id)];
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto iter = shard.transactions.find(txn_id);
        if (iter == shard.transactions.end() || iter->second == nullptr) {
            throw InternalError("Transaction is not registered");
        }
        Transaction *txn = iter->second;
        if (txn->get_thread_id() != std::this_thread::get_id()) {
            throw InternalError("Transaction belongs to a different thread");
        }
        return txn;
    }

    size_t Size() const {
        std::shared_lock<std::shared_mutex> all_lock(all_latch_);
        std::array<std::shared_lock<std::shared_mutex>, kShardCount> shard_locks;
        size_t size = 0;
        for (size_t i = 0; i < kShardCount; ++i) {
            shard_locks[i] = std::shared_lock<std::shared_mutex>(shards_[i].mutex);
            size += shards_[i].transactions.size();
        }
        return size;
    }

    template <typename Fn>
    decltype(auto) WithAllShared(Fn &&fn) const {
        std::shared_lock<std::shared_mutex> all_lock(all_latch_);
        std::array<std::shared_lock<std::shared_mutex>, kShardCount> shard_locks;
        for (size_t i = 0; i < kShardCount; ++i) {
            shard_locks[i] = std::shared_lock<std::shared_mutex>(shards_[i].mutex);
        }
        return std::forward<Fn>(fn)(AllView(const_cast<TransactionRegistry *>(this)));
    }

    template <typename Fn>
    decltype(auto) WithAllExclusive(Fn &&fn) {
        std::unique_lock<std::shared_mutex> all_lock(all_latch_);
        return std::forward<Fn>(fn)(AllView(this));
    }

    template <typename Fn>
    decltype(auto) WithTransactionShared(txn_id_t txn_id, Fn &&fn) const {
        std::shared_lock<std::shared_mutex> all_lock(all_latch_);
        const auto &shard = shards_[ShardIndex(txn_id)];
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto iter = shard.transactions.find(txn_id);
        return std::forward<Fn>(fn)(iter == shard.transactions.end() ? nullptr : iter->second);
    }

   private:
    struct alignas(64) Shard {
        mutable std::shared_mutex mutex;
        Map transactions;
    };

    size_t ShardIndex(txn_id_t txn_id) const {
        return std::hash<txn_id_t>{}(txn_id) % kShardCount;
    }

    Transaction *FindUnlocked(txn_id_t txn_id) const {
        const auto &map = shards_[ShardIndex(txn_id)].transactions;
        auto iter = map.find(txn_id);
        return iter == map.end() ? nullptr : iter->second;
    }

    mutable std::shared_mutex all_latch_;
    std::array<Shard, kShardCount> shards_;
};




class SerializableMetadataRetention {
   public:
    explicit SerializableMetadataRetention(const TransactionRegistry::AllView &transactions);

    void ReleaseIfSafe(txn_id_t txn_id, Transaction *txn, TransactionRegistry::AllView &transactions) const;

   private:
    bool has_active_serializable_{false};
    timestamp_t oldest_active_start_ts_{INVALID_TS};
};


/* 系统采用的并发控制算法，当前题目中要求两阶段封锁并发控制算法 */
enum class ConcurrencyMode { TWO_PHASE_LOCKING = 0, BASIC_TO, MVCC };


class RmRecordPageCursor;
class TransactionManager;

class TransactionDrainGuard {
   public:
    TransactionDrainGuard() = default;
    ~TransactionDrainGuard();

    TransactionDrainGuard(const TransactionDrainGuard &) = delete;
    TransactionDrainGuard &operator=(const TransactionDrainGuard &) = delete;

    TransactionDrainGuard(TransactionDrainGuard &&other) noexcept;
    TransactionDrainGuard &operator=(TransactionDrainGuard &&other) noexcept;

    explicit operator bool() const { return manager_ != nullptr; }

   private:
    friend class TransactionManager;
    explicit TransactionDrainGuard(TransactionManager *manager) : manager_(manager) {}
    void reset();

    TransactionManager *manager_{nullptr};
};
/**
 * Coordinates transaction lifetime, MVCC visibility, SI/SSI validation,
 * rollback and checkpoint admission.
 *
 * Commit invariant: a write transaction persists its COMMIT WAL record before
 * publishing tuple metadata and the COMMITTED state. Abort reverses the whole
 * write set before the caller may report TRANSACTION_ABORT/ERROR.
 */
class TransactionManager{
public:
    struct TupleMutationState {
        TupleMeta meta{0, false};
        std::optional<VersionUndoLink> version;
    };
    enum class UniqueKeyConflictResult { NONE, ABORT, FAILURE };
    enum class IndexEntryVisibilityState { CURRENT_KEY_VISIBLE, INVISIBLE, NEEDS_HEAP };
    using PageVersionInfo = TransactionPageVersionInfo;
    using TableVersionInfo = TransactionTableVersionInfo;
    struct IndexEntryTupleHint {
        bool valid = false;
        page_id_t page_no = INVALID_PAGE_ID;
        int slot_no = -1;
        PageVersionInfo *page_info = nullptr;
        rmdb::u64 visibility_epoch = 0;
        bool has_meta = false;
        TupleMeta meta{0, false};
        bool has_version_link = false;
        VersionUndoLink version_link{};

        void Reset() {
            valid = false;
            page_no = INVALID_PAGE_ID;
            slot_no = -1;
            page_info = nullptr;
            visibility_epoch = 0;
            has_meta = false;
            meta = TupleMeta{0, false};
            has_version_link = false;
            version_link = VersionUndoLink{};
        }

        bool Matches(const Rid &rid) const {
            return valid && page_no == rid.page_no && slot_no == rid.slot_no && page_info != nullptr;
        }
    };

    explicit TransactionManager(LockManager *lock_manager, SmManager *sm_manager,
                             ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING) {
        sm_manager_ = sm_manager;
        lock_manager_ = lock_manager;
        concurrency_mode_ = concurrency_mode;
    }
    
    ~TransactionManager() = default;

    Transaction* begin(Transaction* txn, LogManager* log_manager, IsolationLevel isolation_level);
    Transaction* begin(Transaction* txn, LogManager* log_manager, IsolationLevel isolation_level,
                       const std::vector<lock_data_key_t> &write_intents,
                       bool write_intents_canonical = false);

    Transaction* begin_read_only(IsolationLevel isolation_level);

    void PromoteToWrite(Transaction *txn, LogManager *log_manager);

    void commit(Transaction* txn, LogManager* log_manager);

    void abort(Transaction* txn, LogManager* log_manager);

    void PhysicalizeCommittedDeletes();

    bool UpdateTupleMeta(const std::string &tab_name, Rid rid, std::optional<TupleMeta> meta,
                         std::function<bool(std::optional<TupleMeta>)> &&check = nullptr);

    std::optional<TupleMeta> GetTupleMeta(const std::string &tab_name, Rid rid);

    TupleMeta GetTupleMetaOrDefault(const std::string &tab_name, Rid rid);

    TupleMutationState GetTupleMutationState(const std::string &tab_name, Rid rid);

    std::shared_ptr<PageVersionInfo> GetPageVersionInfo(const std::string &tab_name, page_id_t page_no);
    std::shared_ptr<TableVersionInfo> GetTableVersionInfo(const std::string &tab_name);
    std::shared_ptr<TableVersionInfo> GetOrCreateTableVersionInfo(const std::string &tab_name);
    std::shared_ptr<PageVersionInfo> GetPageVersionInfoOnTable(
        const std::shared_ptr<TableVersionInfo> &table_info, page_id_t page_no);
    std::shared_ptr<PageVersionInfo> GetPageVersionInfoOnTable(
        const std::shared_ptr<TableVersionInfo> &table_info, page_id_t page_no, rmdb::u64 *page_map_epoch);
    PageVersionInfo *GetPageVersionInfoOnTableRaw(
        const std::shared_ptr<TableVersionInfo> &table_info, page_id_t page_no, rmdb::u64 *page_map_epoch = nullptr);

    std::optional<RmRecord> GetVisibleTuple(const std::string &tab_name, const Rid &rid, Transaction *txn,
                                            TupleMeta *visible_meta = nullptr);

    bool GetVisibleTupleInto(const std::string &tab_name, const Rid &rid, Transaction *txn, RmRecord *out_record,
                            TupleMeta *visible_meta = nullptr, RmRecordPageCursor *page_cursor = nullptr);

    bool GetVisibleTupleInto(const std::string &tab_name,
                            const std::shared_ptr<TableVersionInfo> &table_info,
                            const Rid &rid, Transaction *txn, RmRecord *out_record,
                            TupleMeta *visible_meta = nullptr, RmRecordPageCursor *page_cursor = nullptr);

    IndexEntryVisibilityState ClassifySnapshotIndexEntryOnPage(
        PageVersionInfo *page_info, const Rid &rid, Transaction *txn,
        IndexEntryTupleHint *hint = nullptr);
    IndexEntryVisibilityState ClassifySnapshotIndexEntryState(bool has_meta,
                                                                  const TupleMeta &meta,
                                                                  bool has_version_link,
                                                                  Transaction *txn) const;
    bool IsSnapshotPageCleanVisible(const PageVersionInfo *page_info, Transaction *txn) const;

    void EnsureWriteConflictFree(Transaction *txn, const std::string &tab_name, const Rid &rid);

    void EnsureWriteConflictFree(Transaction *txn, const TupleMeta &current_meta);

    void EnsureKeyConflictFree(Transaction *txn, const std::string &tab_name, const TabMeta &tab,
                               const RmRecord &record, const Rid *self_rid = nullptr);

    UniqueKeyConflictResult ClassifyUniqueIndexConflict(Transaction *txn, const std::string &tab_name,
                                                        const IndexMeta &index, const char *key,
                                                        const std::vector<Rid> &matches,
                                                        const Rid *self_rid = nullptr);

    void RecordSnapshotIndexRetirement(const std::string &tab_name, const IndexMeta &index,
                                       const Rid &rid, timestamp_t retire_ts);
    std::vector<Rid> LookupSnapshotIndexHistory(const std::string &tab_name, const IndexMeta &index,
                                                timestamp_t read_ts);
    void EraseSnapshotIndexHistory(const std::string &tab_name, const IndexMeta &index);
    void EraseSnapshotIndexHistory(const std::string &tab_name);
    void ClearSnapshotIndexHistory();
    void EraseTableState(const std::string &tab_name);

    void RecordSerializableRead(Transaction *txn, const std::string &tab_name, const Rid &rid);

    void RecordSerializablePredicateRead(Transaction *txn, const std::string &tab_name,
                                         const std::vector<ColMeta> &cols,
                                         const std::vector<Condition> &conds);

    void RecordSerializableWrite(Transaction *txn, const std::string &tab_name, const Rid &rid,
                                 const RmRecord *old_record, const RmRecord *new_record,
                                 const std::vector<ColMeta> *cols);

    ConcurrencyMode get_concurrency_mode() { return concurrency_mode_; }

    void set_concurrency_mode(ConcurrencyMode concurrency_mode) { concurrency_mode_ = concurrency_mode; }

    LockManager* get_lock_manager() { return lock_manager_; }

    std::vector<CheckpointTxnInfo> CollectActiveTxnCheckpointInfo(Transaction *exclude_txn = nullptr);

    TransactionDrainGuard BlockNewTransactionsAndWait();

    TransactionDrainGuard TryBlockNewTransactionsIfIdle();

    TransactionDrainGuard BlockNewTransactionsAndWaitFor(std::chrono::milliseconds timeout);

    size_t ActiveTransactionCount() const;

    size_t RegisteredTransactionCount() const {
        return transaction_registry_.Size();
    }

    StatementCheckpointGate::ReadGuard EnterStatementExecution() {
        return lifecycle_state_.checkpoint_gate.ReadEnter();
    }

    StatementCheckpointGate::WriteGuard EnterCheckpointExecution() {
        return lifecycle_state_.checkpoint_gate.WriteEnter();
    }

    /**
     * @description: 获取事务ID为txn_id的事务对象
     * @return {Transaction*} 事务对象的指针
     * @param {txn_id_t} txn_id 事务ID
     */    
    Transaction* get_transaction(txn_id_t txn_id) {
        return transaction_registry_.GetThreadOwned(txn_id);
    }
    /** ------------------------以下函数仅可能在MVCC当中使用------------------------------------------*/

    /**
    * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
    * 在更新之前，将调用 `check` 函数以确保有效性。
    */
    bool UpdateUndoLink(const std::string &tab_name, Rid rid, std::optional<UndoLink> prev_link,
                        std::function<bool(std::optional<UndoLink>)> &&check = nullptr);

    /**
     * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
     * 在更新之前，将调用 `check` 函数以确保有效性。
     */
    bool UpdateVersionLink(const std::string &tab_name, Rid rid, std::optional<VersionUndoLink> prev_version,
                           std::function<bool(std::optional<VersionUndoLink>)> &&check = nullptr);

    void InstallTupleVersion(const std::string &tab_name, Rid rid, const VersionUndoLink &version,
                             const TupleMeta &meta);

    /** @brief 获取表堆元组的第一个撤销日志。 */
    std::optional<UndoLink> GetUndoLink(const std::string &tab_name, Rid rid);

    /** @brief 获取表堆元组的第一个撤销日志。*/
    std::optional<VersionUndoLink> GetVersionLink(const std::string &tab_name, Rid rid);

    /** @brief 访问事务撤销日志缓冲区并获取撤销日志。如果事务不存在，返回 nullopt。
     * 如果索引超出范围仍然会抛出异常。 */
    std::optional<UndoLog> GetUndoLogOptional(UndoLink link);

    /** Append an undo log while retaining any cross-transaction predecessor reference. */
    UndoLink AppendUndoLog(Transaction *txn, UndoLog log);

    /** @brief 访问事务撤销日志缓冲区并获取撤销日志。除非访问当前事务缓冲区，
     * 否则应该始终调用此函数以获取撤销日志，而不是手动检索事务 shared_ptr 并访问缓冲区。 */
    UndoLog GetUndoLog(UndoLink link);

    /** @brief 获取系统中的最低读时间戳。 */
    timestamp_t GetWatermark();

    /** @brief 垃圾回收。仅在所有事务都未访问时调用。 */
    void GarbageCollection();


private:
    friend class TransactionDrainGuard;

    // 快路径:原子递增活跃计数,仅当 drain 阻塞置位时才转入持锁等待。
    void ReserveTransactionAdmission();

    using RetiredTuple = TransactionRetiredTuple;
    using RetiredTransaction = TransactionRetiredBatch;

    ConcurrencyMode concurrency_mode_;      // 事务使用的并发控制算法，目前只需要考虑2PL
    SmManager *sm_manager_;
    LockManager *lock_manager_;
    TransactionRegistry transaction_registry_;
    TransactionLifecycleState lifecycle_state_;
    TransactionVersionState version_state_;
    rmdb::SnapshotIndexHistoryStore snapshot_index_history_;

    std::shared_ptr<PageVersionInfo> GetOrCreatePageVersionInfo(const std::string &tab_name, page_id_t page_no);
    std::shared_ptr<PageVersionInfo> GetOrCreatePageVersionInfoOnTable(
        const std::shared_ptr<TableVersionInfo> &table_info, const std::string &tab_name, page_id_t page_no);
    void RetainUndoReference(const UndoLink &link);
    void ReleaseUndoReference(const UndoLink &link);
    void EnqueueRetiredTuples(timestamp_t commit_ts, std::vector<RetiredTuple> tuples);
    bool ReclaimRetiredTuple(timestamp_t commit_ts, const RetiredTuple &retired);
    void QueueFinishedTransactionForGc(Transaction *txn);
    void GarbageCollectFinishedTransactions();
    void FinishTransactionAdmission(Transaction *txn);
    void CancelTransactionAdmissionReservation();
    void ReleaseTransactionDrain();

    TransactionGcState gc_state_;
};

