/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

#include "common/common.h"
#include "recovery/log_manager.h"

#include <chrono>
#include <thread>

namespace {
constexpr size_t kFetchCacheSetCount = 128;
constexpr size_t kFetchCacheWays = 2;

struct FetchCacheEntry {
    const BufferPoolManager *owner = nullptr;
    PageId page_id{0, INVALID_PAGE_ID};
    frame_id_t frame_id = INVALID_FRAME_ID;
    rmdb::u64 generation = 0;
};

struct FetchCacheSet {
    FetchCacheEntry ways[kFetchCacheWays];
    rmdb::u8 next_victim{0};
};

thread_local FetchCacheSet fetch_cache[kFetchCacheSetCount];

size_t fetch_cache_slot(const PageId &page_id) {
    return std::hash<PageId>()(page_id) & (kFetchCacheSetCount - 1);
}
}  // namespace

void ReadPageGuard::Drop() noexcept {
    if (page_ == nullptr) {
        return;
    }
    page_->r_unlatch();
    if (manager_ != nullptr) {
        if (!manager_->unpin_page_fast(page_, page_id_, false)) {
            manager_->unpin_page(page_id_, false);
        }
    }
    manager_ = nullptr;
    page_ = nullptr;
    page_id_ = PageId{-1, INVALID_PAGE_ID};
}

void WritePageGuard::Drop() noexcept {
    if (page_ == nullptr) {
        return;
    }
    page_->w_unlatch();
    if (manager_ != nullptr) {
        if (!manager_->unpin_page_fast(page_, page_id_, dirty_)) {
            manager_->unpin_page(page_id_, dirty_);
        }
    }
    manager_ = nullptr;
    page_ = nullptr;
    page_id_ = PageId{-1, INVALID_PAGE_ID};
    dirty_ = false;
}

Page *WritePageGuard::ReleaseLatchKeepPinned() noexcept {
    if (page_ == nullptr) {
        return nullptr;
    }
    Page *page = page_;
    if (dirty_ && manager_ != nullptr) {
        BufferPoolManager::mark_dirty(page_);
    }
    page_->w_unlatch();
    manager_ = nullptr;
    page_ = nullptr;
    page_id_ = PageId{-1, INVALID_PAGE_ID};
    dirty_ = false;
    return page;
}

ReadPageGuard BufferPoolManager::fetch_page_read(PageId page_id, BufferAccessStrategy *strategy) {
    Page *page = fetch_page(page_id, strategy);
    if (page == nullptr) {
        return {};
    }
    page->r_latch();
    return ReadPageGuard(this, page_id, page);
}

WritePageGuard BufferPoolManager::fetch_page_write(PageId page_id, BufferAccessStrategy *strategy) {
    Page *page = fetch_page(page_id, strategy);
    if (page == nullptr) {
        return {};
    }
    page->w_latch();
    return WritePageGuard(this, page_id, page);
}

BufferAccessClass BufferPoolManager::access_class_from_strategy(BufferAccessStrategy *strategy) {
    return strategy == nullptr ? BufferAccessClass::Default : strategy->cls;
}

bool BufferPoolManager::access_class_uses_ring(BufferAccessClass access_class) const {
    return access_class == BufferAccessClass::BulkRead || access_class == BufferAccessClass::IndexBuild;
}

bool BufferPoolManager::access_class_is_cold(BufferAccessClass access_class) const {
    return access_class == BufferAccessClass::BulkRead || access_class == BufferAccessClass::ColdWrite ||
           access_class == BufferAccessClass::IndexBuild;
}

void BufferPoolManager::set_frame_access_class(frame_id_t frame_id, BufferAccessClass access_class) {
    if (frame_id != INVALID_FRAME_ID && static_cast<size_t>(frame_id) < frame_access_class_.size()) {
        frame_access_class_[frame_id] = static_cast<rmdb::u8>(access_class);
    }
}

void BufferPoolManager::note_frame_access(frame_id_t frame_id, BufferAccessClass access_class) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= frame_access_class_.size() ||
        access_class == BufferAccessClass::BulkRead || access_class == BufferAccessClass::ColdWrite ||
        access_class == BufferAccessClass::IndexBuild) {
        return;
    }
    auto previous_class = static_cast<BufferAccessClass>(frame_access_class_[frame_id]);
    if (access_class == BufferAccessClass::Hot || previous_class != BufferAccessClass::Hot) {
        frame_access_class_[frame_id] = static_cast<rmdb::u8>(access_class);
    }
}

bool BufferPoolManager::try_reuse_strategy_frame(PageId page_id, BufferAccessStrategy *strategy,
                                                 frame_id_t *frame_id) {
    if (strategy == nullptr || frame_id == nullptr || strategy->ring.empty() ||
        !access_class_uses_ring(strategy->cls)) {
        return false;
    }

    const size_t ring_size = strategy->ring.size();
    size_t start = strategy->hand % ring_size;
    for (size_t offset = 0; offset < ring_size; ++offset) {
        size_t slot = (start + offset) % ring_size;
        frame_id_t candidate = strategy->ring[slot];
        if (candidate == INVALID_FRAME_ID || static_cast<size_t>(candidate) >= pool_size_) {
            continue;
        }

        std::scoped_lock<std::mutex> frame_lock(frame_latches_[candidate]);
        Page *page = &pages_[candidate];
        if (page->is_replacing_.load(std::memory_order_relaxed) || page->pin_count_.load(std::memory_order_relaxed) != 0 || page->is_dirty_ ||
            page->id_.page_no == INVALID_PAGE_ID) {
            continue;
        }
        PageId old_page_id = page->id_;
        if (old_page_id == page_id) {
            continue;
        }

        std::unique_lock<std::mutex> shard_lock(page_table_latch_for(old_page_id), std::try_to_lock);
        if (!shard_lock.owns_lock()) {
            continue;
        }
        auto &old_table = page_table_for(old_page_id);
        auto it = old_table.find(old_page_id);
        if (it == old_table.end() || it->second != candidate || page->pin_count_.load(std::memory_order_relaxed) != 0 || page->is_dirty_ ||
            page->is_replacing_.load(std::memory_order_relaxed)) {
            continue;
        }

        begin_frame_transition_locked(old_page_id, candidate);
        *frame_id = candidate;
        strategy->hand = (slot + 1) % ring_size;
        return true;
    }
    return false;
}

void BufferPoolManager::attach_frame_to_strategy_ring(BufferAccessStrategy *strategy, frame_id_t frame_id) {
    if (strategy == nullptr || strategy->ring.empty() || !access_class_uses_ring(strategy->cls) ||
        frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return;
    }
    size_t slot = strategy->hand % strategy->ring.size();
    strategy->ring[slot] = frame_id;
    strategy->hand = (slot + 1) % strategy->ring.size();
}

size_t BufferPoolManager::page_table_shard_for(const PageId &page_id) const {
    return std::hash<PageId>()(page_id) % page_table_shard_count_;
}

std::unordered_map<PageId, frame_id_t, PageIdHash> &BufferPoolManager::page_table_for(const PageId &page_id) {
    return page_table_shards_[page_table_shard_for(page_id)].table;
}

std::mutex &BufferPoolManager::page_table_latch_for(const PageId &page_id) {
    return page_table_shards_[page_table_shard_for(page_id)].latch;
}

std::mutex &BufferPoolManager::page_load_latch_for(const PageId &page_id) {
    return page_load_shards_[page_table_shard_for(page_id)].latch;
}

frame_id_t BufferPoolManager::frame_id_for_page(Page *page) const {
    if (page == nullptr) {
        return INVALID_FRAME_ID;
    }
    auto base = reinterpret_cast<std::uintptr_t>(pages_.get());
    auto ptr = reinterpret_cast<std::uintptr_t>(page);
    auto end = base + sizeof(Page) * pool_size_;
    if (ptr < base || ptr >= end) {
        return INVALID_FRAME_ID;
    }
    auto offset = ptr - base;
    if (offset % sizeof(Page) != 0) {
        return INVALID_FRAME_ID;
    }
    return static_cast<frame_id_t>(offset / sizeof(Page));
}

Page *BufferPoolManager::fetch_page_from_cache(PageId page_id, BufferAccessClass access_class) {
    FetchCacheSet &set = fetch_cache[fetch_cache_slot(page_id)];
    FetchCacheEntry *entry = nullptr;
    for (auto &candidate : set.ways) {
        if (candidate.owner == this && candidate.page_id == page_id &&
            candidate.frame_id != INVALID_FRAME_ID &&
            static_cast<size_t>(candidate.frame_id) < pool_size_) {
            entry = &candidate;
            break;
        }
    }
    if (entry == nullptr) {
        return nullptr;
    }

    // 快速路径：帧未被替换、代次一致且已 pin —— 仅原子递增引用计数，不拿帧锁。
    // 安全依据：替换流程只在 pin_count==0 时开始，此处 +1 后 pin>=1 阻止替换；
    // is_replacing_/generation_ 的 release-store 保证 acquire-load 可见。
    Page *page = &pages_[entry->frame_id];
    if (!page->is_replacing_.load(std::memory_order_acquire) &&
        page->generation_.load(std::memory_order_acquire) == entry->generation) {
        int prev = page->pin_count_.fetch_add(1, std::memory_order_acq_rel);
        if (prev >= 1) {
            note_frame_access(entry->frame_id, access_class);
            return page;
        }
        page->pin_count_.fetch_sub(1, std::memory_order_acq_rel);
    }

    std::scoped_lock<std::mutex> frame_lock(frame_latches_[entry->frame_id]);
    page = &pages_[entry->frame_id];
    if (page->is_replacing_.load(std::memory_order_relaxed) || !(page->id_ == page_id) ||
        page->generation_.load(std::memory_order_relaxed) != entry->generation) {
        *entry = FetchCacheEntry{};
        return nullptr;
    }
    return pin_frame_locked(page_id, entry->frame_id, access_class);
}

BufferPoolManager::DenseFrameChunk *BufferPoolManager::dense_chunk_for(PageId page_id, bool create) {
    if (page_id.fd < 0 || page_id.fd >= DiskManager::MAX_FD || page_id.page_no < 0 ||
        page_id.page_no >= kDenseFramePageLimit) {
        return nullptr;
    }
    auto &dir = dense_frame_dirs_[page_id.fd];
    const size_t chunk_idx = static_cast<size_t>(page_id.page_no) >> kDenseFrameChunkBits;
    DenseFrameChunk *chunk = dir.chunks[chunk_idx].load(std::memory_order_acquire);
    if (chunk != nullptr || !create) {
        return chunk;
    }
    std::scoped_lock<std::mutex> lock(dir.latch);
    chunk = dir.chunks[chunk_idx].load(std::memory_order_acquire);
    if (chunk == nullptr) {
        auto owned_chunk = std::make_unique<DenseFrameChunk>();
        chunk = owned_chunk.get();
        // 先把所有权纳入目录，再用 release-store 发布裸指针。无锁读者一旦看到
        // 非空 chunks 项，对应对象就至少存活到 BufferPoolManager 析构。
        dir.owned_chunks.push_back(std::move(owned_chunk));
        dir.chunks[chunk_idx].store(chunk, std::memory_order_release);
    }
    return chunk;
}

frame_id_t BufferPoolManager::lookup_dense_frame(PageId page_id) {
    DenseFrameChunk *chunk = dense_chunk_for(page_id, false);
    if (chunk == nullptr) {
        return INVALID_FRAME_ID;
    }
    const size_t slot = static_cast<size_t>(page_id.page_no) & (kDenseFrameChunkSize - 1);
    return chunk->frames[slot].load(std::memory_order_acquire);
}

void BufferPoolManager::publish_dense_frame(PageId page_id, frame_id_t frame_id) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return;
    }
    DenseFrameChunk *chunk = dense_chunk_for(page_id, true);
    if (chunk == nullptr) {
        return;
    }
    const size_t slot = static_cast<size_t>(page_id.page_no) & (kDenseFrameChunkSize - 1);
    chunk->frames[slot].store(frame_id, std::memory_order_release);
}

void BufferPoolManager::clear_dense_frame(PageId page_id, frame_id_t expected_frame_id) {
    DenseFrameChunk *chunk = dense_chunk_for(page_id, false);
    if (chunk == nullptr) {
        return;
    }
    const size_t slot = static_cast<size_t>(page_id.page_no) & (kDenseFrameChunkSize - 1);
    if (expected_frame_id == INVALID_FRAME_ID) {
        chunk->frames[slot].store(INVALID_FRAME_ID, std::memory_order_release);
        return;
    }
    frame_id_t current = expected_frame_id;
    chunk->frames[slot].compare_exchange_strong(current, INVALID_FRAME_ID, std::memory_order_acq_rel,
                                                std::memory_order_acquire);
}

Page *BufferPoolManager::fetch_page_from_dense(PageId page_id, BufferAccessClass access_class) {
    frame_id_t frame_id = lookup_dense_frame(page_id);
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return nullptr;
    }

    std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page *page = &pages_[frame_id];
    if (page->is_replacing_.load(std::memory_order_relaxed) || !(page->id_ == page_id)) {
        clear_dense_frame(page_id, frame_id);
        return nullptr;
    }
    return pin_frame_locked(page_id, frame_id, access_class);
}

void BufferPoolManager::remember_fetch_cache(PageId page_id, frame_id_t frame_id, rmdb::u64 generation) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return;
    }
    FetchCacheSet &set = fetch_cache[fetch_cache_slot(page_id)];
    for (auto &entry : set.ways) {
        if (entry.owner == this && entry.page_id == page_id) {
            entry = FetchCacheEntry{this, page_id, frame_id, generation};
            return;
        }
    }
    for (auto &entry : set.ways) {
        if (entry.owner == nullptr || entry.frame_id == INVALID_FRAME_ID) {
            entry = FetchCacheEntry{this, page_id, frame_id, generation};
            return;
        }
    }
    const size_t victim = set.next_victim++ & (kFetchCacheWays - 1);
    set.ways[victim] = FetchCacheEntry{this, page_id, frame_id, generation};
}

void BufferPoolManager::initialize_frame_for_page(Page *page, PageId page_id, bool bump_generation) {
    page->reset_memory();
    page->id_ = page_id;
    page->pin_count_.store(1, std::memory_order_relaxed);
    page->is_dirty_ = false;
    if (bump_generation) {
        page->generation_.store(page->generation_.load(std::memory_order_relaxed) + 1,
                                std::memory_order_relaxed);
    }
    page->is_replacing_.store(true, std::memory_order_release);
}

Page *BufferPoolManager::pin_frame_locked(PageId page_id, frame_id_t frame_id,
                                          BufferAccessClass access_class) {
    Page *page = &pages_[frame_id];
    bool was_unpinned = page->pin_count_.load(std::memory_order_relaxed) == 0;
    page->pin_count_.fetch_add(1, std::memory_order_relaxed);
    if (was_unpinned) {
        mark_frame_pinned_locked(frame_id);
    }
    remember_fetch_cache(page_id, frame_id, page->generation_.load(std::memory_order_relaxed));
    note_frame_access(frame_id, access_class);
    return page;
}

void BufferPoolManager::begin_frame_transition_locked(PageId old_page_id, frame_id_t frame_id) {
    Page *page = &pages_[frame_id];
    mark_frame_pinned_locked(frame_id);
    page->is_replacing_.store(true, std::memory_order_release);
    page->generation_.store(page->generation_.load(std::memory_order_relaxed) + 1,
                            std::memory_order_relaxed);
    page_table_for(old_page_id).erase(old_page_id);
    clear_dense_frame(old_page_id, frame_id);
}

void BufferPoolManager::begin_unmapped_frame_transition_locked(frame_id_t frame_id) {
    Page *page = &pages_[frame_id];
    mark_frame_pinned_locked(frame_id);
    page->is_replacing_.store(true, std::memory_order_release);
    page->generation_.store(page->generation_.load(std::memory_order_relaxed) + 1,
                            std::memory_order_relaxed);
}

void BufferPoolManager::publish_frame_mapping_locked(PageId page_id, frame_id_t frame_id,
                                                      BufferAccessClass access_class) {
    Page *page = &pages_[frame_id];
    set_frame_access_class(frame_id, access_class);
    page_table_for(page_id)[page_id] = frame_id;
    page->is_replacing_.store(false, std::memory_order_release);
    publish_dense_frame(page_id, frame_id);
    remember_fetch_cache(page_id, frame_id, page->generation_.load(std::memory_order_relaxed));
}

void BufferPoolManager::mark_frame_pinned_locked(frame_id_t frame_id) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return;
    }
    FrameEvictState state = frame_evict_states_[frame_id];
    if (state == FrameEvictState::kInReplacer) {
        replacer_->pin(frame_id);
    }
    frame_evict_states_[frame_id] = FrameEvictState::kPinned;
}

void BufferPoolManager::mark_frame_evictable_locked(frame_id_t frame_id) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return;
    }
    if (frame_evict_states_[frame_id] == FrameEvictState::kPinned) {
        frame_evict_states_[frame_id] = FrameEvictState::kPending;
        if (!pending_evictable_queued_[frame_id]) {
            pending_evictable_queued_[frame_id] = 1;
            std::scoped_lock<std::mutex> pending_lock(pending_evictable_latch_);
            pending_evictable_frames_.push_back(frame_id);
        }
    }
}

void BufferPoolManager::mark_frame_reclaimed_locked(frame_id_t frame_id) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return;
    }
    frame_evict_states_[frame_id] = FrameEvictState::kPinned;
}

bool BufferPoolManager::try_publish_frame_to_replacer_locked(frame_id_t frame_id) {
    if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
        return false;
    }
    Page *page = &pages_[frame_id];
    if (frame_evict_states_[frame_id] != FrameEvictState::kPending || page->is_replacing_.load(std::memory_order_relaxed) ||
        page->pin_count_.load(std::memory_order_relaxed) != 0 || page->id_.page_no == INVALID_PAGE_ID) {
        return false;
    }
    auto access_class = static_cast<BufferAccessClass>(frame_access_class_[frame_id]);
    if (access_class_is_cold(access_class)) {
        replacer_->unpin_cold(frame_id);
    } else {
        replacer_->unpin(frame_id);
    }
    frame_evict_states_[frame_id] = FrameEvictState::kInReplacer;
    return true;
}

void BufferPoolManager::reset_frame_to_free_locked(frame_id_t frame_id) {
    Page *page = &pages_[frame_id];
    page->reset_memory();
    page->id_ = PageId{0, INVALID_PAGE_ID};
    page->is_dirty_ = false;
    page->pin_count_.store(0, std::memory_order_relaxed);
    page->is_replacing_.store(false, std::memory_order_release);
    set_frame_access_class(frame_id, BufferAccessClass::Default);
    mark_frame_reclaimed_locked(frame_id);
}

void BufferPoolManager::drain_pending_evictable() {
    std::vector<frame_id_t> frames;
    {
        std::scoped_lock<std::mutex> pending_lock(pending_evictable_latch_);
        if (pending_evictable_frames_.empty()) {
            return;
        }
        frames.swap(pending_evictable_frames_);
    }
    for (frame_id_t frame_id : frames) {
        if (frame_id == INVALID_FRAME_ID || static_cast<size_t>(frame_id) >= pool_size_) {
            continue;
        }
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
        pending_evictable_queued_[frame_id] = 0;
        try_publish_frame_to_replacer_locked(frame_id);
    }
}

void BufferPoolManager::ensure_wal_before_page_flush(Page *page) {
    if (page == nullptr || !page->is_dirty_ || log_manager_ == nullptr) {
        return;
    }
    log_manager_->flush_log_to_disk_until(page->get_page_lsn());
}

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    return find_victim_page(frame_id, nullptr);
}

bool BufferPoolManager::find_victim_page(frame_id_t* frame_id, VictimSource *source) {
    {
        std::scoped_lock<std::mutex> lock(free_list_latch_);
        if (!free_list_.empty()) {
            *frame_id = free_list_.back();
            free_list_.pop_back();
            if (source != nullptr) {
                *source = VictimSource::kFreeList;
            }
            return true;
        }
    }
    drain_pending_evictable();
    if (replacer_->victim(frame_id)) {
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[*frame_id]);
        mark_frame_reclaimed_locked(*frame_id);
        if (source != nullptr) {
            *source = VictimSource::kReplacer;
        }
        return true;
    }
    return false;
}

bool BufferPoolManager::find_wal_safe_victim_page(frame_id_t *frame_id, VictimSource *source) {
    constexpr size_t kMaxCandidates = 128;
    std::vector<frame_id_t> deferred_dirty_frames;
    deferred_dirty_frames.reserve(kMaxCandidates);
    const lsn_t persist_lsn = log_manager_ == nullptr ? INVALID_LSN : log_manager_->get_persist_lsn();

    auto requeue_deferred = [&](size_t begin) {
        for (size_t index = begin; index < deferred_dirty_frames.size(); ++index) {
            frame_id_t deferred = deferred_dirty_frames[index];
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[deferred]);
            Page *page = &pages_[deferred];
            if (frame_evict_states_[deferred] == FrameEvictState::kPinned && !page->is_replacing_.load(std::memory_order_relaxed) &&
                page->pin_count_.load(std::memory_order_relaxed) == 0 && page->id_.page_no != INVALID_PAGE_ID) {
                mark_frame_evictable_locked(deferred);
            }
        }
    };

    for (size_t scanned = 0; scanned < kMaxCandidates; ++scanned) {
        frame_id_t candidate = INVALID_FRAME_ID;
        VictimSource candidate_source;
        if (!find_victim_page(&candidate, &candidate_source)) {
            break;
        }
        if (candidate_source == VictimSource::kFreeList) {
            requeue_deferred(0);
            *frame_id = candidate;
            if (source != nullptr) {
                *source = candidate_source;
            }
            return true;
        }

        bool wal_safe = false;
        {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[candidate]);
            Page *page = &pages_[candidate];
            lsn_t page_lsn = page->get_page_lsn();
            wal_safe = !page->is_dirty_ || log_manager_ == nullptr || page_lsn == INVALID_LSN ||
                       page_lsn <= persist_lsn;
        }
        if (wal_safe) {
            requeue_deferred(0);
            *frame_id = candidate;
            if (source != nullptr) {
                *source = candidate_source;
            }
            return true;
        }
        deferred_dirty_frames.push_back(candidate);
    }

    if (deferred_dirty_frames.empty()) {
        return false;
    }
    *frame_id = deferred_dirty_frames.front();
    if (source != nullptr) {
        *source = VictimSource::kReplacer;
    }
    requeue_deferred(1);
    return true;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    if (page == nullptr || new_frame_id == INVALID_FRAME_ID ||
        static_cast<size_t>(new_frame_id) >= pool_size_ || page != &pages_[new_frame_id]) {
        return;
    }

    PageId old_page_id = page->id_;
    bool need_flush = false;
    if (old_page_id.page_no != INVALID_PAGE_ID) {
        std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(old_page_id));
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[new_frame_id]);
        need_flush = page->is_dirty_;
        begin_frame_transition_locked(old_page_id, new_frame_id);
    } else {
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[new_frame_id]);
        begin_unmapped_frame_transition_locked(new_frame_id);
    }

    std::scoped_lock<std::mutex> frame_lock(frame_latches_[new_frame_id]);
    if (need_flush) {
        ensure_wal_before_page_flush(page);
        disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, page->data_, PAGE_SIZE);
        if (perf_stats_ != nullptr) {
            perf_stats_->RecordDirtyFlush();
        }
        page->is_dirty_ = false;
    }
    initialize_frame_for_page(page, new_page_id, false);
    std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(new_page_id));
    publish_frame_mapping_locked(new_page_id, new_frame_id, BufferAccessClass::Default);
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page *BufferPoolManager::fetch_page(PageId page_id, BufferAccessStrategy *strategy) {
    // A frame that reaches pin_count zero is published to the replacer through
    // a short deferred queue.  Under OLTP concurrency a miss can race that
    // publication and observe no victim even though one is about to become
    // available.  Retry for a bounded interval before surfacing exhaustion.
    constexpr int kFetchAttempts = 24;
    for (int attempt = 0; attempt < kFetchAttempts; ++attempt) {
        if (Page *page = fetch_page_once(page_id, strategy)) {
            return page;
        }
        if (attempt < 7) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
    return nullptr;
}

Page *BufferPoolManager::fetch_page_once(PageId page_id, BufferAccessStrategy *strategy) {
    BufferAccessClass access_class = access_class_from_strategy(strategy);
    if (Page *cached_page = fetch_page_from_cache(page_id, access_class)) {
        if (perf_stats_ != nullptr) {
            perf_stats_->RecordBufferPoolHit();
        }
        return cached_page;
    }
    if (Page *dense_page = fetch_page_from_dense(page_id, access_class)) {
        if (perf_stats_ != nullptr) {
            perf_stats_->RecordBufferPoolHit();
        }
        return dense_page;
    }

    {
        std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
        auto &table = page_table_for(page_id);
        auto it = table.find(page_id);
        if (it != table.end()) {
            frame_id_t frame_id = it->second;
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            Page *page = pin_frame_locked(page_id, frame_id, access_class);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordBufferPoolHit();
            }
            return page;
        }
    }

    // Serialize installation only with other misses in the same PageId shard.
    // Frame and page-table latches independently protect victim transitions,
    // so unrelated disk reads no longer wait behind one global slow path.
    std::scoped_lock load_lock{page_load_latch_for(page_id)};
    {
        std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
        auto &table = page_table_for(page_id);
        auto it = table.find(page_id);
        if (it != table.end()) {
            frame_id_t frame_id = it->second;
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            Page *page = pin_frame_locked(page_id, frame_id, access_class);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordBufferPoolHit();
            }
            return page;
        }
    }

    frame_id_t strategy_frame_id = INVALID_FRAME_ID;
    if (try_reuse_strategy_frame(page_id, strategy, &strategy_frame_id)) {
        Page *page = &pages_[strategy_frame_id];
        {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[strategy_frame_id]);
            initialize_frame_for_page(page, page_id, false);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordBufferPoolMiss();
                perf_stats_->RecordBufferPoolEviction();
            }
            disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
            std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
            publish_frame_mapping_locked(page_id, strategy_frame_id, access_class);
        }
        return page;
    }

    frame_id_t frame_id;
    VictimSource source;
    while (find_wal_safe_victim_page(&frame_id, &source)) {
        Page *page = &pages_[frame_id];
        if (source == VictimSource::kFreeList) {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            begin_unmapped_frame_transition_locked(frame_id);
            initialize_frame_for_page(page, page_id, false);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordBufferPoolMiss();
            }
            disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
            std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
            publish_frame_mapping_locked(page_id, frame_id, access_class);
            attach_frame_to_strategy_ring(strategy, frame_id);
            return page;
        }

        PageId old_page_id = page->id_;
        bool need_flush = false;
        {
            std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(old_page_id));
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            auto &old_table = page_table_for(old_page_id);
            auto it = old_table.find(old_page_id);
            if (it == old_table.end() || it->second != frame_id || page->pin_count_.load(std::memory_order_relaxed) != 0) {
                if (page->pin_count_.load(std::memory_order_relaxed) == 0) {
                    mark_frame_evictable_locked(frame_id);
                }
                continue;
            }
            need_flush = page->is_dirty_;
            begin_frame_transition_locked(old_page_id, frame_id);
        }

        {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            if (need_flush) {
                ensure_wal_before_page_flush(page);
                disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, page->data_, PAGE_SIZE);
                if (perf_stats_ != nullptr) {
                    perf_stats_->RecordDirtyFlush();
                }
                page->is_dirty_ = false;
            }
            initialize_frame_for_page(page, page_id, false);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordBufferPoolMiss();
                perf_stats_->RecordBufferPoolEviction();
            }
            disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
            std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
            publish_frame_mapping_locked(page_id, frame_id, access_class);
            attach_frame_to_strategy_ring(strategy, frame_id);
        }
        return page;
    }
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordPinFailure();
    }
    return nullptr;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
    auto &table = page_table_for(page_id);
    auto it = table.find(page_id);
    if (it == table.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page *page = &pages_[frame_id];
    if (page->pin_count_.load(std::memory_order_relaxed) <= 0) {
        return false;
    }
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    page->pin_count_.fetch_sub(1, std::memory_order_relaxed);
    if (page->pin_count_.load(std::memory_order_relaxed) == 0) {
        mark_frame_evictable_locked(frame_id);
    }
    return true;
}

bool BufferPoolManager::unpin_page_fast(Page *page, PageId expected_page_id, bool is_dirty) {
    frame_id_t frame_id = frame_id_for_page(page);
    if (frame_id == INVALID_FRAME_ID) {
        return false;
    }
    // 快速路径：读请求（非脏）且计数递减后仍 > 0 —— 原子计数，不拿帧锁。
    // 安全依据：替换流程只在 pin_count==0 时开始，递减后仍 >=1 阻止替换。
    if (!is_dirty) {
        int prev = page->pin_count_.fetch_sub(1, std::memory_order_acq_rel);
        if (prev > 1) {
            return true;
        }
        page->pin_count_.fetch_add(1, std::memory_order_acq_rel);
    }
    std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    if (page->is_replacing_.load(std::memory_order_relaxed) || !(page->id_ == expected_page_id) ||
        page->pin_count_.load(std::memory_order_relaxed) <= 0) {
        return false;
    }
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    page->pin_count_.fetch_sub(1, std::memory_order_relaxed);
    if (page->pin_count_.load(std::memory_order_relaxed) == 0) {
        mark_frame_evictable_locked(frame_id);
    }
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
    auto &table = page_table_for(page_id);
    auto it = table.find(page_id);
    if (it == table.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page *page = &pages_[frame_id];
    bool was_dirty = page->is_dirty_;
    ensure_wal_before_page_flush(page);
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    if (was_dirty && perf_stats_ != nullptr) {
        perf_stats_->RecordDirtyFlush();
    }
    page->is_dirty_ = false;
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id, BufferAccessStrategy *strategy) {
    BufferAccessClass access_class = access_class_from_strategy(strategy);
    std::unique_lock<std::mutex> slow_lock{slow_path_latch_};
    frame_id_t frame_id;
    VictimSource source;
    while (find_wal_safe_victim_page(&frame_id, &source)) {
        Page *page = &pages_[frame_id];
        bool need_flush = false;
        PageId old_page_id = page->id_;

        if (source == VictimSource::kReplacer) {
            std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(old_page_id));
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            auto &old_table = page_table_for(old_page_id);
            auto it = old_table.find(old_page_id);
            if (it == old_table.end() || it->second != frame_id || page->pin_count_.load(std::memory_order_relaxed) != 0) {
                if (page->pin_count_.load(std::memory_order_relaxed) == 0) {
                    mark_frame_evictable_locked(frame_id);
                }
                continue;
            }
            need_flush = page->is_dirty_;
            begin_frame_transition_locked(old_page_id, frame_id);
        } else {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            begin_unmapped_frame_transition_locked(frame_id);
        }

        page_id->page_no = disk_manager_->allocate_page(page_id->fd);
        // The victim is now pinned, marked replacing, and detached from its
        // old mapping; the new disk page number is reserved as well.  No
        // other allocator can claim this frame, so do not serialize WAL
        // durability and dirty-page I/O behind the global allocation latch.
        // In particular, a slow fdatasync for this victim must not prevent
        // other threads from allocating from clean victims.
        slow_lock.unlock();
        {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
            if (need_flush) {
                ensure_wal_before_page_flush(page);
                disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, page->data_, PAGE_SIZE);
                if (perf_stats_ != nullptr) {
                    perf_stats_->RecordDirtyFlush();
                }
                page->is_dirty_ = false;
            }
            initialize_frame_for_page(page, *page_id, false);
            if (source == VictimSource::kReplacer && perf_stats_ != nullptr) {
                perf_stats_->RecordBufferPoolEviction();
            }
            std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(*page_id));
            publish_frame_mapping_locked(*page_id, frame_id, access_class);
        }
        return page;
    }
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordPinFailure();
    }
    return nullptr;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    std::scoped_lock slow_lock{slow_path_latch_};
    frame_id_t frame_id = INVALID_FRAME_ID;
    Page *page = nullptr;
    bool need_flush = false;
    {
        std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
        auto &table = page_table_for(page_id);
        auto it = table.find(page_id);
        if (it == table.end()) {
            disk_manager_->deallocate_page(page_id.page_no);
            return true;
        }
        frame_id = it->second;
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
        page = &pages_[frame_id];
        if (page->pin_count_.load(std::memory_order_relaxed) != 0) {
            return false;
        }
        need_flush = page->is_dirty_;
        begin_frame_transition_locked(page_id, frame_id);
    }
    {
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
        if (need_flush) {
            ensure_wal_before_page_flush(page);
            disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordDirtyFlush();
            }
        }
        reset_frame_to_free_locked(frame_id);
    }
    {
        std::scoped_lock<std::mutex> free_lock(free_list_latch_);
        free_list_.push_back(frame_id);
    }
    disk_manager_->deallocate_page(page_id.page_no);
    return true;
}

/**
 * @description: 将buffer_pool中的脏页写回到磁盘
 * @param {int} fd 文件句柄，负数表示所有文件
 * @return 实际写回的脏页数量
 */
size_t BufferPoolManager::flush_all_pages(int fd) {
    std::vector<FlushCandidate> candidates;
    lsn_t max_page_lsn = INVALID_LSN;

    for (size_t shard_idx = 0; shard_idx < page_table_shard_count_; ++shard_idx) {
        std::scoped_lock<std::mutex> shard_lock(page_table_shards_[shard_idx].latch);
        for (auto &entry : page_table_shards_[shard_idx].table) {
            if (fd < 0 || entry.first.fd == fd) {
                frame_id_t frame_id = entry.second;
                std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
                Page *page = &pages_[frame_id];
                if (page->id_ == entry.first && page->is_dirty_) {
                    lsn_t page_lsn = page->get_page_lsn();
                    candidates.push_back(FlushCandidate{entry.first, frame_id, page_lsn});
                    if (page_lsn > max_page_lsn) {
                        max_page_lsn = page_lsn;
                    }
                }
            }
        }
    }

    if (log_manager_ != nullptr && max_page_lsn != INVALID_LSN) {
        log_manager_->flush_log_to_disk_until(max_page_lsn);
    }

    size_t flushed_pages = 0;
    for (const auto &candidate : candidates) {
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[candidate.frame_id]);
        Page *page = &pages_[candidate.frame_id];
        if (page->id_ == candidate.page_id && page->is_dirty_) {
            disk_manager_->write_page(candidate.page_id.fd, candidate.page_id.page_no, page->data_, PAGE_SIZE);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordDirtyFlush();
            }
            page->is_dirty_ = false;
            ++flushed_pages;
        }
    }
    return flushed_pages;
}

size_t BufferPoolManager::flush_unpinned_pages_batch(size_t max_pages, size_t max_frames,
                                                     size_t *next_frame, bool *pass_complete) {
    if (next_frame == nullptr || pass_complete == nullptr || max_pages == 0 || max_frames == 0 ||
        pool_size_ == 0) {
        return 0;
    }

    *pass_complete = false;
    size_t frame = *next_frame < pool_size_ ? *next_frame : 0;
    size_t scanned = 0;
    std::vector<FlushCandidate> candidates;
    candidates.reserve(std::min(max_pages, max_frames));
    const lsn_t persist_lsn = log_manager_ == nullptr ? INVALID_LSN : log_manager_->get_persist_lsn();
    while (scanned < max_frames && candidates.size() < max_pages) {
        {
            std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame]);
            Page *page = &pages_[frame];
            if (!page->is_replacing_.load(std::memory_order_relaxed) && page->pin_count_.load(std::memory_order_relaxed) == 0 && page->id_.page_no != INVALID_PAGE_ID &&
                page->is_dirty_) {
                lsn_t page_lsn = page->get_page_lsn();
                if (log_manager_ == nullptr || page_lsn == INVALID_LSN || page_lsn <= persist_lsn) {
                    candidates.push_back(
                        FlushCandidate{page->id_, static_cast<frame_id_t>(frame), page_lsn});
                }
            }
        }

        ++scanned;
        ++frame;
        if (frame == pool_size_) {
            frame = 0;
            *pass_complete = true;
            break;
        }
    }
    *next_frame = frame;

    size_t flushed = 0;
    for (const auto &candidate : candidates) {
        std::scoped_lock<std::mutex> frame_lock(frame_latches_[candidate.frame_id]);
        Page *page = &pages_[candidate.frame_id];
        if (page->id_ == candidate.page_id && !page->is_replacing_.load(std::memory_order_relaxed) && page->pin_count_.load(std::memory_order_relaxed) == 0 &&
            page->is_dirty_ && page->get_page_lsn() == candidate.page_lsn) {
            disk_manager_->write_page(candidate.page_id.fd, candidate.page_id.page_no, page->data_, PAGE_SIZE);
            if (perf_stats_ != nullptr) {
                perf_stats_->RecordDirtyFlush();
            }
            page->is_dirty_ = false;
            ++flushed;
        }
    }
    return flushed;
}

bool BufferPoolManager::set_page_lsn(PageId page_id, lsn_t page_lsn) {
    std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
    auto &table = page_table_for(page_id);
    auto it = table.find(page_id);
    if (it == table.end()) {
        return false;
    }
    std::scoped_lock<std::mutex> frame_lock(frame_latches_[it->second]);
    Page *page = &pages_[it->second];
    page->set_page_lsn(page_lsn);
    return true;
}

bool BufferPoolManager::finalize_page_write(PageId page_id, lsn_t page_lsn, bool is_dirty) {
    std::scoped_lock<std::mutex> shard_lock(page_table_latch_for(page_id));
    auto &table = page_table_for(page_id);
    auto it = table.find(page_id);
    if (it == table.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page *page = &pages_[frame_id];
    page->set_page_lsn(page_lsn);
    if (page->pin_count_.load(std::memory_order_relaxed) <= 0) {
        return false;
    }
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    page->pin_count_.fetch_sub(1, std::memory_order_relaxed);
    if (page->pin_count_.load(std::memory_order_relaxed) == 0) {
        mark_frame_evictable_locked(frame_id);
    }
    return true;
}

bool BufferPoolManager::finalize_page_write_fast(Page *page, PageId expected_page_id, lsn_t page_lsn, bool is_dirty) {
    frame_id_t frame_id = frame_id_for_page(page);
    if (frame_id == INVALID_FRAME_ID) {
        return false;
    }
    std::scoped_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    if (page->is_replacing_.load(std::memory_order_relaxed) || !(page->id_ == expected_page_id) ||
        page->pin_count_.load(std::memory_order_relaxed) <= 0) {
        return false;
    }
    page->set_page_lsn(page_lsn);
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    page->pin_count_.fetch_sub(1, std::memory_order_relaxed);
    if (page->pin_count_.load(std::memory_order_relaxed) == 0) {
        mark_frame_evictable_locked(frame_id);
    }
    return true;
}
