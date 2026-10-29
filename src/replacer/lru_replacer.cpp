/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

#include <algorithm>
#include <thread>

namespace {
using size_type = Replacer::size_type;

size_type choose_shard_count(size_type capacity) {
    if (capacity < 64) {
        return 1;
    }
    const size_type reported = std::thread::hardware_concurrency();
    const size_type workers = reported == 0 ? 8 : reported;
    return std::clamp<size_type>(workers * 2, 1, std::min<size_type>(64, capacity));
}
}  // namespace

LRUReplacer::LRUReplacer(size_type num_pages)
    : capacity_(num_pages), shard_count_(choose_shard_count(num_pages)) {
    shards_ = std::make_unique<Shard[]>(shard_count_);
    states_ = std::make_unique<std::atomic<rmdb::u8>[]>(capacity_);
    for (size_type frame = 0; frame < capacity_; ++frame) {
        states_[frame].store(static_cast<rmdb::u8>(FrameState::Pinned), std::memory_order_relaxed);
    }
    for (size_type shard = 0; shard < shard_count_; ++shard) {
        shards_[shard].frame_count = shard < capacity_ ? (capacity_ - 1 - shard) / shard_count_ + 1 : 0;
    }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::contains(frame_type frame) const {
    return frame >= 0 && static_cast<size_type>(frame) < capacity_;
}

LRUReplacer::frame_type LRUReplacer::frame_at(size_type shard, size_type position) const {
    return static_cast<frame_type>(shard + position * shard_count_);
}

bool LRUReplacer::try_evict_from(size_type shard_index, frame_type *result) {
    Shard &shard = shards_[shard_index];
    if (shard.frame_count == 0) {
        return false;
    }

    for (size_type scanned = 0; scanned < shard.frame_count * 2; ++scanned) {
        const size_type position = shard.hand.fetch_add(1, std::memory_order_relaxed) % shard.frame_count;
        const frame_type candidate = frame_at(shard_index, position);
        rmdb::u8 observed = states_[candidate].load(std::memory_order_acquire);
        if (observed == static_cast<rmdb::u8>(FrameState::Pinned)) {
            continue;
        }
        if (observed == static_cast<rmdb::u8>(FrameState::Referenced)) {
            states_[candidate].compare_exchange_strong(observed, static_cast<rmdb::u8>(FrameState::Cold),
                                                       std::memory_order_acq_rel, std::memory_order_acquire);
            continue;
        }
        if (!states_[candidate].compare_exchange_strong(observed, static_cast<rmdb::u8>(FrameState::Pinned),
                                                        std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }
        evictable_count_.fetch_sub(1, std::memory_order_relaxed);
        *result = candidate;
        return true;
    }
    return false;
}

bool LRUReplacer::victim(frame_type *result) {
    if (result == nullptr) {
        return false;
    }
    const size_type first = next_shard_.fetch_add(1, std::memory_order_relaxed) % shard_count_;
    for (size_type offset = 0; offset < shard_count_; ++offset) {
        if (try_evict_from((first + offset) % shard_count_, result)) {
            return true;
        }
    }
    return false;
}

void LRUReplacer::pin(frame_type frame) {
    if (!contains(frame)) {
        return;
    }
    const rmdb::u8 old_state =
        states_[frame].exchange(static_cast<rmdb::u8>(FrameState::Pinned), std::memory_order_acq_rel);
    if (old_state != static_cast<rmdb::u8>(FrameState::Pinned)) {
        evictable_count_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void LRUReplacer::make_evictable(frame_type frame, FrameState initial_state) {
    if (!contains(frame)) {
        return;
    }
    rmdb::u8 expected = static_cast<rmdb::u8>(FrameState::Pinned);
    if (states_[frame].compare_exchange_strong(expected, static_cast<rmdb::u8>(initial_state),
                                               std::memory_order_acq_rel, std::memory_order_acquire)) {
        evictable_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void LRUReplacer::unpin(frame_type frame) {
    make_evictable(frame, FrameState::Referenced);
}

void LRUReplacer::unpin_cold(frame_type frame) {
    make_evictable(frame, FrameState::Cold);
}

LRUReplacer::size_type LRUReplacer::Size() {
    return evictable_count_.load(std::memory_order_relaxed);
}
