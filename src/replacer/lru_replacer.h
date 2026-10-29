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
#include <memory>

#include "replacer/replacer.h"

// The historical class name is retained for callers. Internally this is a
// sharded CLOCK policy, avoiding a single global replacement-policy latch.
class LRUReplacer final : public Replacer {
   public:
    explicit LRUReplacer(size_type num_pages);
    ~LRUReplacer() override;

    [[nodiscard]] bool victim(frame_type *result) override;
    void pin(frame_type frame) override;
    void unpin(frame_type frame) override;
    void unpin_cold(frame_type frame) override;
    [[nodiscard]] size_type Size() override;

   private:
    enum class FrameState : rmdb::u8 { Pinned = 0, Cold = 1, Referenced = 2 };

    struct alignas(64) Shard {
        std::atomic<size_type> hand{0};
        size_type frame_count{0};
    };

    [[nodiscard]] bool contains(frame_type frame) const;
    [[nodiscard]] frame_type frame_at(size_type shard, size_type position) const;
    [[nodiscard]] bool try_evict_from(size_type shard, frame_type *result);
    void make_evictable(frame_type frame, FrameState initial_state);

    size_type capacity_;
    size_type shard_count_{1};
    std::atomic<size_type> evictable_count_{0};
    std::atomic<size_type> next_shard_{0};
    std::unique_ptr<Shard[]> shards_;
    std::unique_ptr<std::atomic<rmdb::u8>[]> states_;
};
