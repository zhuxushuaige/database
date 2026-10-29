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

#include "ix_defs.h"
#include "ix_index_handle.h"

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan {
   public:
    using slot_index_t = int;

   private:
    const IxIndexHandle *ih_;
    Iid iid_;  // 初始为lower（用于遍历的指针）
    Iid end_;  // 初始为upper
    IxReadGuard read_guard_;
    IxNodeHandleGuard current_guard_;

   public:
    IxScan(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, IxNodeHandleGuard current_guard)
        : ih_(ih), iid_(lower), end_(upper), read_guard_(ih), current_guard_(std::move(current_guard)) {
        assert(!current_guard_ || current_guard_.latch_mode() == LatchMode::Shared);
    }

    IxScan(const IxScan &) = delete;
    IxScan &operator=(const IxScan &) = delete;
    IxScan(IxScan &&) = delete;
    IxScan &operator=(IxScan &&) = delete;

    void next() override;

    [[nodiscard]] bool is_end() const override { return iid_ == end_; }

    Rid rid() const override;

    [[nodiscard]] const Iid &iid() const { return iid_; }

    const char *key() const;

    [[nodiscard]] slot_index_t current_leaf_slot() const { return iid_.slot_no; }

    [[nodiscard]] slot_index_t current_leaf_scan_end_slot() const;

    [[nodiscard]] slot_index_t current_leaf_scan_remaining() const {
        return current_leaf_scan_end_slot() - iid_.slot_no;
    }

    [[nodiscard]] Rid current_leaf_rid_at(slot_index_t slot_no) const;

    [[nodiscard]] const char *current_leaf_key_at(slot_index_t slot_no) const;

    void advance_current_leaf(slot_index_t slots);
};
