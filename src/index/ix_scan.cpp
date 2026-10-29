/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

/**
 * @brief 
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() {
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(current_guard_->is_leaf_page());
    assert(iid_.slot_no < current_guard_->get_size());
    // increment slot no
    iid_.slot_no++;
    while (iid_.slot_no >= current_guard_->get_size()) {
        page_id_t next_leaf = ih_->safe_next_leaf(current_guard_.get());
        if (next_leaf == IX_LEAF_HEADER_PAGE || next_leaf == IX_NO_PAGE) {
            iid_ = end_;
            return;
        }
        current_guard_.reset();
        current_guard_ = ih_->fetch_node_guard(next_leaf, LatchMode::Shared);
        iid_.slot_no = 0;
        iid_.page_no = next_leaf;
        while (current_guard_->is_tombstone() || current_guard_->get_size() == 0) {
            next_leaf = ih_->safe_next_leaf(current_guard_.get());
            if (next_leaf == IX_LEAF_HEADER_PAGE || next_leaf == IX_NO_PAGE) {
                iid_ = end_;
                return;
            }
            current_guard_.reset();
            current_guard_ = ih_->fetch_node_guard(next_leaf, LatchMode::Shared);
            iid_.page_no = next_leaf;
        }
    }
}

int IxScan::current_leaf_scan_end_slot() const {
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(current_guard_->is_leaf_page());
    assert(iid_.slot_no <= current_guard_->get_size());
    if (iid_.page_no == end_.page_no && end_.slot_no < current_guard_->get_size()) {
        return end_.slot_no;
    }
    return current_guard_->get_size();
}

Rid IxScan::current_leaf_rid_at(int slot_no) const {
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(current_guard_->is_leaf_page());
    assert(slot_no >= iid_.slot_no);
    assert(slot_no < current_leaf_scan_end_slot());
    return current_guard_->get_rid(slot_no);
}

const char *IxScan::current_leaf_key_at(int slot_no) const {
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(current_guard_->is_leaf_page());
    assert(slot_no >= iid_.slot_no);
    assert(slot_no < current_leaf_scan_end_slot());
    return current_guard_->get_key(slot_no);
}

void IxScan::advance_current_leaf(int slots) {
    assert(slots >= 0);
    if (slots == 0) {
        return;
    }
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(current_guard_->is_leaf_page());
    const int scan_end_slot = current_leaf_scan_end_slot();
    assert(iid_.slot_no + slots <= scan_end_slot);
    iid_.slot_no += slots;
    if (iid_.slot_no < scan_end_slot) {
        return;
    }
    if (iid_.page_no == end_.page_no && iid_.slot_no == end_.slot_no) {
        return;
    }
    while (iid_.slot_no >= current_guard_->get_size()) {
        page_id_t next_leaf = ih_->safe_next_leaf(current_guard_.get());
        if (next_leaf == IX_LEAF_HEADER_PAGE || next_leaf == IX_NO_PAGE) {
            iid_ = end_;
            return;
        }
        current_guard_.reset();
        current_guard_ = ih_->fetch_node_guard(next_leaf, LatchMode::Shared);
        iid_.slot_no = 0;
        iid_.page_no = next_leaf;
        while (current_guard_->is_tombstone() || current_guard_->get_size() == 0) {
            next_leaf = ih_->safe_next_leaf(current_guard_.get());
            if (next_leaf == IX_LEAF_HEADER_PAGE || next_leaf == IX_NO_PAGE) {
                iid_ = end_;
                return;
            }
            current_guard_.reset();
            current_guard_ = ih_->fetch_node_guard(next_leaf, LatchMode::Shared);
            iid_.page_no = next_leaf;
        }
    }
}

Rid IxScan::rid() const {
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(iid_.slot_no < current_guard_->get_size());
    return current_guard_->get_rid(iid_.slot_no);
}

const char *IxScan::key() const {
    assert(!is_end());
    assert(current_guard_);
    assert(current_guard_.latch_mode() == LatchMode::Shared);
    assert(iid_.slot_no < current_guard_->get_size());
    return current_guard_->get_key(iid_.slot_no);
}
