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
#include <optional>
#include <queue>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<std::pair<ColMeta, bool>> order_cols_;
    std::vector<RmRecord> tuples_;
    size_t cursor_ = 0;
    int limit_ = -1;

    int compare_records(const RmRecord &lhs, const RmRecord &rhs) const {
        for (const auto &order_col : order_cols_) {
            const auto &col = order_col.first;
            int cmp = compare_value(lhs.data + col.offset, rhs.data + col.offset, col.type, col.len);
            if (cmp != 0) {
                return order_col.second ? -cmp : cmp;
            }
        }
        return 0;
    }

    bool output_less(const RmRecord &lhs, const RmRecord &rhs) const {
        return compare_records(lhs, rhs) < 0;
    }

    bool output_worse(const RmRecord &lhs, const RmRecord &rhs) const {
        return compare_records(lhs, rhs) > 0;
    }

    auto materialize_child_tuple() -> std::optional<RmRecord> {
        auto tuple = prev_->ReadTupleView();
        if (!tuple) {
            return std::nullopt;
        }
        RmRecord record(static_cast<int>(len_));
        materialize_tuple_view(*tuple, cols_, &record, len_);
        return record;
    }

    void collect_all_tuples() {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = materialize_child_tuple();
            if (!record.has_value()) {
                break;
            }
            tuples_.push_back(std::move(*record));
        }
    }

    void collect_top_k_tuples() {
        auto worse = [&](const RmRecord &lhs, const RmRecord &rhs) { return output_less(lhs, rhs); };
        std::priority_queue<RmRecord, std::vector<RmRecord>, decltype(worse)> heap(worse);
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = materialize_child_tuple();
            if (!record.has_value()) {
                break;
            }
            if (static_cast<int>(heap.size()) < limit_) {
                heap.push(std::move(*record));
            } else if (output_less(*record, heap.top())) {
                heap.pop();
                heap.push(std::move(*record));
            }
        }
        tuples_.reserve(heap.size());
        while (!heap.empty()) {
            tuples_.push_back(heap.top());
            heap.pop();
        }
    }

    void sort_materialized_tuples() {
        std::sort(tuples_.begin(), tuples_.end(),
                  [&](const RmRecord &lhs, const RmRecord &rhs) { return output_less(lhs, rhs); });
    }

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        order_cols_.push_back({prev_->get_col_offset(sel_cols), is_desc});
    }

    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<std::pair<TabCol, bool>> &order_cols) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        for (auto &order_col : order_cols) {
            order_cols_.push_back({prev_->get_col_offset(order_col.first), order_col.second});
        }
    }

    void set_limit(int limit) { limit_ = limit; }

    void beginTuple() override { 
        tuples_.clear();
        if (limit_ == 0) {
            prev_->beginTuple();
            cursor_ = 0;
            return;
        }
        if (limit_ > 0 && !order_cols_.empty()) {
            collect_top_k_tuples();
        } else {
            collect_all_tuples();
        }
        sort_materialized_tuples();
        cursor_ = 0;
    }

    void nextTuple() override {
        if (cursor_ < tuples_.size()) {
            cursor_++;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (cursor_ >= tuples_.size()) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        memcpy(rec->data, tuples_[cursor_].data, len_);
        return rec;
    }

    const RmRecord *CurrentTuple() const override {
        return cursor_ >= tuples_.size() ? nullptr : &tuples_[cursor_];
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "SortExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
};
