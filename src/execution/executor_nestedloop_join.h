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
#include "common/common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    std::vector<CompiledCondition> compiled_conds_;
    bool compiled_conds_valid_ = false;
    bool isend;
    bool has_current_ = false;
    mutable RmRecord current_record_;
    TupleView current_view_;
    std::vector<const char *> current_cells_;
    rmdb::RuntimeJoinFeedbackCollector feedback_;

    bool set_current_from_children() {
        auto left_view = left_->CurrentTupleView();
        auto right_view = right_->CurrentTupleView();
        if (left_view == nullptr || right_view == nullptr) {
            return false;
        }
        feedback_.add_right_rows();
        feedback_.add_candidate_pairs();
        const auto &left_cols = left_->cols();
        const auto &right_cols = right_->cols();
        current_cells_.resize(left_cols.size() + right_cols.size());
        size_t out_idx = 0;
        for (size_t i = 0; i < left_cols.size(); ++i) {
            current_cells_[out_idx++] = left_view->cell_at(left_cols[i], i);
        }
        for (size_t i = 0; i < right_cols.size(); ++i) {
            current_cells_[out_idx++] = right_view->cell_at(right_cols[i], i);
        }
        current_view_.record = nullptr;
        current_view_.cells = &current_cells_;
        bool passes = compiled_conds_valid_ ? eval_compiled_conds_view(current_view_, compiled_conds_)
                                            : eval_conds_view(cols_, current_view_, fed_conds_);
        if (passes) {
            feedback_.add_rows_output();
        }
        return passes;
    }

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds,
                           std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        current_record_.Resize(static_cast<int>(len_));
        fed_conds_ = std::move(conds);
        compiled_conds_valid_ = compile_conds(cols_, fed_conds_, &compiled_conds_);
        feedback_.bind(std::move(runtime_feedback));

    }

    void beginTuple() override {
        feedback_.begin();
        isend = false;
        has_current_ = false;
        left_->beginTuple();
        right_->beginTuple();
        if (!left_->is_end()) {
            feedback_.add_left_rows();
        }
        while (!left_->is_end()) {
            while (!right_->is_end()) {
                if (set_current_from_children()) {
                    has_current_ = true;
                    return;
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            if (!left_->is_end()) {
                feedback_.add_left_rows();
            }
            right_->beginTuple();
        }
        isend = true;
        feedback_.flush();
    }

    void nextTuple() override {
        if (isend) {
            return;
        }
        has_current_ = false;
        right_->nextTuple();
        while (!left_->is_end()) {
            while (!right_->is_end()) {
                if (set_current_from_children()) {
                    has_current_ = true;
                    return;
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            if (!left_->is_end()) {
                feedback_.add_left_rows();
            }
            right_->beginTuple();
        }
        isend = true;
        feedback_.flush();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (!has_current_) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        materialize_tuple_view(current_view_, cols_, rec.get(), len_);
        return rec;
    }

    const TupleView *CurrentTupleView() const override { return has_current_ ? &current_view_ : nullptr; }

    const RmRecord *CurrentTuple() const override {
        if (!has_current_) {
            return nullptr;
        }
        materialize_tuple_view(current_view_, cols_, &current_record_, len_);
        return &current_record_;
    }

    bool is_end() const override { return isend; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "NestedLoopJoinExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
};

#include <unordered_map>

#include "common/common.h"
#include "common/common.h"
#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class HashJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t left_len_ = 0;
    size_t right_len_ = 0;
    size_t len_ = 0;
    std::vector<ColMeta> cols_;
    std::vector<Condition> fed_conds_;
    std::vector<CompiledCondition> compiled_conds_;
    bool compiled_conds_valid_ = false;
    std::vector<ExecutionJoinKeyPart> key_parts_;

    std::vector<RmRecord> right_records_;
    std::unordered_map<rmdb::u64, std::vector<size_t>> hash_table_;

    bool isend_ = false;
    bool has_current_ = false;
    AbstractExecutor::TupleViewRef left_tuple_;
    const std::vector<size_t> *current_bucket_ = nullptr;
    size_t bucket_pos_ = 0;
    mutable RmRecord current_record_;
    TupleView current_view_;
    std::vector<const char *> current_cells_;
    rmdb::RuntimeJoinFeedbackCollector feedback_;

    static rmdb::u64 append_hash_bytes(rmdb::u64 hash, const char *data, size_t size) {
        constexpr rmdb::u64 kFnvPrime = 1099511628211ULL;
        for (size_t i = 0; i < size; ++i) {
            hash ^= static_cast<unsigned char>(data[i]);
            hash *= kFnvPrime;
        }
        return hash;
    }

    rmdb::u64 build_hash_from_left(const TupleView &view) const {
        rmdb::u64 hash = 14695981039346656037ULL;
        for (const auto &part : key_parts_) {
            hash = append_hash_bytes(hash, view.cell_at(part.left_col, part.left_idx),
                                     static_cast<size_t>(part.left_col.len));
        }
        return hash;
    }

    rmdb::u64 build_hash_from_right_record(const RmRecord &record) const {
        rmdb::u64 hash = 14695981039346656037ULL;
        for (const auto &part : key_parts_) {
            hash = append_hash_bytes(hash, record.data + part.right_col.offset,
                                     static_cast<size_t>(part.right_col.len));
        }
        return hash;
    }

    void build_right_hash_table() {
        right_records_.clear();
        hash_table_.clear();
        right_->beginTuple();
        const auto &right_cols = right_->cols();
        for (; !right_->is_end(); right_->nextTuple()) {
            auto tuple = right_->ReadTupleView();
            if (!tuple) {
                continue;
            }
            RmRecord record(static_cast<int>(right_len_));
            materialize_tuple_view(*tuple, right_cols, &record, right_len_);
            rmdb::u64 hash = build_hash_from_right_record(record);
            size_t idx = right_records_.size();
            right_records_.push_back(std::move(record));
            hash_table_[hash].push_back(idx);
            feedback_.add_right_rows();
        }
    }

    void load_matches_for_left() {
        current_bucket_ = nullptr;
        bucket_pos_ = 0;
        left_tuple_ = {};
        if (left_->is_end()) {
            return;
        }
        left_tuple_ = left_->ReadTupleView();
        if (!left_tuple_) {
            return;
        }
        feedback_.add_left_rows();
        rmdb::u64 hash = build_hash_from_left(*left_tuple_);
        auto it = hash_table_.find(hash);
        if (it != hash_table_.end()) {
            current_bucket_ = &it->second;
        }
    }

    bool set_current_from_right_record(const RmRecord &right_record) {
        if (!left_tuple_) {
            return false;
        }
        const auto &left_cols = left_->cols();
        const auto &right_cols = right_->cols();
        current_cells_.resize(left_cols.size() + right_cols.size());
        size_t out_idx = 0;
        for (size_t i = 0; i < left_cols.size(); ++i) {
            current_cells_[out_idx++] = left_tuple_->cell_at(left_cols[i], i);
        }
        for (const auto &col : right_cols) {
            current_cells_[out_idx++] = right_record.data + col.offset;
        }
        current_view_.record = nullptr;
        current_view_.cells = &current_cells_;
        return compiled_conds_valid_ ? eval_compiled_conds_view(current_view_, compiled_conds_)
                                     : eval_conds_view(cols_, current_view_, fed_conds_);
    }

    bool advance_to_match() {
        while (!left_->is_end()) {
            while (current_bucket_ != nullptr && bucket_pos_ < current_bucket_->size()) {
                const auto &right_record = right_records_[(*current_bucket_)[bucket_pos_++]];
                feedback_.add_candidate_pairs();
                if (set_current_from_right_record(right_record)) {
                    feedback_.add_rows_output();
                    has_current_ = true;
                    return true;
                }
            }
            left_->nextTuple();
            load_matches_for_left();
        }
        return false;
    }

   public:
    HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                     std::vector<Condition> conds,
                     std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        left_ = std::move(left);
        right_ = std::move(right);
        left_len_ = left_->tupleLen();
        right_len_ = right_->tupleLen();
        len_ = left_len_ + right_len_;
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += static_cast<int>(left_len_);
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
        compiled_conds_valid_ = compile_conds(cols_, fed_conds_, &compiled_conds_);
        key_parts_ = BuildEquiJoinKeyParts(left_->cols(), right_->cols(), fed_conds_);
        current_record_.Resize(static_cast<int>(len_));
        feedback_.bind(std::move(runtime_feedback));
    }

    void beginTuple() override {
        feedback_.begin();
        isend_ = false;
        has_current_ = false;
        build_right_hash_table();
        left_->beginTuple();
        load_matches_for_left();
        if (!advance_to_match()) {
            isend_ = true;
            feedback_.flush();
        }
    }

    void nextTuple() override {
        if (isend_) {
            return;
        }
        has_current_ = false;
        if (!advance_to_match()) {
            isend_ = true;
            feedback_.flush();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (!has_current_) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        materialize_tuple_view(current_view_, cols_, rec.get(), len_);
        return rec;
    }

    const TupleView *CurrentTupleView() const override { return has_current_ ? &current_view_ : nullptr; }

    const RmRecord *CurrentTuple() const override {
        if (!has_current_) {
            return nullptr;
        }
        materialize_tuple_view(current_view_, cols_, &current_record_, len_);
        return &current_record_;
    }

    bool is_end() const override { return isend_; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "HashJoinExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
};

#include "common/common.h"
#include "executor_abstract.h"

class SemiJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    std::vector<Condition> conds_;
    std::vector<ColMeta> cols_;
    std::vector<ColMeta> combined_cols_;
    std::vector<CompiledCondition> compiled_conds_;
    bool compiled_conds_valid_ = false;
    size_t len_;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t cursor_;
    TupleView combined_view_;
    std::vector<const char *> combined_cells_;
    rmdb::RuntimeJoinFeedbackCollector feedback_;

   public:
    SemiJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                     std::vector<Condition> conds,
                     std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        cols_ = left_->cols();
        combined_cols_ = cols_;
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
            combined_cols_.push_back(col);
        }
        compiled_conds_valid_ = compile_conds(combined_cols_, conds_, &compiled_conds_);
        len_ = left_->tupleLen();
        cursor_ = 0;
        feedback_.bind(std::move(runtime_feedback));
    }

    void beginTuple() override {
        feedback_.begin();
        tuples_.clear();
        for (left_->beginTuple(); !left_->is_end(); left_->nextTuple()) {
            auto left_rec = left_->ReadTupleView();
            if (!left_rec) {
                break;
            }
            feedback_.add_left_rows();
            bool matched = false;
            for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
                auto right_rec = right_->ReadTupleView();
                if (!right_rec) {
                    break;
                }
                feedback_.add_right_rows();
                feedback_.add_candidate_pairs();
                const auto &left_cols = left_->cols();
                const auto &right_cols = right_->cols();
                combined_cells_.resize(left_cols.size() + right_cols.size());
                size_t out_idx = 0;
                for (size_t i = 0; i < left_cols.size(); ++i) {
                    combined_cells_[out_idx++] = left_rec->cell_at(left_cols[i], i);
                }
                for (size_t i = 0; i < right_cols.size(); ++i) {
                    combined_cells_[out_idx++] = right_rec->cell_at(right_cols[i], i);
                }
                combined_view_.record = nullptr;
                combined_view_.cells = &combined_cells_;
                bool pass = compiled_conds_valid_ ? eval_compiled_conds_view(combined_view_, compiled_conds_)
                                                  : eval_conds_view(combined_cols_, combined_view_, conds_);
                if (pass) {
                    matched = true;
                    break;
                }
            }
            if (matched) {
                feedback_.add_rows_output();
                auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
                materialize_tuple_view(*left_rec, cols_, rec.get(), len_);
                tuples_.push_back(std::move(rec));
            }
        }
        cursor_ = 0;
        feedback_.flush();
    }

    void nextTuple() override {
        if (cursor_ < tuples_.size()) {
            cursor_++;
        }
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "SemiJoinExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
    std::unique_ptr<RmRecord> Next() override {
        if (cursor_ >= tuples_.size()) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        memcpy(rec->data, tuples_[cursor_]->data, len_);
        return rec;
    }
    const RmRecord *CurrentTuple() const override {
        return cursor_ >= tuples_.size() ? nullptr : tuples_[cursor_].get();
    }
};

#include <algorithm>

#include "common/common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "execution_common.h"
#include "index/ix.h"
#include "system/sm.h"

class SortMergeJoinExecutor : public AbstractExecutor {
   private:
    struct StoredRecord {
        RmRecord record;
    };

    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t left_len_ = 0;
    size_t right_len_ = 0;
    size_t len_ = 0;
    std::vector<ColMeta> cols_;
    std::vector<Condition> fed_conds_;
    std::vector<CompiledCondition> compiled_conds_;
    bool compiled_conds_valid_ = false;
    std::vector<ExecutionJoinKeyPart> key_parts_;

    std::vector<StoredRecord> left_records_;
    std::vector<StoredRecord> right_records_;
    std::vector<size_t> left_order_;
    std::vector<size_t> right_order_;
    size_t left_pos_ = 0;
    size_t right_pos_ = 0;
    size_t left_group_begin_ = 0;
    size_t left_group_end_ = 0;
    size_t right_group_begin_ = 0;
    size_t right_group_end_ = 0;
    size_t left_pair_pos_ = 0;
    size_t right_pair_pos_ = 0;
    size_t current_left_idx_ = 0;
    size_t current_right_idx_ = 0;

    bool isend_ = false;
    bool has_current_ = false;
    mutable RmRecord current_record_;
    TupleView current_view_;
    std::vector<const char *> current_cells_;
    std::vector<const char *> eval_cells_;
    rmdb::RuntimeJoinFeedbackCollector feedback_;

    void materialize_child(AbstractExecutor *child, size_t tuple_len, std::vector<StoredRecord> *records) {
        records->clear();
        const auto &child_cols = child->cols();
        child->beginTuple();
        for (; !child->is_end(); child->nextTuple()) {
            auto tuple = child->ReadTupleView();
            if (!tuple) {
                continue;
            }
            StoredRecord stored;
            stored.record.Resize(static_cast<int>(tuple_len));
            materialize_tuple_view(*tuple, child_cols, &stored.record, tuple_len);
            records->push_back(std::move(stored));
        }
    }

    int compare_left_records(size_t lhs_idx, size_t rhs_idx) const {
        const auto &lhs = left_records_[lhs_idx].record;
        const auto &rhs = left_records_[rhs_idx].record;
        for (const auto &part : key_parts_) {
            int cmp = compare_value(lhs.data + part.left_col.offset, rhs.data + part.left_col.offset,
                                    part.left_col.type, part.left_col.len);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    int compare_right_records(size_t lhs_idx, size_t rhs_idx) const {
        const auto &lhs = right_records_[lhs_idx].record;
        const auto &rhs = right_records_[rhs_idx].record;
        for (const auto &part : key_parts_) {
            int cmp = compare_value(lhs.data + part.right_col.offset, rhs.data + part.right_col.offset,
                                    part.right_col.type, part.right_col.len);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    int compare_left_right(size_t left_idx, size_t right_idx) const {
        const auto &left = left_records_[left_idx].record;
        const auto &right = right_records_[right_idx].record;
        for (const auto &part : key_parts_) {
            int cmp = compare_value(left.data + part.left_col.offset, right.data + part.right_col.offset,
                                    part.left_col.type, part.left_col.len);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    bool pair_passes_conditions(const RmRecord &left_record, const RmRecord &right_record) {
        feedback_.add_candidate_pairs();
        const auto &left_cols = left_->cols();
        const auto &right_cols = right_->cols();
        eval_cells_.resize(left_cols.size() + right_cols.size());
        size_t out_idx = 0;
        for (const auto &col : left_cols) {
            eval_cells_[out_idx++] = left_record.data + col.offset;
        }
        for (const auto &col : right_cols) {
            eval_cells_[out_idx++] = right_record.data + col.offset;
        }
        TupleView view;
        view.record = nullptr;
        view.cells = &eval_cells_;
        return compiled_conds_valid_ ? eval_compiled_conds_view(view, compiled_conds_)
                                     : eval_conds_view(cols_, view, fed_conds_);
    }

    template <typename Compare>
    size_t group_end(const std::vector<size_t> &order, size_t begin, Compare compare) const {
        size_t end = begin + 1;
        while (end < order.size() && compare(order[begin], order[end]) == 0) {
            ++end;
        }
        return end;
    }

    void build_sorted_inputs() {
        materialize_child(left_.get(), left_len_, &left_records_);
        materialize_child(right_.get(), right_len_, &right_records_);
        feedback_.add_left_rows(left_records_.size());
        feedback_.add_right_rows(right_records_.size());
        left_order_.clear();
        right_order_.clear();
        if (left_records_.empty() || right_records_.empty()) {
            return;
        }

        left_order_.resize(left_records_.size());
        right_order_.resize(right_records_.size());
        for (size_t i = 0; i < left_order_.size(); ++i) {
            left_order_[i] = i;
        }
        for (size_t i = 0; i < right_order_.size(); ++i) {
            right_order_[i] = i;
        }
        std::stable_sort(left_order_.begin(), left_order_.end(), [&](size_t lhs, size_t rhs) {
            return compare_left_records(lhs, rhs) < 0;
        });
        std::stable_sort(right_order_.begin(), right_order_.end(), [&](size_t lhs, size_t rhs) {
            return compare_right_records(lhs, rhs) < 0;
        });
    }

    bool find_next_equal_group() {
        while (left_pos_ < left_order_.size() && right_pos_ < right_order_.size()) {
            int cmp = compare_left_right(left_order_[left_pos_], right_order_[right_pos_]);
            if (cmp < 0) {
                left_pos_ = group_end(left_order_, left_pos_, [&](size_t lhs, size_t rhs) {
                    return compare_left_records(lhs, rhs);
                });
                continue;
            }
            if (cmp > 0) {
                right_pos_ = group_end(right_order_, right_pos_, [&](size_t lhs, size_t rhs) {
                    return compare_right_records(lhs, rhs);
                });
                continue;
            }

            left_group_begin_ = left_pos_;
            left_group_end_ = group_end(left_order_, left_pos_, [&](size_t lhs, size_t rhs) {
                return compare_left_records(lhs, rhs);
            });
            right_group_begin_ = right_pos_;
            right_group_end_ = group_end(right_order_, right_pos_, [&](size_t lhs, size_t rhs) {
                return compare_right_records(lhs, rhs);
            });
            left_pair_pos_ = left_group_begin_;
            right_pair_pos_ = right_group_begin_;
            left_pos_ = left_group_end_;
            right_pos_ = right_group_end_;
            return true;
        }
        return false;
    }

    bool advance_to_match() {
        while (true) {
            while (left_pair_pos_ < left_group_end_) {
                while (right_pair_pos_ < right_group_end_) {
                    size_t left_idx = left_order_[left_pair_pos_];
                    size_t right_idx = right_order_[right_pair_pos_++];
                    if (pair_passes_conditions(left_records_[left_idx].record, right_records_[right_idx].record)) {
                        feedback_.add_rows_output();
                        current_left_idx_ = left_idx;
                        current_right_idx_ = right_idx;
                        set_current_from_pair();
                        return true;
                    }
                }
                ++left_pair_pos_;
                right_pair_pos_ = right_group_begin_;
            }
            if (!find_next_equal_group()) {
                return false;
            }
        }
    }

    void set_current_from_pair() {
        const auto &left_record = left_records_[current_left_idx_].record;
        const auto &right_record = right_records_[current_right_idx_].record;
        const auto &left_cols = left_->cols();
        const auto &right_cols = right_->cols();
        current_cells_.resize(left_cols.size() + right_cols.size());
        size_t out_idx = 0;
        for (const auto &col : left_cols) {
            current_cells_[out_idx++] = left_record.data + col.offset;
        }
        for (const auto &col : right_cols) {
            current_cells_[out_idx++] = right_record.data + col.offset;
        }
        current_view_.record = nullptr;
        current_view_.cells = &current_cells_;
        has_current_ = true;
    }

   public:
    SortMergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                          std::vector<Condition> conds,
                          std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        left_ = std::move(left);
        right_ = std::move(right);
        left_len_ = left_->tupleLen();
        right_len_ = right_->tupleLen();
        len_ = left_len_ + right_len_;
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += static_cast<int>(left_len_);
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
        compiled_conds_valid_ = compile_conds(cols_, fed_conds_, &compiled_conds_);
        key_parts_ = BuildEquiJoinKeyParts(left_->cols(), right_->cols(), fed_conds_);
        current_record_.Resize(static_cast<int>(len_));
        feedback_.bind(std::move(runtime_feedback));
    }

    void beginTuple() override {
        feedback_.begin();
        isend_ = false;
        has_current_ = false;
        left_pos_ = 0;
        right_pos_ = 0;
        left_group_begin_ = 0;
        left_group_end_ = 0;
        right_group_begin_ = 0;
        right_group_end_ = 0;
        left_pair_pos_ = 0;
        right_pair_pos_ = 0;
        build_sorted_inputs();
        if (!find_next_equal_group() || !advance_to_match()) {
            isend_ = true;
            feedback_.flush();
            return;
        }
    }

    void nextTuple() override {
        if (isend_) {
            return;
        }
        has_current_ = false;
        if (!advance_to_match()) {
            isend_ = true;
            feedback_.flush();
            return;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (!has_current_) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        materialize_tuple_view(current_view_, cols_, rec.get(), len_);
        return rec;
    }

    const TupleView *CurrentTupleView() const override { return has_current_ ? &current_view_ : nullptr; }

    const RmRecord *CurrentTuple() const override {
        if (!has_current_) {
            return nullptr;
        }
        materialize_tuple_view(current_view_, cols_, &current_record_, len_);
        return &current_record_;
    }

    bool is_end() const override { return isend_; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "SortMergeJoinExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
};

#include <algorithm>
#include <iterator>
#include <unordered_set>

#include "index/ix.h"
#include "common/common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexNestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;
    std::string right_tab_name_;
    std::string right_visible_name_;
    TabMeta right_tab_;
    RmFileHandle *right_fh_;
    RmRecordPageCursor right_page_cursor_;
    IndexMeta index_meta_;
    std::vector<Condition> fed_conds_;
    std::vector<ColMeta> cols_;
    std::vector<ColMeta> right_full_cols_;
    std::vector<ColMeta> right_output_cols_;
    std::vector<ColMeta> right_output_source_cols_;
    std::vector<ColMeta> eval_cols_;
    std::vector<CompiledCondition> compiled_conds_;
    bool compiled_conds_valid_ = false;
    size_t left_len_;
    size_t right_len_;
    size_t right_full_len_;
    size_t len_;
    SmManager *sm_manager_;
    Context *context_;
    IxIndexHandle *ih_{nullptr};
    std::shared_ptr<TransactionManager::TableVersionInfo> right_table_version_info_;

    bool isend_ = false;
    AbstractExecutor::TupleViewRef left_tuple_;
    std::vector<Rid> matched_rids_;
    size_t cursor_ = 0;
    bool has_current_ = false;
    mutable RmRecord current_record_;
    RmRecord current_right_record_;
    TupleView current_view_;
    std::vector<const char *> current_cells_;
    std::vector<const char *> eval_cells_;
    std::string local_index_key_scratch_;
    std::string *index_key_scratch_ = &local_index_key_scratch_;
    rmdb::RuntimeJoinFeedbackCollector feedback_;

    int build_key_from_left_view(const TupleView &left_view, char *key) {
        int offset = 0;
        const auto &left_cols = left_->cols();
        for (auto &index_col : index_meta_.cols) {
            bool found = false;
            for (auto &cond : fed_conds_) {
                if (cond.op != OP_EQ) {
                    continue;
                }
                if (cond.is_rhs_val) {
                    if (cond.lhs_col.tab_name == right_visible_name_ &&
                        cond.lhs_col.col_name == index_col.name && cond.rhs_val.raw != nullptr) {
                        memcpy(key + offset, cond.rhs_val.raw->data, index_col.len);
                        found = true;
                        break;
                    }
                    continue;
                }
                TabCol left_col;
                if (cond.rhs_col.tab_name == right_visible_name_ && cond.rhs_col.col_name == index_col.name) {
                    left_col = cond.lhs_col;
                } else if (cond.lhs_col.tab_name == right_visible_name_ && cond.lhs_col.col_name == index_col.name) {
                    left_col = cond.rhs_col;
                } else {
                    continue;
                }
                auto left_meta = get_col(left_cols, left_col);
                memcpy(key + offset, left_view.cell_at(*left_meta, static_cast<size_t>(left_meta - left_cols.begin())),
                       index_col.len);
                found = true;
                break;
            }
            if (!found) {
                break;
            }
            offset += index_col.len;
        }
        return offset;
    }

    bool right_record_passes(const RmRecord &right_rec) {
        if (!left_tuple_) {
            return false;
        }
        const auto &left_cols = left_->cols();
        eval_cells_.resize(left_cols.size() + right_full_cols_.size());
        size_t eval_idx = 0;
        for (size_t i = 0; i < left_cols.size(); ++i) {
            eval_cells_[eval_idx++] = left_tuple_->cell_at(left_cols[i], i);
        }
        for (const auto &col : right_full_cols_) {
            eval_cells_[eval_idx++] = right_rec.data + col.offset;
        }
        TupleView eval_view;
        eval_view.record = nullptr;
        eval_view.cells = &eval_cells_;
        return compiled_conds_valid_ ? eval_compiled_conds_view(eval_view, compiled_conds_)
                                     : eval_conds_view(eval_cols_, eval_view, fed_conds_);
    }

    void set_current_output(const RmRecord &right_rec) {
        const auto &left_cols = left_->cols();
        current_cells_.resize(left_cols.size() + right_output_source_cols_.size());
        size_t out_idx = 0;
        for (size_t i = 0; i < left_cols.size(); ++i) {
            current_cells_[out_idx++] = left_tuple_->cell_at(left_cols[i], i);
        }
        for (const auto &col : right_output_source_cols_) {
            current_cells_[out_idx++] = right_rec.data + col.offset;
        }
        current_view_.record = nullptr;
        current_view_.cells = &current_cells_;
    }

    void load_matches_for_left() {
        matched_rids_.clear();
        cursor_ = 0;
        if (left_->is_end()) {
            return;
        }
        left_tuple_ = left_->ReadTupleView();
        if (!left_tuple_) {
            return;
        }
        feedback_.add_left_rows();
        index_key_scratch_->resize(index_meta_.col_tot_len);
        std::fill(index_key_scratch_->begin(), index_key_scratch_->end(), '\0');
        int prefix_len = build_key_from_left_view(*left_tuple_, index_key_scratch_->data());
        if (prefix_len > 0) {
            if (index_meta_.unique && prefix_len == index_meta_.col_tot_len) {
                ih_->get_value(index_key_scratch_->data(), &matched_rids_,
                               context_ == nullptr ? nullptr : context_->txn_);
            } else {
                ih_->get_prefix_values(index_key_scratch_->data(), prefix_len, &matched_rids_);
            }
            feedback_.add_right_rows(matched_rids_.size());
        }
    }

    bool advance_to_match() {
        while (!left_->is_end()) {
            while (cursor_ < matched_rids_.size()) {
                Rid rid = matched_rids_[cursor_];
                if (context_ == nullptr || context_->txn_mgr_ == nullptr ||
                    !context_->txn_mgr_->GetVisibleTupleInto(right_tab_name_, right_table_version_info_, rid,
                                                             context_->txn_,
                                                             &current_right_record_, nullptr,
                                                             &right_page_cursor_)) {
                    cursor_++;
                    continue;
                }
                cursor_++;
                feedback_.add_candidate_pairs();
                if (right_record_passes(current_right_record_)) {
                    set_current_output(current_right_record_);
                    feedback_.add_rows_output();
                    has_current_ = true;
                    return true;
                }
            }
            left_->nextTuple();
            load_matches_for_left();
        }
        right_page_cursor_.reset();
        return false;
    }

   public:
    int CountDistinctRightInt(const TabCol &target) {
        auto target_col = get_col(right_full_cols_, target);
        if (target_col->type != TYPE_INT || target_col->len != static_cast<int>(sizeof(int))) {
            throw InternalError("fused index join distinct count requires an INT right column");
        }

        std::unordered_set<int> values;
        feedback_.begin();
        right_page_cursor_.reset();
        left_->beginTuple();
        load_matches_for_left();
        while (!left_->is_end()) {
            while (cursor_ < matched_rids_.size()) {
                const Rid rid = matched_rids_[cursor_++];
                if (context_ == nullptr || context_->txn_mgr_ == nullptr ||
                    !context_->txn_mgr_->GetVisibleTupleInto(right_tab_name_, right_table_version_info_, rid,
                                                             context_->txn_,
                                                             &current_right_record_, nullptr,
                                                             &right_page_cursor_)) {
                    continue;
                }
                feedback_.add_candidate_pairs();
                if (right_record_passes(current_right_record_)) {
                    values.insert(rmdb::load_unaligned<int>(current_right_record_.data + target_col->offset));
                    feedback_.add_rows_output();
                }
            }
            left_->nextTuple();
            load_matches_for_left();
        }
        right_page_cursor_.reset();
        feedback_.flush();
        return static_cast<int>(values.size());
    }

    IndexNestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, SmManager *sm_manager,
                                const std::string &right_tab_name, std::vector<Condition> join_conds,
                                std::vector<Condition> right_conds,
                                std::vector<std::string> index_col_names, Context *context,
                                std::string right_visible_name = "", std::vector<TabCol> right_required_cols = {},
                                std::shared_ptr<const PlanRuntimeCache> runtime_cache = nullptr,
                                std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        left_ = std::move(left);
        sm_manager_ = sm_manager;
        context_ = context;
        right_tab_name_ = right_tab_name;
        right_visible_name_ = right_visible_name.empty() ? right_tab_name_ : std::move(right_visible_name);
        const bool use_cache = runtime_cache != nullptr && runtime_cache->has_table;
        right_tab_ = sm_manager_->db_.get_table(right_tab_name_);
        right_fh_ = sm_manager_->fhs_.at(right_tab_name_).get();
        right_page_cursor_.bind(right_fh_);
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            right_table_version_info_ = context_->txn_mgr_->GetOrCreateTableVersionInfo(right_tab_name_);
        }
        index_meta_ = runtime_cache != nullptr && runtime_cache->has_index
                          ? runtime_cache->index_meta
                          : *(right_tab_.get_index_meta(index_col_names, true));
        if (context_ != nullptr) {
            if (auto *scratch = context_->acquire_statement_string(index_meta_.col_tot_len); scratch != nullptr) {
                index_key_scratch_ = scratch;
            }
        }
        ih_ = rmdb::resolve_index_handle(sm_manager_, right_tab_name_, index_meta_);
        fed_conds_ = std::move(join_conds);
        fed_conds_.insert(fed_conds_.end(), std::make_move_iterator(right_conds.begin()),
                          std::make_move_iterator(right_conds.end()));

        left_len_ = left_->tupleLen();
        if (use_cache) {
            right_full_cols_ = runtime_cache->full_cols;
            right_full_len_ = runtime_cache->full_len;
        } else {
            right_full_cols_ = right_tab_.cols;
            for (auto &col : right_full_cols_) {
                col.tab_name = right_visible_name_;
            }
            right_full_len_ = right_full_cols_.back().offset + right_full_cols_.back().len;
        }
        if (right_required_cols.empty()) {
            right_output_cols_ = right_full_cols_;
            right_output_source_cols_ = right_full_cols_;
            right_len_ = right_full_len_;
        } else {
            build_column_projection(right_full_cols_, right_required_cols, &right_output_cols_,
                                    &right_output_source_cols_, &right_len_);
            if (right_output_cols_.empty()) {
                right_output_cols_ = right_full_cols_;
                right_output_source_cols_ = right_full_cols_;
                right_len_ = right_full_len_;
            }
        }
        len_ = left_len_ + right_len_;
        cols_ = left_->cols();
        for (auto &col : right_output_cols_) {
            col.offset += left_len_;
            cols_.push_back(col);
        }
        eval_cols_ = left_->cols();
        auto adjusted_full = right_full_cols_;
        for (auto &col : adjusted_full) {
            col.offset += static_cast<int>(left_len_);
            eval_cols_.push_back(col);
        }
        compiled_conds_valid_ = compile_conds(eval_cols_, fed_conds_, &compiled_conds_);
        current_record_.Resize(static_cast<int>(len_));
        feedback_.bind(std::move(runtime_feedback));
    }

    void beginTuple() override {
        feedback_.begin();
        isend_ = false;
        has_current_ = false;
        right_page_cursor_.reset();
        left_->beginTuple();
        load_matches_for_left();
        if (!advance_to_match()) {
            isend_ = true;
            feedback_.flush();
        }
    }

    void nextTuple() override {
        if (isend_) {
            return;
        }
        has_current_ = false;
        if (!advance_to_match()) {
            isend_ = true;
            feedback_.flush();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (!has_current_) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        materialize_tuple_view(current_view_, cols_, rec.get(), len_);
        return rec;
    }
    const TupleView *CurrentTupleView() const override { return has_current_ ? &current_view_ : nullptr; }
    const RmRecord *CurrentTuple() const override {
        if (!has_current_) {
            return nullptr;
        }
        materialize_tuple_view(current_view_, cols_, &current_record_, len_);
        return &current_record_;
    }
    bool is_end() const override { return isend_; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "IndexNestedLoopJoinExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
};

#include <cstring>
#include <memory>
#include <utility>

#include "executor_abstract.h"

class IndexJoinDistinctCountExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<IndexNestedLoopJoinExecutor> join_;
    TabCol target_;
    std::vector<ColMeta> cols_;
    RmRecord output_{static_cast<int>(sizeof(int))};
    bool is_end_ = true;

   public:
    IndexJoinDistinctCountExecutor(std::unique_ptr<IndexNestedLoopJoinExecutor> join, TabCol target,
                                   const TabCol &output_col)
        : join_(std::move(join)), target_(std::move(target)) {
        cols_.push_back({"", output_col.col_name, TYPE_INT, static_cast<int>(sizeof(int)), 0, false});
    }

    void beginTuple() override {
        const int count = join_->CountDistinctRightInt(target_);
        rmdb::store_unaligned<int>(output_.data, count);
        is_end_ = false;
    }

    void nextTuple() override { is_end_ = true; }
    bool is_end() const override { return is_end_; }
    size_t tupleLen() const override { return sizeof(int); }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "IndexJoinDistinctCountExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(output_);
    }

    const RmRecord *CurrentTuple() const override { return is_end_ ? nullptr : &output_; }
};

