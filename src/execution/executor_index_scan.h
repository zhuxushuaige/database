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
#include <utility>

#include "index/ix.h"
#include "parser/ast.h"
#include "common/common.h"
#include "execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

// Merged low-level index scan helpers must be complete before executors keep
// them as direct members.
class IndexRangeTraversal {
   public:
    IndexRangeTraversal() = default;
    IndexRangeTraversal(IxIndexHandle *index, const rmdb::IndexRangeSpec *range) { bind(index, range); }

    void bind(IxIndexHandle *index, const rmdb::IndexRangeSpec *range) {
        index_ = index;
        range_ = range;
    }

    std::unique_ptr<IxScan> open_scan() const {
        if (index_ == nullptr || range_ == nullptr) {
            return nullptr;
        }
        return index_->create_scan(lookup_bound(range_->lower_lookup, range_->lower_key),
                                   lookup_bound(range_->upper_lookup, range_->upper_key));
    }

    bool matches_prefix(const char *key) const {
        return range_ != nullptr &&
               (range_->scan_prefix_len <= 0 ||
                std::memcmp(key, range_->lower_key.data(), range_->scan_prefix_len) == 0);
    }

   private:
    Iid lookup_bound(rmdb::IndexBoundLookup lookup, const std::string &key) const {
        switch (lookup) {
            case rmdb::IndexBoundLookup::LeafBegin: return index_->leaf_begin();
            case rmdb::IndexBoundLookup::LowerBound: return index_->lower_bound(key.data());
            case rmdb::IndexBoundLookup::UpperBound: return index_->upper_bound(key.data());
            case rmdb::IndexBoundLookup::LeafEnd: return index_->leaf_end();
        }
        return index_->leaf_end();
    }

    IxIndexHandle *index_{nullptr};
    const rmdb::IndexRangeSpec *range_{nullptr};
};

class VisibleIndexCursor {
   public:
    enum class ReadState { Invisible, IndexOnly, Materialized };

    struct ReadResult {
        ReadState state{ReadState::Invisible};
        bool heap_fetched{false};
        explicit operator bool() const { return state != ReadState::Invisible; }
    };

    VisibleIndexCursor();
    VisibleIndexCursor(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
                       const std::vector<std::string> *index_col_names, const IndexMeta *index_meta,
                       RmFileHandle *file_handle);
    ~VisibleIndexCursor();

    VisibleIndexCursor(VisibleIndexCursor &&) noexcept;
    VisibleIndexCursor &operator=(VisibleIndexCursor &&) noexcept;
    VisibleIndexCursor(const VisibleIndexCursor &) = delete;
    VisibleIndexCursor &operator=(const VisibleIndexCursor &) = delete;

    void bind(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
              const std::vector<std::string> *index_col_names, const IndexMeta *index_meta,
              RmFileHandle *file_handle);
    void reset();
    void finish();
    const std::vector<Rid> &history_rids();
    bool is_historical(const Rid &rid);
    void deduplicate_and_append_history(std::vector<Rid> *candidates);
    ReadResult read_current(const Rid &rid, const char *index_key, RmRecord *out_record,
                            bool materialize_index_only, bool require_matching_key);
    ReadResult read_visible_entry(const Rid &rid, RmRecord *out_record);
    bool current_page_clean_visible() const;
    bool current_page_still_clean_visible(page_id_t page_no);

   private:
    class Impl;
    static std::vector<std::unique_ptr<Impl>> &impl_pool();
    static std::unique_ptr<Impl> acquire_impl();
    static void release_impl(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

class IndexScanExecutor : public AbstractExecutor {
   private:
    static CompOp swap_comparison_op(CompOp op) {
        switch (op) {
            case OP_EQ: return OP_EQ;
            case OP_NE: return OP_NE;
            case OP_LT: return OP_GT;
            case OP_GT: return OP_LT;
            case OP_LE: return OP_GE;
            case OP_GE: return OP_LE;
        }
        return op;
    }

    std::string tab_name_;                      // 表名称
    std::string visible_name_;
    const TabMeta *tab_ = nullptr;              // 表的元数据
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> owned_cols_;           // 投影输出列；完整schema直接借用runtime cache
    std::vector<ColMeta> owned_full_cols_;
    std::vector<ColMeta> owned_output_source_cols_;
    const std::vector<ColMeta> *cols_ = nullptr;
    const std::vector<ColMeta> *full_cols_ = nullptr;
    const std::vector<ColMeta> *output_source_cols_ = nullptr;
    std::vector<ColMeta> index_key_cols_;
    std::vector<const char *> current_cells_;
    std::vector<int> index_output_offsets_;
    size_t len_;                                // 选取出来的一条记录的长度
    size_t full_len_ = 0;
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta owned_index_meta_;
    const IndexMeta *index_meta_ = nullptr;     // 缓存计划的索引元数据可直接借用
    IxIndexHandle *ih_{nullptr};

    Rid rid_;
    const std::vector<Rid> *history_rids_ = nullptr;
    size_t history_cursor_ = 0;
    mutable RmRecord current_record_;
    VisibleTupleRef current_index_key_;
    TupleView current_view_;
    std::unique_ptr<IxScan> index_scan_;
    Rid point_rid_;
    bool point_rid_valid_ = false;
    bool point_consumed_ = false;
    bool at_end_ = true;
    bool current_is_history_ = false;
    bool index_only_ = false;
    bool projected_output_ = false;
    bool compiled_unique_point_ = false;
    std::shared_ptr<const rmdb::CompiledAccessProgram> access_program_;
    std::shared_ptr<const PlanRuntimeCache> runtime_cache_;
    rmdb::u64 access_program_schema_epoch_ = 0;
    bool access_program_matches_ = false;
    rmdb::IndexRangeSpec range_spec_;
    IndexRangeTraversal range_traversal_;

    VisibleIndexCursor visible_index_cursor_;

    SmManager *sm_manager_;
    rmdb::RuntimeScanFeedbackCollector feedback_;

    void reset_data_page() {
        visible_index_cursor_.finish();
    }

    void record_current_read() {
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->RecordSerializableRead(context_->txn_, tab_name_, rid_);
        }
    }

    void compile_index_key_cols() {
        index_key_cols_.clear();
        int key_offset = 0;
        for (const auto &index_col : index_meta_->cols) {
            ColMeta col = index_col;
            col.tab_name = visible_name_;
            col.offset = key_offset;
            index_key_cols_.push_back(col);
            key_offset += index_col.len;
        }
    }

    bool key_conds_match(const char *key) const {
        if (range_spec_.all_conditions_consumed) {
            return true;
        }
        if (access_program_matches_) {
            return access_program_->EvaluateIndexKey(key, fed_conds_);
        }
        RmRecord key_record = RmRecord::borrow(key, index_meta_->logical_col_tot_len());
        return eval_conds(index_key_cols_, &key_record, fed_conds_);
    }

    void set_current_from_heap() {
        if (!projected_output_) {
            current_view_.record = &current_record_;
            current_view_.cells = nullptr;
            return;
        }
        current_cells_.resize(output_source_cols_->size());
        for (size_t i = 0; i < output_source_cols_->size(); ++i) {
            current_cells_[i] = current_record_.data + (*output_source_cols_)[i].offset;
        }
        current_view_.record = nullptr;
        current_view_.cells = &current_cells_;
    }

    void set_current_from_index_key(const char *key) {
        current_index_key_ = VisibleTupleRef::IndexKeyBorrowed(key, index_meta_->logical_col_tot_len());
        if (!current_index_key_) {
            throw RMDBError("unable to lease index leaf for key view");
        }
        key = current_index_key_.data();
        current_cells_.resize(output_source_cols_->size());
        for (size_t i = 0; i < output_source_cols_->size(); ++i) {
            current_cells_[i] = key + index_output_offsets_[i];
        }
        current_view_.record = nullptr;
        current_view_.cells = &current_cells_;
    }

    bool advance_current_index() {
        while (index_scan_ != nullptr && !index_scan_->is_end()) {
            if (context_ != nullptr) {
                context_->check_execution_deadline_periodic();
            }
            current_index_key_ = {};
            const char *key = index_scan_->key();
            if (!range_traversal_.matches_prefix(key)) {
                break;
            }
            feedback_.add_index_entries();
            rid_ = index_scan_->rid();
            if (visible_index_cursor_.is_historical(rid_)) {
                index_scan_->next();
                continue;
            }
            auto visible = visible_index_cursor_.read_current(rid_, key, &current_record_, !index_only_, true);
            feedback_.add_heap_fetches(visible.heap_fetched ? 1 : 0);
            if (!visible) {
                index_scan_->next();
                continue;
            }
            feedback_.add_rows_visible();
            if (visible.state == VisibleIndexCursor::ReadState::IndexOnly) {
                if (!key_conds_match(key)) {
                    index_scan_->next();
                    continue;
                }
                feedback_.add_index_only_rows();
                set_current_from_index_key(key);
            } else {
                if (!eval_conds(*full_cols_, &current_record_, fed_conds_)) {
                    index_scan_->next();
                    continue;
                }
                set_current_from_heap();
            }
            feedback_.add_rows_output();
            current_is_history_ = false;
            record_current_read();
            return true;
        }
        index_scan_.reset();
        return false;
    }

    bool advance_unique_point() {
        if (!point_rid_valid_ || point_consumed_) {
            return false;
        }
        point_consumed_ = true;
        {
            current_index_key_ = {};
            rid_ = point_rid_;
            const char *key = range_spec_.lower_key.data();
            feedback_.add_index_entries();
            if (visible_index_cursor_.is_historical(rid_)) {
                return false;
            }
            auto visible = visible_index_cursor_.read_current(rid_, key, &current_record_, !index_only_, true);
            feedback_.add_heap_fetches(visible.heap_fetched ? 1 : 0);
            if (!visible) {
                return false;
            }
            feedback_.add_rows_visible();
            if (visible.state == VisibleIndexCursor::ReadState::IndexOnly) {
                if (!key_conds_match(key)) {
                    return false;
                }
                feedback_.add_index_only_rows();
                set_current_from_index_key(key);
            } else {
                if (!eval_conds(*full_cols_, &current_record_, fed_conds_)) {
                    return false;
                }
                set_current_from_heap();
            }
            feedback_.add_rows_output();
            current_is_history_ = false;
            record_current_read();
            return true;
        }
    }

    bool advance_history() {
        while (history_rids_ != nullptr && history_cursor_ < history_rids_->size()) {
            rid_ = (*history_rids_)[history_cursor_];
            auto visible = visible_index_cursor_.read_visible_entry(rid_, &current_record_);
            feedback_.add_heap_fetches(visible.heap_fetched ? 1 : 0);
            if (!visible) {
                ++history_cursor_;
                continue;
            }
            feedback_.add_rows_visible();
            if (!eval_conds(*full_cols_, &current_record_, fed_conds_)) {
                ++history_cursor_;
                continue;
            }
            feedback_.add_rows_output();
            set_current_from_heap();
            current_is_history_ = true;
            record_current_read();
            return true;
        }
        return false;
    }

    void advance_index_cursor() {
        at_end_ = false;
        if ((compiled_unique_point_ ? advance_unique_point() : advance_current_index()) || advance_history()) {
            return;
        }
        at_end_ = true;
        reset_data_page();
        feedback_.flush();
    }

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                    std::vector<std::string> index_col_names, Context *context, std::string visible_name = "",
                    const std::vector<TabCol> &required_cols = {},
                    std::shared_ptr<const PlanRuntimeCache> runtime_cache = nullptr,
                    std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        sm_manager_ = sm_manager;
        context_ = context;
        feedback_.bind(std::move(runtime_feedback), rmdb::RuntimeNodeKind::kIndexScan);
        tab_name_ = std::move(tab_name);
        visible_name_ = visible_name.empty() ? tab_name_ : std::move(visible_name);
        runtime_cache_ = std::move(runtime_cache);
        const bool use_cache = runtime_cache_ != nullptr && runtime_cache_->has_table;
        tab_ = &sm_manager_->db_.get_table(tab_name_);
        fed_conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = std::move(index_col_names);
        if (runtime_cache_ != nullptr && runtime_cache_->has_index) {
            index_meta_ = &runtime_cache_->index_meta;
        } else {
            owned_index_meta_ = *(tab_->get_index_meta(index_col_names_, true));
            index_meta_ = &owned_index_meta_;
        }
        access_program_schema_epoch_ = rmdb::sql_template_schema_epoch();
        if (runtime_cache_ != nullptr) {
            access_program_ = runtime_cache_->access_program;
        }
        access_program_matches_ = access_program_ != nullptr &&
                                  access_program_->Matches(*index_meta_, fed_conds_.size(),
                                                           access_program_schema_epoch_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        ih_ = rmdb::resolve_index_handle(sm_manager_, tab_name_, *index_meta_);
        visible_index_cursor_.bind(context_ == nullptr ? nullptr : context_->txn_mgr_,
                                   context_ == nullptr ? nullptr : context_->txn_, tab_name_,
                                   &index_col_names_, index_meta_, fh_);
        if (use_cache) {
            full_cols_ = &runtime_cache_->full_cols;
            full_len_ = runtime_cache_->full_len;
        } else {
            owned_full_cols_ = tab_->cols;
            for (auto &col : owned_full_cols_) {
                col.tab_name = visible_name_;
            }
            full_cols_ = &owned_full_cols_;
            full_len_ = full_cols_->back().offset + full_cols_->back().len;
        }
        for (auto &cond : fed_conds_) {
            if (cond.lhs_col.tab_name != visible_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == visible_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_comparison_op(cond.op);
            }
        }
        if (required_cols.empty()) {
            cols_ = full_cols_;
            output_source_cols_ = full_cols_;
            len_ = full_len_;
        } else if (runtime_cache_ != nullptr && runtime_cache_->has_projection) {
            cols_ = &runtime_cache_->projected_cols;
            output_source_cols_ = &runtime_cache_->projected_source_cols;
            len_ = runtime_cache_->projected_len;
            projected_output_ = true;
        } else {
            build_column_projection(*full_cols_, required_cols, &owned_cols_, &owned_output_source_cols_, &len_);
            if (owned_cols_.empty()) {
                cols_ = full_cols_;
                output_source_cols_ = full_cols_;
                len_ = full_len_;
            } else {
                cols_ = &owned_cols_;
                output_source_cols_ = &owned_output_source_cols_;
                projected_output_ = true;
            }
        }
        compile_index_key_cols();
        index_only_ = rmdb::index_covers_conditions(*index_meta_, fed_conds_, visible_name_);
        for (const auto &col : *output_source_cols_) {
            index_only_ = index_only_ && rmdb::index_covers_col(*index_meta_, col.name);
        }
        if (index_only_) {
            bool bound = access_program_ != nullptr &&
                         access_program_->BindCoveringOutput(*index_meta_, *output_source_cols_,
                                                             &index_output_offsets_);
            if (!bound) {
                for (const auto &col : *output_source_cols_) {
                    auto key_col = rmdb::find_index_logical_col(*index_meta_, col.name);
                    if (!key_col) throw RMDBError("covering index column disappeared");
                    index_output_offsets_.push_back(key_col.offset);
                }
            }
        }
        bool compiled_range = access_program_matches_ &&
                              access_program_->MaterializeRange(*index_meta_, fed_conds_, &range_spec_);
        if (!compiled_range) {
            range_spec_ = rmdb::build_index_range_spec(*index_meta_, fed_conds_, visible_name_);
        }
        compiled_unique_point_ = range_spec_.exact_unique_key && access_program_matches_ &&
                                 access_program_->exact_unique_key();
        range_traversal_.bind(ih_, &range_spec_);
    }

    void beginTuple() override {
        feedback_.begin();
        index_scan_.reset();
        current_view_ = {};
        current_index_key_ = {};
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->RecordSerializablePredicateRead(context_->txn_, tab_name_, *full_cols_, fed_conds_);
        }
        visible_index_cursor_.reset();
        history_rids_ = &visible_index_cursor_.history_rids();
        history_cursor_ = 0;
        point_rid_ = {};
        point_rid_valid_ = false;
        point_consumed_ = false;
        current_is_history_ = false;
        if (compiled_unique_point_) {
            point_rid_valid_ = ih_->get_unique_value(range_spec_.lower_key.data(), &point_rid_,
                                                     context_ == nullptr ? nullptr : context_->txn_);
        } else {
            index_scan_ = range_traversal_.open_scan();
        }
        advance_index_cursor();
    }

    void nextTuple() override {
        if (at_end_) {
            return;
        }
        if (current_is_history_) {
            ++history_cursor_;
        } else if (compiled_unique_point_) {
            point_consumed_ = true;
        } else if (index_scan_ != nullptr && !index_scan_->is_end()) {
            current_index_key_ = {};
            index_scan_->next();
        }
        advance_index_cursor();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (at_end_) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        materialize_tuple_view(*CurrentTupleView(), *cols_, rec.get(), len_);
        return rec;
    }

    const RmRecord *CurrentTuple() const override {
        if (at_end_) {
            return nullptr;
        }
        if (current_view_.record == &current_record_) {
            return &current_record_;
        }
        materialize_tuple_view(*CurrentTupleView(), *cols_, &current_record_, len_);
        return &current_record_;
    }

    const TupleView *CurrentTupleView() const override {
        if (at_end_) {
            return nullptr;
        }
        if (current_view_.cells != nullptr || current_view_.record != nullptr) {
            return &current_view_;
        }
        return nullptr;
    }

    bool is_end() const override { return at_end_; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return *cols_; }
    std::string getType() override { return "IndexScanExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(*cols_, target); }
    Rid &rid() override { return rid_; }
};

#include <cstring>

#include "index/ix.h"
#include "common/common.h"
#include "execution_common.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class CountIndexAggregateExecutor : public AbstractExecutor {
   private:
    struct CompiledKeyCond {
        int lhs_offset = 0;
        int rhs_offset = 0;
        int len = 0;
        CompiledComparator comparator = CompiledComparator::kInvalid;
        const char *rhs_value = nullptr;
        bool rhs_is_value = false;
    };

    SmManager *sm_manager_ = nullptr;
    std::string tab_name_;
    std::string visible_name_;
    const TabMeta *tab_ = nullptr;
    std::vector<Condition> conds_;
    std::vector<std::string> index_col_names_;
    IndexMeta index_meta_;
    RmFileHandle *fh_ = nullptr;
    IxIndexHandle *ih_ = nullptr;
    std::vector<ColMeta> index_key_cols_;
    std::vector<CompiledKeyCond> compiled_key_conds_;
    std::vector<ColMeta> full_cols_;
    std::vector<ColMeta> cols_;
    size_t len_ = sizeof(int);
    rmdb::IndexRangeSpec range_spec_;
    IndexRangeTraversal range_traversal_;
    VisibleIndexCursor visible_index_cursor_;
    RmRecord current_record_;
    RmRecord output_tuple_;
    size_t cursor_ = 0;
    bool has_tuple_ = false;
    rmdb::RuntimeScanFeedbackCollector feedback_;
    bool compiled_key_conds_valid_ = false;
    std::shared_ptr<const rmdb::CompiledAccessProgram> access_program_;

    void compile_index_key_cols() {
        index_key_cols_.clear();
        int key_offset = 0;
        for (const auto &index_col : index_meta_.cols) {
            ColMeta col = index_col;
            col.tab_name = visible_name_;
            col.offset = key_offset;
            index_key_cols_.push_back(col);
            key_offset += index_col.len;
        }
    }

    void compile_key_conds() {
        compiled_key_conds_.clear();
        compiled_key_conds_.reserve(conds_.size());
        compiled_key_conds_valid_ = true;
        for (const auto &cond : conds_) {
            auto lhs_col = get_col(index_key_cols_, cond.lhs_col);
            CompiledKeyCond compiled;
            compiled.lhs_offset = lhs_col->offset;
            compiled.len = lhs_col->len;
            compiled.comparator = compile_comparator(lhs_col->type, cond.op);
            if (compiled.comparator == CompiledComparator::kInvalid) {
                compiled_key_conds_.clear();
                compiled_key_conds_valid_ = false;
                return;
            }
            compiled.rhs_is_value = cond.is_rhs_val;
            if (cond.is_rhs_val) {
                if (cond.rhs_val.raw == nullptr) {
                    compiled_key_conds_.clear();
                    compiled_key_conds_valid_ = false;
                    return;
                }
                compiled.rhs_value = cond.rhs_val.raw->data;
            } else {
                auto rhs_col = get_col(index_key_cols_, cond.rhs_col);
                if (rhs_col->type != lhs_col->type || rhs_col->len != lhs_col->len) {
                    compiled_key_conds_.clear();
                    compiled_key_conds_valid_ = false;
                    return;
                }
                compiled.rhs_offset = rhs_col->offset;
            }
            compiled_key_conds_.push_back(compiled);
        }
    }

    bool eval_key_conds(const char *key) const {
        if (compiled_key_conds_valid_) {
            for (const auto &cond : compiled_key_conds_) {
                const char *lhs = key + cond.lhs_offset;
                const char *rhs = cond.rhs_is_value ? cond.rhs_value : key + cond.rhs_offset;
                if (!eval_compiled_comparator(cond.comparator, lhs, rhs, cond.len)) {
                    return false;
                }
            }
            return true;
        }
        RmRecord key_record = RmRecord::borrow(key, index_meta_.logical_col_tot_len());
        return eval_conds(index_key_cols_, &key_record, conds_);
    }

    bool key_conds_match(const char *key) const {
        return range_spec_.all_conditions_consumed || eval_key_conds(key);
    }

    bool entry_counts(const Rid &rid, const char *key) {
        auto visible = visible_index_cursor_.read_current(rid, key, &current_record_, false, true);
        feedback_.add_heap_fetches(visible.heap_fetched ? 1 : 0);
        if (!visible) {
            return false;
        }
        return key_conds_match(key);
    }

    int count_visible_entries() {
        int count = 0;
        const auto &historical = visible_index_cursor_.history_rids();
        IxReadGuard read_guard = ih_->make_read_guard();
        auto scan = range_traversal_.open_scan();
        page_id_t clean_page_no = RM_NO_PAGE;
        bool stop_scan = false;
        while (!stop_scan && scan != nullptr && !scan->is_end()) {
            if (context_ != nullptr) {
                context_->check_execution_deadline();
            }
            int slot = scan->current_leaf_slot();
            const int leaf_end = scan->current_leaf_scan_end_slot();
            int processed = 0;
            while (slot < leaf_end) {
                if (context_ != nullptr) {
                    context_->check_execution_deadline_periodic();
                }
                const char *key = scan->current_leaf_key_at(slot);
                if (!range_traversal_.matches_prefix(key)) {
                    stop_scan = true;
                    break;
                }
                feedback_.add_index_entries();
                const Rid rid = scan->current_leaf_rid_at(slot);
                processed++;

                if (visible_index_cursor_.is_historical(rid)) {
                    clean_page_no = RM_NO_PAGE;
                    slot++;
                    continue;
                }

                if (rid.page_no == clean_page_no) {
                    bool clean_visible = visible_index_cursor_.current_page_still_clean_visible(rid.page_no);
                    if (clean_visible && range_spec_.all_conditions_consumed) {
                        count++;
                        slot++;
                        while (slot < leaf_end) {
                            const char *next_key = scan->current_leaf_key_at(slot);
                            if (!range_traversal_.matches_prefix(next_key)) {
                                stop_scan = true;
                                break;
                            }
                            const Rid next_rid = scan->current_leaf_rid_at(slot);
                            if (next_rid.page_no != clean_page_no) {
                                break;
                            }
                            feedback_.add_index_entries();
                            count++;
                            processed++;
                            slot++;
                        }
                        if (stop_scan) {
                            break;
                        }
                        continue;
                    }
                    if (clean_visible && key_conds_match(key)) {
                        count++;
                    }
                    slot++;
                    continue;
                }

                if (entry_counts(rid, key)) {
                    count++;
                }
                clean_page_no = visible_index_cursor_.current_page_clean_visible() ? rid.page_no : RM_NO_PAGE;
                slot++;
            }
            if (!stop_scan) {
                scan->advance_current_leaf(processed);
            }
        }
        read_guard.reset();
        for (const auto &rid : historical) {
            if (context_ != nullptr) {
                context_->check_execution_deadline_periodic();
            }
            auto visible = visible_index_cursor_.read_visible_entry(rid, &current_record_);
            feedback_.add_heap_fetches(visible.heap_fetched ? 1 : 0);
            if (visible && eval_conds(full_cols_, &current_record_, conds_)) {
                count++;
            }
        }
        return count;
    }

    void write_count(int value) {
        output_tuple_ = RmRecord(static_cast<int>(len_));
        std::memcpy(output_tuple_.data, &value, sizeof(int));
    }

   public:
    CountIndexAggregateExecutor(SmManager *sm_manager,
                                std::string tab_name,
                                std::string visible_name,
                                std::vector<Condition> conds,
                                std::vector<std::string> index_col_names,
                                std::vector<TabCol> output_cols,
                                Context *context,
                                std::shared_ptr<const PlanRuntimeCache> runtime_cache = nullptr,
                                std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        sm_manager_ = sm_manager;
        context_ = context;
        feedback_.bind(std::move(runtime_feedback), rmdb::RuntimeNodeKind::kCountIndex);
        tab_name_ = std::move(tab_name);
        visible_name_ = visible_name.empty() ? tab_name_ : std::move(visible_name);
        conds_ = std::move(conds);
        index_col_names_ = std::move(index_col_names);

        const bool use_cache = runtime_cache != nullptr && runtime_cache->has_table;
        tab_ = &sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        if (use_cache) {
            full_cols_ = runtime_cache->full_cols;
        } else {
            full_cols_ = tab_->cols;
            for (auto &col : full_cols_) {
                col.tab_name = visible_name_;
            }
        }
        index_meta_ = runtime_cache != nullptr && runtime_cache->has_index
                          ? runtime_cache->index_meta
                          : *(tab_->get_index_meta(index_col_names_, true));
        ih_ = rmdb::resolve_index_handle(sm_manager_, tab_name_, index_meta_);
        compile_index_key_cols();
        compile_key_conds();
        if (runtime_cache != nullptr) access_program_ = runtime_cache->access_program;
        bool compiled_range = access_program_ != nullptr &&
                              access_program_->Matches(index_meta_, conds_.size(), rmdb::sql_template_schema_epoch()) &&
                              access_program_->MaterializeRange(index_meta_, conds_, &range_spec_);
        if (!compiled_range) {
            range_spec_ = rmdb::build_index_range_spec(index_meta_, conds_, visible_name_);
        }
        range_traversal_.bind(ih_, &range_spec_);
        visible_index_cursor_.bind(context_ == nullptr ? nullptr : context_->txn_mgr_,
                                   context_ == nullptr ? nullptr : context_->txn_, tab_name_,
                                   &index_col_names_, &index_meta_, fh_);

        ColMeta output_col;
        output_col.tab_name = "";
        output_col.name = output_cols.empty() ? "count" : output_cols[0].col_name;
        output_col.offset = 0;
        output_col.type = TYPE_INT;
        output_col.len = sizeof(int);
        cols_.push_back(output_col);
    }

    void beginTuple() override {
        feedback_.begin();
        cursor_ = 0;
        has_tuple_ = true;
        visible_index_cursor_.reset();
        int count = count_visible_entries();
        visible_index_cursor_.finish();
        feedback_.add_rows_visible(static_cast<rmdb::u64>(count));
        feedback_.add_rows_output(1);
        write_count(count);
        feedback_.flush();
    }

    void nextTuple() override {
        if (cursor_ == 0) {
            cursor_ = 1;
        }
    }

    bool is_end() const override { return !has_tuple_ || cursor_ > 0; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "CountIndexAggregateExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        std::memcpy(rec->data, output_tuple_.data, len_);
        return rec;
    }

    const RmRecord *CurrentTuple() const override {
        return is_end() ? nullptr : &output_tuple_;
    }
};

#include <algorithm>
#include <cstring>
#include <limits>

#include "index/ix.h"
#include "execution_common.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class MinMaxIndexAggregateExecutor : public AbstractExecutor {
   private:
    struct PrefixCond {
        int key_offset = 0;
        int len = 0;
        const char *rhs_value = nullptr;
    };

    std::string tab_name_;
    std::string visible_name_;
    TabMeta tab_;
    std::vector<Condition> conds_;
    std::vector<std::string> index_col_names_;
    IndexMeta index_meta_;
    IxIndexHandle *ih_ = nullptr;
    RmFileHandle *fh_ = nullptr;
    std::vector<ColMeta> full_cols_;
    ColMeta agg_input_col_;
    ColMeta output_col_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    ast::AggType agg_type_ = ast::AGG_MIN;

    std::vector<PrefixCond> compiled_prefix_conds_;
    int equality_prefix_len_ = 0;
    std::string local_index_key_scratch_;
    std::string *index_key_scratch_ = &local_index_key_scratch_;
    rmdb::IndexRangeSpec range_spec_;
    IndexRangeTraversal range_traversal_;
    VisibleIndexCursor visible_index_cursor_;
    RmRecord current_record_;
    RmRecord output_tuple_;
    size_t cursor_ = 0;
    bool has_tuple_ = false;
    rmdb::RuntimeScanFeedbackCollector feedback_;
    std::shared_ptr<const rmdb::CompiledAccessProgram> access_program_;

    bool record_visible_result(const VisibleIndexCursor::ReadResult &visible) {
        feedback_.add_heap_fetches(visible.heap_fetched ? 1 : 0);
        if (visible) {
            feedback_.add_rows_visible();
            return true;
        }
        return false;
    }

    void compile_index_prefix() {
        compiled_prefix_conds_.clear();
        equality_prefix_len_ = 0;
        int key_offset = 0;
        for (const auto &index_col : index_meta_.cols) {
            auto cond_it = std::find_if(conds_.begin(), conds_.end(), [&](const Condition &cond) {
                return cond.is_rhs_val && cond.op == OP_EQ && cond.rhs_val.raw != nullptr &&
                       cond.lhs_col.tab_name == visible_name_ && cond.lhs_col.col_name == index_col.name;
            });
            if (cond_it == conds_.end()) {
                break;
            }
            compiled_prefix_conds_.push_back({key_offset, index_col.len, cond_it->rhs_val.raw->data});
            key_offset += index_col.len;
            equality_prefix_len_ = key_offset;
        }
    }

    void write_empty_value() {
        output_tuple_ = RmRecord(static_cast<int>(len_));
        memset(output_tuple_.data, 0, len_);
    }

    void write_value(const char *slot) {
        output_tuple_ = RmRecord(static_cast<int>(len_));
        memcpy(output_tuple_.data + output_col_.offset, slot + agg_input_col_.offset, agg_input_col_.len);
    }

    bool value_greater_than_output(const char *slot) const {
        const char *lhs = slot + agg_input_col_.offset;
        const char *rhs = output_tuple_.data + output_col_.offset;
        if (agg_input_col_.type == TYPE_INT) {
            return rmdb::load_unaligned<int>(lhs) > rmdb::load_unaligned<int>(rhs);
        }
        if (agg_input_col_.type == TYPE_FLOAT) {
            return rmdb::load_unaligned<float>(lhs) > rmdb::load_unaligned<float>(rhs);
        }
        return std::memcmp(lhs, rhs, static_cast<size_t>(agg_input_col_.len)) > 0;
    }

    bool value_less_than_output(const char *slot) const {
        const char *lhs = slot + agg_input_col_.offset;
        const char *rhs = output_tuple_.data + output_col_.offset;
        if (agg_input_col_.type == TYPE_INT) {
            return rmdb::load_unaligned<int>(lhs) < rmdb::load_unaligned<int>(rhs);
        }
        if (agg_input_col_.type == TYPE_FLOAT) {
            return rmdb::load_unaligned<float>(lhs) < rmdb::load_unaligned<float>(rhs);
        }
        return std::memcmp(lhs, rhs, static_cast<size_t>(agg_input_col_.len)) < 0;
    }

    bool find_min_value() {
        std::fill(index_key_scratch_->begin(), index_key_scratch_->end(), '\0');
        for (const auto &prefix : compiled_prefix_conds_) {
            memcpy(index_key_scratch_->data() + prefix.key_offset, prefix.rhs_value, prefix.len);
        }
        int key_offset = equality_prefix_len_;
        for (int i = static_cast<int>(compiled_prefix_conds_.size()); i < index_meta_.col_num; ++i) {
            const auto &col = index_meta_.cols[i];
            if (col.type == TYPE_INT) {
                int min_value = std::numeric_limits<int>::min();
                memcpy(index_key_scratch_->data() + key_offset, &min_value, sizeof(int));
            } else if (col.type == TYPE_FLOAT) {
                float min_value = -std::numeric_limits<float>::max();
                memcpy(index_key_scratch_->data() + key_offset, &min_value, sizeof(float));
            }
            key_offset += col.len;
        }

        bool found = false;
        bool inclusive = true;
        std::string candidate_key;
        Rid rid;
        while (ih_->successor(index_key_scratch_->data(), &candidate_key, &rid, inclusive)) {
            if (context_ != nullptr) {
                context_->check_execution_deadline_periodic();
            }
            inclusive = false;
            if (!range_traversal_.matches_prefix(candidate_key.data())) {
                break;
            }
            feedback_.add_index_entries();
            *index_key_scratch_ = candidate_key;
            auto visible = visible_index_cursor_.read_current(
                rid, candidate_key.data(), &current_record_, true, true);
            if (record_visible_result(visible) &&
                eval_conds(full_cols_, &current_record_, conds_)) {
                write_value(current_record_.data);
                found = true;
                break;
            }
        }

        for (const auto &historical_rid : visible_index_cursor_.history_rids()) {
            auto visible = visible_index_cursor_.read_visible_entry(historical_rid, &current_record_);
            if (!record_visible_result(visible) ||
                !eval_conds(full_cols_, &current_record_, conds_)) {
                continue;
            }
            if (!found || value_less_than_output(current_record_.data)) {
                write_value(current_record_.data);
                found = true;
            }
        }
        visible_index_cursor_.finish();
        return found;
    }

    bool find_max_value() {
        std::fill(index_key_scratch_->begin(), index_key_scratch_->end(), '\0');
        for (const auto &prefix : compiled_prefix_conds_) {
            memcpy(index_key_scratch_->data() + prefix.key_offset, prefix.rhs_value, prefix.len);
        }
        rmdb::fill_index_key_max_suffix(index_meta_, equality_prefix_len_, index_key_scratch_->data());

        bool found = false;
        bool inclusive = true;
        std::string candidate_key;
        Rid rid;
        while (ih_->predecessor(index_key_scratch_->data(), &candidate_key, &rid, inclusive)) {
            feedback_.add_index_entries();
            inclusive = false;
            if (!range_traversal_.matches_prefix(candidate_key.data())) {
                break;
            }
            *index_key_scratch_ = candidate_key;
            auto visible = visible_index_cursor_.read_current(
                rid, candidate_key.data(), &current_record_, true, true);
            if (record_visible_result(visible) &&
                eval_conds(full_cols_, &current_record_, conds_)) {
                write_value(current_record_.data);
                found = true;
                break;
            }
        }

        for (const auto &historical_rid : visible_index_cursor_.history_rids()) {
            auto visible = visible_index_cursor_.read_visible_entry(historical_rid, &current_record_);
            if (!record_visible_result(visible) ||
                !eval_conds(full_cols_, &current_record_, conds_)) {
                continue;
            }
            if (!found || value_greater_than_output(current_record_.data)) {
                write_value(current_record_.data);
                found = true;
            }
        }
        visible_index_cursor_.finish();
        return found;
    }

   public:
    MinMaxIndexAggregateExecutor(SmManager *sm_manager,
                                 std::string tab_name,
                                 std::string visible_name,
                                 std::vector<Condition> conds,
                                 std::vector<std::string> index_col_names,
                                 TabCol agg_col,
                                 ast::AggType agg_type,
                                 std::vector<TabCol> output_cols,
                                 Context *context,
                                 std::shared_ptr<const PlanRuntimeCache> runtime_cache = nullptr,
                                 std::shared_ptr<rmdb::RuntimeNodeFeedback> runtime_feedback = nullptr) {
        context_ = context;
        feedback_.bind(std::move(runtime_feedback), rmdb::RuntimeNodeKind::kMinMaxIndex);
        tab_name_ = std::move(tab_name);
        visible_name_ = visible_name.empty() ? tab_name_ : std::move(visible_name);
        conds_ = std::move(conds);
        index_col_names_ = std::move(index_col_names);
        agg_type_ = agg_type;

        const bool use_cache = runtime_cache != nullptr && runtime_cache->has_table;
        tab_ = sm_manager->db_.get_table(tab_name_);
        index_meta_ = runtime_cache != nullptr && runtime_cache->has_index
                          ? runtime_cache->index_meta
                          : *(tab_.get_index_meta(index_col_names_, true));
        ih_ = rmdb::resolve_index_handle(sm_manager, tab_name_, index_meta_);
        fh_ = sm_manager->fhs_.at(tab_name_).get();
        if (use_cache) {
            full_cols_ = runtime_cache->full_cols;
        } else {
            full_cols_ = tab_.cols;
            for (auto &col : full_cols_) {
                col.tab_name = visible_name_;
            }
        }
        agg_input_col_ = *get_col(full_cols_, agg_col);

        output_col_.tab_name = "";
        output_col_.name = output_cols.empty() ? agg_col.col_name : output_cols[0].col_name;
        output_col_.offset = 0;
        output_col_.type = agg_input_col_.type;
        output_col_.len = agg_input_col_.len;
        cols_.push_back(output_col_);
        len_ = static_cast<size_t>(output_col_.len);

        if (context_ != nullptr) {
            if (auto *scratch = context_->acquire_statement_string(index_meta_.col_tot_len); scratch != nullptr) {
                index_key_scratch_ = scratch;
            }
        }
        index_key_scratch_->resize(index_meta_.col_tot_len);
        visible_index_cursor_.bind(context_ == nullptr ? nullptr : context_->txn_mgr_,
                                   context_ == nullptr ? nullptr : context_->txn_, tab_name_,
                                   &index_col_names_, &index_meta_, fh_);
        compile_index_prefix();
        if (runtime_cache != nullptr) access_program_ = runtime_cache->access_program;
        bool compiled_range = access_program_ != nullptr &&
                              access_program_->Matches(index_meta_, conds_.size(), rmdb::sql_template_schema_epoch()) &&
                              access_program_->MaterializeRange(index_meta_, conds_, &range_spec_);
        if (!compiled_range) {
            range_spec_ = rmdb::build_index_range_spec(index_meta_, conds_, visible_name_);
        }
        range_traversal_.bind(ih_, &range_spec_);
    }

    void beginTuple() override {
        feedback_.begin();
        cursor_ = 0;
        has_tuple_ = true;
        visible_index_cursor_.reset();
        if (agg_type_ != ast::AGG_MIN && agg_type_ != ast::AGG_MAX) {
            has_tuple_ = false;
            feedback_.flush();
            return;
        }
        if (!(agg_type_ == ast::AGG_MIN ? find_min_value() : find_max_value())) {
            write_empty_value();
        }
        feedback_.add_rows_output();
        feedback_.flush();
    }

    void nextTuple() override {
        if (cursor_ == 0) {
            cursor_ = 1;
        }
    }

    bool is_end() const override { return !has_tuple_ || cursor_ > 0; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "MinMaxIndexAggregateExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        memcpy(rec->data, output_tuple_.data, len_);
        return rec;
    }

    const RmRecord *CurrentTuple() const override {
        return is_end() ? nullptr : &output_tuple_;
    }
};

#if 0  // Definitions moved above the executors after source-file consolidation.
#include <cstring>
#include <memory>

#include "index/ix.h"
#include "index/ix.h"

class IndexRangeTraversal {
   public:
    IndexRangeTraversal() = default;
    IndexRangeTraversal(IxIndexHandle *index, const rmdb::IndexRangeSpec *range) {
        bind(index, range);
    }

    void bind(IxIndexHandle *index, const rmdb::IndexRangeSpec *range) {
        index_ = index;
        range_ = range;
    }

    std::unique_ptr<IxScan> open_scan() const {
        if (index_ == nullptr || range_ == nullptr) {
            return nullptr;
        }
        return index_->create_scan(lookup_bound(range_->lower_lookup, range_->lower_key),
                                   lookup_bound(range_->upper_lookup, range_->upper_key));
    }

    bool matches_prefix(const char *key) const {
        return range_ != nullptr &&
               (range_->scan_prefix_len <= 0 ||
                std::memcmp(key, range_->lower_key.data(), range_->scan_prefix_len) == 0);
    }

   private:
    Iid lookup_bound(rmdb::IndexBoundLookup lookup, const std::string &key) const {
        switch (lookup) {
            case rmdb::IndexBoundLookup::LeafBegin: return index_->leaf_begin();
            case rmdb::IndexBoundLookup::LowerBound: return index_->lower_bound(key.data());
            case rmdb::IndexBoundLookup::UpperBound: return index_->upper_bound(key.data());
            case rmdb::IndexBoundLookup::LeafEnd: return index_->leaf_end();
        }
        return index_->leaf_end();
    }

    IxIndexHandle *index_{nullptr};
    const rmdb::IndexRangeSpec *range_{nullptr};
};

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "defs.h"

class RmFileHandle;
struct RmRecord;
class Transaction;
class TransactionManager;
struct IndexMeta;

class VisibleIndexCursor {
   public:
    enum class ReadState { Invisible, IndexOnly, Materialized };

    struct ReadResult {
        ReadState state{ReadState::Invisible};
        bool heap_fetched{false};

        explicit operator bool() const { return state != ReadState::Invisible; }
    };

    VisibleIndexCursor();
    VisibleIndexCursor(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
                       const std::vector<std::string> *index_col_names, const IndexMeta *index_meta,
                       RmFileHandle *file_handle);
    ~VisibleIndexCursor();

    VisibleIndexCursor(VisibleIndexCursor &&) noexcept;
    VisibleIndexCursor &operator=(VisibleIndexCursor &&) noexcept;
    VisibleIndexCursor(const VisibleIndexCursor &) = delete;
    VisibleIndexCursor &operator=(const VisibleIndexCursor &) = delete;

    void bind(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
              const std::vector<std::string> *index_col_names, const IndexMeta *index_meta,
              RmFileHandle *file_handle);
    void reset();
    void finish();

    const std::vector<Rid> &history_rids();
    bool is_historical(const Rid &rid);
    void deduplicate_and_append_history(std::vector<Rid> *candidates);
    ReadResult read_current(const Rid &rid, const char *index_key, RmRecord *out_record,
                            bool materialize_index_only, bool require_matching_key);
    ReadResult read_visible_entry(const Rid &rid, RmRecord *out_record);
    bool current_page_clean_visible() const;
    bool current_page_still_clean_visible(page_id_t page_no);

   private:
    class Impl;
    static std::vector<std::unique_ptr<Impl>> &impl_pool();
    static std::unique_ptr<Impl> acquire_impl();
    static void release_impl(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};
#endif
