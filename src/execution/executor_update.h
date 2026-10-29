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
#include <cmath>
#include <fstream>
#include "index/ix.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "execution_common.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "recovery/log_manager.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    struct CompiledSetClause {
        int lhs_offset = 0;
        int rhs_offset = 0;
        int len = 0;
        ColType lhs_type = TYPE_INT;
        ColType rhs_type = TYPE_INT;
        SetOp op = SetOp::ASSIGN;
        bool reversed_operands = false;
        const char *literal = nullptr;
        std::vector<SetOp> extra_ops;
        std::vector<const char *> extra_literals;
        void (*apply)(char *, const char *, const CompiledSetClause &) = nullptr;
    };

    static void apply_literal_assign(char *dst, const char *, const CompiledSetClause &clause) {
        memcpy(dst + clause.lhs_offset, clause.literal, clause.len);
    }

    static void apply_column_assign(char *dst, const char *src, const CompiledSetClause &clause) {
        memcpy(dst + clause.lhs_offset, src + clause.rhs_offset, clause.len);
    }

    static void apply_column_assign_int_to_float(char *dst, const char *src, const CompiledSetClause &clause) {
        float value = static_cast<float>(rmdb::load_unaligned<int>(src + clause.rhs_offset));
        memcpy(dst + clause.lhs_offset, &value, clause.len);
    }

    static void apply_int_arithmetic(char *dst, const char *src, const CompiledSetClause &clause) {
        int lhs_val = rmdb::load_unaligned<int>(src + clause.rhs_offset);
        int rhs_val = rmdb::load_unaligned<int>(clause.literal);
        int base = clause.reversed_operands ? rhs_val : lhs_val;
        int delta = clause.reversed_operands ? lhs_val : rhs_val;
        auto apply_op = [](int value, int operand, SetOp op) {
            if (op == SetOp::ADD) return value + operand;
            if (op == SetOp::SUB) return value - operand;
            if (op == SetOp::MUL) return value * operand;
            if (op == SetOp::DIV) {
                if (operand == 0) throw NonRetryableSqlExecutionError("Division by zero");
                return value / operand;
            }
            throw InternalError("Invalid integer update arithmetic operation");
        };
        int result = apply_op(base, delta, clause.op);
        for (size_t i = 0; i < clause.extra_ops.size(); ++i) {
            result = apply_op(result, rmdb::load_unaligned<int>(clause.extra_literals[i]), clause.extra_ops[i]);
        }
        memcpy(dst + clause.lhs_offset, &result, clause.len);
    }

    static void apply_float_arithmetic(char *dst, const char *src, const CompiledSetClause &clause) {
        float lhs_val = clause.rhs_type == TYPE_FLOAT
                         ? rmdb::load_unaligned<float>(src + clause.rhs_offset)
                         : static_cast<float>(rmdb::load_unaligned<int>(src + clause.rhs_offset));
        float rhs_val = rmdb::load_unaligned<float>(clause.literal);
        float base = clause.reversed_operands ? rhs_val : lhs_val;
        float delta = clause.reversed_operands ? lhs_val : rhs_val;
        if (!std::isfinite(base) || !std::isfinite(delta)) {
            throw NonRetryableSqlExecutionError("FLOAT value must be finite");
        }
        auto apply_op = [](float value, float operand, SetOp op) {
            if (op == SetOp::ADD) return value + operand;
            if (op == SetOp::SUB) return value - operand;
            if (op == SetOp::MUL) return value * operand;
            if (op == SetOp::DIV) {
                if (operand == 0.0f) throw NonRetryableSqlExecutionError("Division by zero");
                return value / operand;
            }
            throw InternalError("Invalid float update arithmetic operation");
        };
        float result = apply_op(base, delta, clause.op);
        for (size_t i = 0; i < clause.extra_ops.size(); ++i) {
            result = apply_op(result, rmdb::load_unaligned<float>(clause.extra_literals[i]), clause.extra_ops[i]);
        }
        if (!std::isfinite(result)) {
            throw NonRetryableSqlExecutionError("FLOAT arithmetic result must be finite");
        }
        memcpy(dst + clause.lhs_offset, &result, clause.len);
    }

    static void apply_unsupported_arithmetic(char *, const char *, const CompiledSetClause &clause) {
        throw IncompatibleTypeError("numeric column", coltype2str(clause.lhs_type));
    }

    const TabMeta *tab_ = nullptr;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::shared_ptr<std::vector<Rid>> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    std::vector<rmdb::IndexBinding> index_bindings_;
    std::vector<std::string> owned_index_key_scratch_;
    std::vector<std::string> owned_index_key_scratch_alt_;
    std::vector<std::string *> index_key_scratch_;
    std::vector<std::string *> index_key_scratch_alt_;
    std::vector<std::string> changed_cols_;
    std::vector<bool> index_key_touched_;
    std::vector<CompiledSetClause> compiled_set_clauses_;
    bool update_touches_index_columns_{false};
    bool key_conflict_check_required_{false};
    bool refresh_committed_hot_update_{false};
    std::shared_ptr<const rmdb::CompiledMutationProgram> mutation_program_;
    struct InsertedIndexKey {
        IxIndexHandle *ih;
        std::string key;
    };

    void compile_set_clauses() {
        compiled_set_clauses_.clear();
        compiled_set_clauses_.reserve(set_clauses_.size());
        refresh_committed_hot_update_ = !set_clauses_.empty() && !update_touches_index_columns_;
        const auto *cached_slots = mutation_program_ == nullptr ? nullptr : &mutation_program_->slots();
        for (size_t clause_index = 0; clause_index < set_clauses_.size(); ++clause_index) {
            auto &set_clause = set_clauses_[clause_index];
            CompiledSetClause compiled;
            if (cached_slots != nullptr) {
                const auto &slot = (*cached_slots)[clause_index];
                compiled.lhs_offset = slot.lhs_offset;
                compiled.len = slot.len;
                compiled.lhs_type = slot.lhs_type;
                compiled.op = slot.op;
                compiled.reversed_operands = slot.reversed;
            } else {
                auto lhs_col = tab_->get_col(set_clause.lhs.col_name);
                compiled.lhs_offset = lhs_col->offset;
                compiled.len = lhs_col->len;
                compiled.lhs_type = lhs_col->type;
                compiled.op = set_clause.op;
                compiled.reversed_operands = set_clause.reversed_operands;
            }

            if (!set_clause.rhs_is_col) {
                if (set_clause.rhs.raw == nullptr) {
                    set_clause.rhs.init_raw(compiled.len);
                }
                compiled.literal = set_clause.rhs.raw->data;
                if (compiled.lhs_type == TYPE_FLOAT &&
                    !std::isfinite(rmdb::load_unaligned<float>(compiled.literal))) {
                    throw NonRetryableSqlExecutionError("FLOAT value must be finite");
                }
                compiled.apply = apply_literal_assign;
                refresh_committed_hot_update_ = false;
                compiled_set_clauses_.push_back(compiled);
                continue;
            }

            if (cached_slots != nullptr) {
                const auto &slot = (*cached_slots)[clause_index];
                compiled.rhs_offset = slot.rhs_offset;
                compiled.rhs_type = slot.rhs_type;
            } else {
                auto rhs_col = tab_->get_col(set_clause.rhs_col.col_name);
                compiled.rhs_offset = rhs_col->offset;
                compiled.rhs_type = rhs_col->type;
            }

            if (set_clause.op == SetOp::ASSIGN) {
                compiled.apply = compiled.lhs_type == TYPE_FLOAT && compiled.rhs_type == TYPE_INT
                                     ? apply_column_assign_int_to_float
                                     : apply_column_assign;
                refresh_committed_hot_update_ = false;
                compiled_set_clauses_.push_back(compiled);
                continue;
            }

                if (set_clause.rhs.raw == nullptr) {
                set_clause.rhs.init_raw(compiled.len);
            }
            compiled.literal = set_clause.rhs.raw->data;
            if (compiled.lhs_type == TYPE_FLOAT &&
                !std::isfinite(rmdb::load_unaligned<float>(compiled.literal))) {
                throw NonRetryableSqlExecutionError("FLOAT value must be finite");
            }
            compiled.extra_ops.reserve(set_clause.extra_steps.size());
            compiled.extra_literals.reserve(set_clause.extra_steps.size());
            for (auto &step : set_clause.extra_steps) {
                if (step.rhs.raw == nullptr) {
                    step.rhs.init_raw(compiled.len);
                }
                const char *step_literal = step.rhs.raw->data;
                if (compiled.lhs_type == TYPE_FLOAT &&
                    !std::isfinite(rmdb::load_unaligned<float>(step_literal))) {
                    throw NonRetryableSqlExecutionError("FLOAT value must be finite");
                }
                compiled.extra_ops.push_back(step.op);
                compiled.extra_literals.push_back(step_literal);
            }
            if (compiled.lhs_type == TYPE_INT) {
                compiled.apply = apply_int_arithmetic;
            } else if (compiled.lhs_type == TYPE_FLOAT) {
                compiled.apply = apply_float_arithmetic;
            } else {
                compiled.apply = apply_unsupported_arithmetic;
            }
            if (set_clause.lhs.col_name != set_clause.rhs_col.col_name) {
                refresh_committed_hot_update_ = false;
            }
            compiled_set_clauses_.push_back(compiled);
        }
    }

    void apply_prepared_update(Rid &rid, std::unique_ptr<RmRecord> old_record, RmRecord *new_rec,
                               const TupleMeta &old_meta,
                               const std::optional<VersionUndoLink> &old_version) {
        RmRecord *old_rec = old_record.get();
        std::vector<InsertedIndexKey> inserted_index_keys;
        if (update_touches_index_columns_) {
            inserted_index_keys.reserve(index_bindings_.size());
        }
        auto rollback_inserted_index_keys = [&]() {
            Transaction *txn = context_ == nullptr ? nullptr : context_->txn_;
            for (auto iter = inserted_index_keys.rbegin(); iter != inserted_index_keys.rend(); ++iter) {
                iter->ih->delete_entry(iter->key.data(), txn);
            }
            inserted_index_keys.clear();
        };
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->RecordSerializableWrite(context_->txn_, tab_name_, rid, old_rec, new_rec,
                                                        &tab_->cols);
        }
        try {
            if (context_ != nullptr && context_->txn_ != nullptr) {
                auto undo_image = std::shared_ptr<RmRecord>(std::move(old_record));
                old_rec = undo_image.get();
                if (context_->txn_mgr_ != nullptr) {
                    UndoLog undo_log{old_meta.is_deleted_, {}, {}, undo_image, old_meta.ts_,
                                     old_version.has_value() ? old_version->prev_ : UndoLink{}};
                    UndoLink undo_link = context_->txn_mgr_->AppendUndoLog(context_->txn_, std::move(undo_log));
                    context_->txn_mgr_->InstallTupleVersion(
                        tab_name_, rid, VersionUndoLink{undo_link, true},
                        TupleMeta{TXN_START_ID + context_->txn_->get_transaction_id(), false});
                }
                context_->txn_->emplace_write_record(WType::UPDATE_TUPLE, tab_name_, rid, undo_image,
                                                     update_touches_index_columns_);
            }
            if (update_touches_index_columns_) {
                for (size_t i = 0; i < index_bindings_.size(); ++i) {
                    if (!index_key_touched_[i]) {
                        continue;
                    }
                    const auto &binding = index_bindings_[i];
                    const auto &index = *binding.meta;
                    auto *ih = binding.ih;
                    char *old_key = rmdb::build_index_key_into(index, old_rec->data, rid, index_key_scratch_[i]);
                    char *new_key = rmdb::build_index_key_into(index, new_rec->data, rid, index_key_scratch_alt_[i]);
                    if (memcmp(old_key, new_key, index.col_tot_len) == 0) {
                        continue;
                    }
                    auto outcome = ih->insert_entry(new_key, rid, context_->txn_);
                    if (outcome.result == IxInsertResult::kInserted) {
                        inserted_index_keys.push_back(InsertedIndexKey{ih, std::string(new_key, index.col_tot_len)});
                    } else if (outcome.result == IxInsertResult::kDuplicate) {
                        if (!index.unique) {
                            continue;
                        }
                        std::vector<Rid> result;
                        ih->get_value(new_key, &result, context_->txn_);
                        auto conflict = context_->txn_mgr_ == nullptr
                                            ? TransactionManager::UniqueKeyConflictResult::FAILURE
                                            : context_->txn_mgr_->ClassifyUniqueIndexConflict(
                                                  context_ == nullptr ? nullptr : context_->txn_, tab_name_, index,
                                                  new_key, result, &rid);
                        if (conflict == TransactionManager::UniqueKeyConflictResult::ABORT) {
                            throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                            AbortReason::DEADLOCK_PREVENTION,
                                                            AbortSubReason::UNIQUE_KEY_CONFLICT);
                        }
                        throw NonRetryableSqlExecutionError("Duplicate key on table " + tab_name_);
                    }
                }
            }
            PageId modified_page_id{};
            Page *modified_page = nullptr;
            bool defer_page_unpin = context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr;
            bool page_finalized = !defer_page_unpin;
            fh_->update_record(rid, new_rec->data, context_, defer_page_unpin ? &modified_page_id : nullptr,
                               defer_page_unpin, defer_page_unpin ? &modified_page : nullptr);
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                try {
                    UpdateLogRecord log_record(context_->txn_->get_transaction_id(), *old_rec, *new_rec, rid,
                                               tab_name_);
                    log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                    context_->txn_->set_prev_lsn(lsn);
                    if (!sm_manager_->get_bpm()->finalize_page_write_fast(modified_page, modified_page_id, lsn,
                                                                          true)) {
                        sm_manager_->get_bpm()->finalize_page_write(modified_page_id, lsn, true);
                    }
                    page_finalized = true;
                } catch (...) {
                    if (defer_page_unpin && !page_finalized) {
                        if (!sm_manager_->get_bpm()->unpin_page_fast(modified_page, modified_page_id, true)) {
                            sm_manager_->get_bpm()->unpin_page(modified_page_id, true);
                        }
                    }
                    throw;
                }
            }
        } catch (...) {
            rollback_inserted_index_keys();
            throw;
        }
    }

    std::unique_ptr<RmRecord> execute_single_non_index_update(Rid &rid) {
        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr &&
            !context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd())) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                            AbortSubReason::RECORD_LOCK_CONFLICT);
        }
        RmRecordPageCursor read_cursor(fh_);
        auto old_rec = std::make_unique<RmRecord>();
        if (!read_cursor.read_record(rid, old_rec.get())) {
            throw RecordNotFoundError(rid.page_no, rid.slot_no);
        }
        TransactionManager::TupleMutationState old_state;
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            old_state = context_->txn_mgr_->GetTupleMutationState(tab_name_, rid);
            context_->txn_mgr_->EnsureWriteConflictFree(context_->txn_, old_state.meta);
            if (old_state.meta.is_deleted_) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                                AbortSubReason::TARGET_NOT_VISIBLE);
            }
        }
        RmRecord new_rec(*old_rec);
        for (const auto &set_clause : compiled_set_clauses_) {
            set_clause.apply(new_rec.data, old_rec->data, set_clause);
        }
        if (memcmp(new_rec.data, old_rec->data, fh_->get_file_hdr().record_size) == 0) {
            return nullptr;
        }
        read_cursor.reset();
        apply_prepared_update(rid, std::move(old_rec), &new_rec, old_state.meta, old_state.version);
        return nullptr;
    }

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::shared_ptr<std::vector<Rid>> rids, Context *context,
                   std::shared_ptr<const PlanRuntimeCache> runtime_cache = nullptr) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = &sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
        index_bindings_ = rmdb::bind_table_indexes(sm_manager_, tab_name_, *tab_);
        if (runtime_cache != nullptr && runtime_cache->mutation_program != nullptr &&
            runtime_cache->mutation_program->Matches(set_clauses_.size(), index_bindings_.size(),
                                                      rmdb::sql_template_schema_epoch())) {
            mutation_program_ = runtime_cache->mutation_program;
        }
        index_key_touched_.assign(index_bindings_.size(), false);
        changed_cols_.reserve(set_clauses_.size());
        for (const auto &set_clause : set_clauses_) {
            changed_cols_.push_back(set_clause.lhs.col_name);
        }
        if (mutation_program_ != nullptr) {
            index_key_touched_ = mutation_program_->index_touched();
            update_touches_index_columns_ = mutation_program_->touches_index();
            key_conflict_check_required_ = mutation_program_->key_conflict_required();
        } else {
            for (const auto &col_name : changed_cols_) {
                if (col_name == "id") {
                    key_conflict_check_required_ = true;
                }
                for (size_t i = 0; i < index_bindings_.size(); ++i) {
                    const auto &index = *index_bindings_[i].meta;
                    for (const auto &index_col : index.cols) {
                        if (index_col.name == col_name) {
                            index_key_touched_[i] = true;
                            update_touches_index_columns_ = true;
                            key_conflict_check_required_ = key_conflict_check_required_ || index.unique;
                            break;
                        }
                    }
                }
            }
        }
        owned_index_key_scratch_.reserve(index_bindings_.size());
        owned_index_key_scratch_alt_.reserve(index_bindings_.size());
        index_key_scratch_.assign(index_bindings_.size(), nullptr);
        index_key_scratch_alt_.assign(index_bindings_.size(), nullptr);
        for (size_t i = 0; i < index_bindings_.size(); ++i) {
            if (!index_key_touched_[i]) {
                continue;
            }
            const size_t key_len = static_cast<size_t>(index_bindings_[i].meta->col_tot_len);
            std::string *old_scratch = context_ == nullptr ? nullptr : context_->acquire_statement_string(key_len);
            std::string *new_scratch = context_ == nullptr ? nullptr : context_->acquire_statement_string(key_len);
            if (old_scratch == nullptr) {
                owned_index_key_scratch_.emplace_back(key_len, '\0');
                old_scratch = &owned_index_key_scratch_.back();
            }
            if (new_scratch == nullptr) {
                owned_index_key_scratch_alt_.emplace_back(key_len, '\0');
                new_scratch = &owned_index_key_scratch_alt_.back();
            }
            index_key_scratch_[i] = old_scratch;
            index_key_scratch_alt_[i] = new_scratch;
        }
        compile_set_clauses();
    }
    std::unique_ptr<RmRecord> Next() override {
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->PromoteToWrite(context_->txn_, context_->log_mgr_);
        }
        if (!update_touches_index_columns_ && rids_->size() == 1) {
            return execute_single_non_index_update(rids_->front());
        }
        std::vector<Rid> effective_rids;
        std::vector<std::unique_ptr<RmRecord>> old_records;
        std::vector<std::unique_ptr<RmRecord>> new_records;
        std::vector<TupleMeta> old_metas;
        std::vector<std::optional<VersionUndoLink>> old_versions;
        old_records.reserve(rids_->size());
        new_records.reserve(rids_->size());
        old_metas.reserve(rids_->size());
        old_versions.reserve(rids_->size());
        effective_rids.reserve(rids_->size());
        for (const auto &rid : *rids_) {
            if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr &&
                !context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd())) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                                AbortSubReason::RECORD_LOCK_CONFLICT);
            }
        }
        RmRecordPageCursor read_cursor(fh_);
        for (auto &rid : *rids_) {
            auto rec = std::make_unique<RmRecord>();
            if (!read_cursor.read_record(rid, rec.get())) {
                throw RecordNotFoundError(rid.page_no, rid.slot_no);
            }
            TransactionManager::TupleMutationState current_state;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                current_state = context_->txn_mgr_->GetTupleMutationState(tab_name_, rid);
                context_->txn_mgr_->EnsureWriteConflictFree(context_->txn_, current_state.meta);
                if (current_state.meta.is_deleted_) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::DEADLOCK_PREVENTION,
                                                    AbortSubReason::TARGET_NOT_VISIBLE);
                }
            }
            auto new_rec = std::make_unique<RmRecord>(*rec);
            for (auto &set_clause : compiled_set_clauses_) {
                set_clause.apply(new_rec->data, rec->data, set_clause);
            }
            if (memcmp(new_rec->data, rec->data, fh_->get_file_hdr().record_size) == 0) {
                continue;
            }
            if (key_conflict_check_required_ && context_ != nullptr && context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->EnsureKeyConflictFree(context_->txn_, tab_name_, *tab_, *new_rec, &rid);
            }
            effective_rids.push_back(rid);
            old_records.push_back(std::move(rec));
            new_records.push_back(std::move(new_rec));
            old_metas.push_back(current_state.meta);
            old_versions.push_back(current_state.version);
        }
        read_cursor.reset();

        if (update_touches_index_columns_) {
            for (size_t rec_idx = 0; rec_idx < new_records.size(); ++rec_idx) {
                for (size_t i = 0; i < index_bindings_.size(); ++i) {
                    if (!index_key_touched_[i]) {
                        continue;
                    }
                    const auto &binding = index_bindings_[i];
                    const auto &index = *binding.meta;
                    if (!index.unique) {
                        continue;
                    }
                    auto *ih = binding.ih;
                    char *key = rmdb::build_index_key_into(index, new_records[rec_idx]->data, index_key_scratch_[i]);
                    std::vector<Rid> result;
                    if (ih->get_value(key, &result, context_->txn_)) {
                        for (auto &existing : result) {
                            if (existing != effective_rids[rec_idx]) {
                                auto conflict = context_->txn_mgr_ == nullptr
                                                    ? TransactionManager::UniqueKeyConflictResult::FAILURE
                                                    : context_->txn_mgr_->ClassifyUniqueIndexConflict(
                                                          context_ == nullptr ? nullptr : context_->txn_, tab_name_,
                                                          index, key, result, &effective_rids[rec_idx]);
                                if (conflict == TransactionManager::UniqueKeyConflictResult::ABORT) {
                                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                                    AbortReason::DEADLOCK_PREVENTION,
                                                                    AbortSubReason::UNIQUE_KEY_CONFLICT);
                                }
                                throw NonRetryableSqlExecutionError("Duplicate key on table " + tab_name_);
                            }
                        }
                    }
                }
            }
        }

        for (size_t rec_idx = 0; rec_idx < effective_rids.size(); ++rec_idx) {
            apply_prepared_update(effective_rids[rec_idx], std::move(old_records[rec_idx]),
                                  new_records[rec_idx].get(), old_metas[rec_idx], old_versions[rec_idx]);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
