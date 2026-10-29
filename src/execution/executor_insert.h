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
#include "index/ix.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "recovery/log_manager.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class InsertExecutor : public AbstractExecutor {
   private:
    const TabMeta *tab_ = nullptr;  // 表的元数据
    const std::vector<Value> *values_ = nullptr;  // 由 Portal 中的 DMLPlan 持有
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;
    std::vector<rmdb::IndexBinding> index_bindings_;
    std::vector<std::string> owned_index_key_scratch_;
    std::vector<std::string *> index_key_scratch_;
    BufferAccessStrategy cold_write_strategy_{BufferAccessClass::ColdWrite};
    bool use_cold_write_strategy_ = false;
    struct InsertedIndexKey {
        IxIndexHandle *ih;
        std::string key;
    };

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Value> &values, Context *context,
                   std::shared_ptr<const PlanRuntimeCache> runtime_cache = nullptr) {
        sm_manager_ = sm_manager;
        tab_ = &sm_manager_->db_.get_table(tab_name);
        values_ = &values;
        tab_name_ = tab_name;
        if (values.size() != tab_->cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        index_bindings_ = rmdb::bind_table_indexes(sm_manager_, tab_name_, *tab_);
        owned_index_key_scratch_.reserve(index_bindings_.size());
        index_key_scratch_.reserve(index_bindings_.size());
        for (size_t i = 0; i < index_bindings_.size(); ++i) {
            std::string *scratch = context_ == nullptr
                                       ? nullptr
                                       : context_->acquire_statement_string(index_bindings_[i].meta->col_tot_len);
            if (scratch == nullptr) {
                owned_index_key_scratch_.emplace_back(index_bindings_[i].meta->col_tot_len, '\0');
                scratch = &owned_index_key_scratch_.back();
            }
            index_key_scratch_.push_back(scratch);
        }
        use_cold_write_strategy_ = context_ != nullptr && context_->txn_ == nullptr &&
                                   context_->txn_mgr_ == nullptr && context_->log_mgr_ == nullptr;
    };

    std::unique_ptr<RmRecord> Next() override {
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->PromoteToWrite(context_->txn_, context_->log_mgr_);
        }
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_->size(); i++) {
            const auto &col = tab_->cols[i];
            const auto &val = (*values_)[i];
            char *dst = rec.data + col.offset;
            if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                rmdb::store_unaligned<float>(dst, static_cast<float>(val.int_val));
            } else if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            } else if (col.type == TYPE_INT) {
                rmdb::store_unaligned<int>(dst, val.int_val);
            } else if (col.type == TYPE_FLOAT) {
                if (!std::isfinite(val.float_val)) {
                    throw RMDBError("FLOAT value must be finite");
                }
                rmdb::store_unaligned<float>(dst, val.float_val);
            } else if (col.type == TYPE_STRING) {
                if (val.str_val.size() > static_cast<size_t>(col.len)) {
                    throw StringOverflowError();
                }
                memset(dst, 0, static_cast<size_t>(col.len));
                memcpy(dst, val.str_val.data(), val.str_val.size());
            }
        }
        if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->EnsureKeyConflictFree(context_ == nullptr ? nullptr : context_->txn_,
                                                      tab_name_, *tab_, rec);
        }
        for (size_t i = 0; i < index_bindings_.size(); ++i) {
            const auto &binding = index_bindings_[i];
            const auto &index = *binding.meta;
            if (!index.unique) {
                continue;
            }
            auto *ih = binding.ih;
            char *key = rmdb::build_index_key_into(index, rec.data, index_key_scratch_[i]);
            std::vector<Rid> result;
            if (ih->get_value(key, &result, context_->txn_) && !result.empty()) {
                auto conflict = context_->txn_mgr_ == nullptr
                                    ? TransactionManager::UniqueKeyConflictResult::FAILURE
                                    : context_->txn_mgr_->ClassifyUniqueIndexConflict(
                                          context_ == nullptr ? nullptr : context_->txn_, tab_name_, index, key,
                                          result);
                if (conflict == TransactionManager::UniqueKeyConflictResult::ABORT) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::DEADLOCK_PREVENTION,
                                                    AbortSubReason::UNIQUE_KEY_CONFLICT);
                }
                throw RMDBError("Duplicate key on table " + tab_name_);
            }
        }
        PageId modified_page_id{};
        Page *modified_page = nullptr;
        bool defer_page_unpin = context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr;
        bool page_finalized = !defer_page_unpin;
        bool write_record_appended = false;
        bool insert_pending_visibility = false;
        std::vector<InsertedIndexKey> inserted_index_keys;
        inserted_index_keys.reserve(index_bindings_.size());
        auto rollback_inserted_index_keys = [&]() {
            Transaction *txn = context_ == nullptr ? nullptr : context_->txn_;
            for (auto iter = inserted_index_keys.rbegin(); iter != inserted_index_keys.rend(); ++iter) {
                iter->ih->delete_entry(iter->key.data(), txn);
            }
            inserted_index_keys.clear();
        };
        auto forget_insert_write_record = [&]() {
            if (context_ == nullptr || context_->txn_ == nullptr || !write_record_appended) {
                return;
            }
            auto &write_set = context_->txn_->get_write_set();
            if (write_set.empty()) {
                return;
            }
            WriteRecord &write_record = write_set.back();
            if (write_record.GetWriteType() == WType::INSERT_TUPLE &&
                write_record.GetTableName() == tab_name_ && write_record.GetRid() == rid_) {
                write_set.pop_back();
                write_record_appended = false;
            }
        };
        // Insert into record file
        bool defer_visibility = context_ != nullptr && context_->txn_ != nullptr && context_->txn_mgr_ != nullptr;
        rid_ = fh_->insert_record(rec.data, context_, defer_page_unpin ? &modified_page_id : nullptr,
                                  defer_page_unpin, defer_page_unpin ? &modified_page : nullptr,
                                  use_cold_write_strategy_ ? &cold_write_strategy_ : nullptr, defer_visibility);
        insert_pending_visibility = defer_visibility;
        try {
            if (context_ != nullptr && context_->txn_ != nullptr) {
                context_->txn_mgr_->UpdateTupleMeta(
                    tab_name_, rid_, TupleMeta{TXN_START_ID + context_->txn_->get_transaction_id(), false});
                fh_->publish_pending_insert(rid_);
                insert_pending_visibility = false;
                context_->txn_->emplace_write_record(WType::INSERT_TUPLE, tab_name_, rid_);
                write_record_appended = true;
            }
            // Insert into index
            for (size_t i = 0; i < index_bindings_.size(); ++i) {
                const auto &binding = index_bindings_[i];
                const auto &index = *binding.meta;
                auto *ih = binding.ih;
                char *key = rmdb::build_index_key_into(index, rec.data, rid_, index_key_scratch_[i]);
                auto outcome = ih->insert_entry(key, rid_, context_->txn_);
                if (outcome.result == IxInsertResult::kInserted) {
                    inserted_index_keys.push_back(InsertedIndexKey{ih, std::string(key, index.col_tot_len)});
                } else if (outcome.result == IxInsertResult::kDuplicate) {
                    if (!index.unique) {
                        continue;
                    }
                    std::vector<Rid> result;
                    ih->get_value(key, &result, context_->txn_);
                    auto conflict = context_->txn_mgr_ == nullptr
                                        ? TransactionManager::UniqueKeyConflictResult::FAILURE
                                        : context_->txn_mgr_->ClassifyUniqueIndexConflict(
                                              context_ == nullptr ? nullptr : context_->txn_, tab_name_, index, key,
                                              result);
                    if (conflict == TransactionManager::UniqueKeyConflictResult::ABORT) {
                        throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                        AbortReason::DEADLOCK_PREVENTION,
                                                        AbortSubReason::UNIQUE_KEY_CONFLICT);
                    }
                    throw RMDBError("Duplicate key on table " + tab_name_);
                }
            }
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->RecordSerializableWrite(context_->txn_, tab_name_, rid_, nullptr, &rec,
                                                            &tab_->cols);
            }
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
                if (!sm_manager_->get_bpm()->finalize_page_write_fast(modified_page, modified_page_id, lsn, true)) {
                    sm_manager_->get_bpm()->finalize_page_write(modified_page_id, lsn, true);
                }
                page_finalized = true;
            }
        } catch (...) {
            if (defer_page_unpin && !page_finalized) {
                if (!sm_manager_->get_bpm()->unpin_page_fast(modified_page, modified_page_id, true)) {
                    sm_manager_->get_bpm()->unpin_page(modified_page_id, true);
                }
            }
            rollback_inserted_index_keys();
            forget_insert_write_record();
            try {
                if (insert_pending_visibility) {
                    fh_->rollback_pending_insert(rid_);
                    insert_pending_visibility = false;
                } else {
                    fh_->delete_record(rid_, nullptr);
                }
            } catch (...) {
            }
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->UpdateTupleMeta(tab_name_, rid_, std::nullopt);
                context_->txn_mgr_->UpdateVersionLink(tab_name_, rid_, std::nullopt);
            }
            throw;
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};
