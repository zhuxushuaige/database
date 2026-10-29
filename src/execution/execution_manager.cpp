/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include <cstring>
#include <string_view>
#include <vector>

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "system/sm_manager.h"
#include "record_printer.h"
#include "recovery/log_manager.h"

const char *help_info = "Supported SQL syntax:\n"
                   "  command ;\n"
                   "command:\n"
                   "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                   "  DROP TABLE table_name\n"
                   "  CREATE INDEX table_name (column_name)\n"
                   "  DROP INDEX table_name (column_name)\n"
                   "  INSERT INTO table_name VALUES (value [, value ...])\n"
                   "  DELETE FROM table_name [WHERE where_clause]\n"
                   "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                   "  SELECT selector FROM table_name [WHERE where_clause]\n"
                   "type:\n"
                   "  {INT | FLOAT | CHAR(n)}\n"
                   "where_clause:\n"
                   "  condition [AND condition ...]\n"
                   "condition:\n"
                   "  column op {column | value}\n"
                   "column:\n"
                   "  [table_name.]column_name\n"
                   "op:\n"
                   "  {= | <> | < | > | <= | >=}\n"
                   "selector:\n"
                   "  {* | column [, column ...]}\n";

namespace {

constexpr size_t kOutputColumnWidth = 16;

void append_fixed_width_cell(std::string *row, std::string_view cell) {
    row->append("| ");
    if (cell.size() > kOutputColumnWidth) {
        row->append(cell.data(), kOutputColumnWidth - 3);
        row->append("...");
    } else {
        row->append(kOutputColumnWidth - cell.size(), ' ');
        if (!cell.empty()) {
            row->append(cell.data(), cell.size());
        }
    }
    row->push_back(' ');
}

size_t format_int_cell(int value, char *out) {
    char digits[16];
    size_t digit_count = 0;
    bool negative = value < 0;
    unsigned int magnitude = 0;
    if (negative) {
        magnitude = static_cast<unsigned int>(-(value + 1)) + 1;
    } else {
        magnitude = static_cast<unsigned int>(value);
    }
    do {
        digits[digit_count++] = static_cast<char>('0' + (magnitude % 10));
        magnitude /= 10;
    } while (magnitude != 0);

    size_t pos = 0;
    if (negative) {
        out[pos++] = '-';
    }
    while (digit_count > 0) {
        out[pos++] = digits[--digit_count];
    }
    return pos;
}

void append_cell_to_rows(const char *rec_buf, const ColMeta &col, std::string *client_row) {
    char number_buf[64];
    std::string_view cell;
    if (col.type == TYPE_INT) {
        size_t len = format_int_cell(rmdb::load_unaligned<int>(rec_buf), number_buf);
        cell = std::string_view(number_buf, len);
    } else if (col.type == TYPE_FLOAT) {
        int len = snprintf(number_buf, sizeof(number_buf), "%.6f", rmdb::load_unaligned<float>(rec_buf));
        cell = len <= 0 ? std::string_view()
                        : std::string_view(number_buf, std::min(static_cast<size_t>(len), sizeof(number_buf) - 1));
    } else if (col.type == TYPE_STRING) {
        const void *nul = std::memchr(rec_buf, '\0', static_cast<size_t>(col.len));
        size_t len = nul == nullptr ? static_cast<size_t>(col.len)
                                    : static_cast<const char *>(nul) - rec_buf;
        cell = std::string_view(rec_buf, len);
    } else {
        cell = std::string_view();
    }
    if (client_row != nullptr) {
        append_fixed_width_cell(client_row, cell);
    }
}

void append_client_row(std::string *row, Context *context) {
    if (context->ellipsis_) {
        return;
    }
    row->append("|\n");
    if (*context->offset_ + RECORD_COUNT_LENGTH + row->length() < BUFFER_LENGTH) {
        memcpy(context->data_send_ + *(context->offset_), row->data(), row->length());
        *(context->offset_) += row->length();
    } else {
        context->ellipsis_ = true;
    }
}

}  // namespace

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context){
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch(x->tag) {
            case T_CreateTable:
            {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                rmdb::publish_schema_change();
                break;
            }
            case T_DropTable:
            {
                sm_manager_->drop_table(x->tab_name_, context);
                rmdb::publish_schema_change();
                break;
            }
            case T_CreateIndex:
            {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                rmdb::publish_schema_change();
                break;
            }
            case T_DropIndex:
            {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                rmdb::publish_schema_change();
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;  
        }
    } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
        if (x->tag == T_Load) {
            rmdb::load_csv_into_table(sm_manager_, x->tab_name_, x->file_name_);
            rmdb::publish_bulk_data_change();
            return;
        }
        throw InternalError("Unexpected dml type in run_mutli_query");
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto set_isolation = std::dynamic_pointer_cast<SetTransactionIsolationPlan>(plan)) {
        if (context != nullptr && context->session_isolation_ != nullptr) {
            *(context->session_isolation_) = set_isolation->isolation_level_;
        }
        return;
    }
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch(x->tag) {
            case T_Help:
            {
                memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
                *(context->offset_) = strlen(help_info);
                break;
            }
            case T_ShowTable:
            {
                sm_manager_->show_tables(context);
                break;
            }
            case T_ShowIndex:
            {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case T_DescTable:
            {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case T_Transaction_begin:
            {
                // 显示开启一个事务
                if (context->txn_ != nullptr) {
                    context->txn_->set_txn_mode(true);
                    *txn_id = context->txn_->get_transaction_id();
                }
                break;
            }  
            case T_Transaction_commit:
            {
                if (context->txn_ != nullptr) {
                    txn_mgr_->commit(context->txn_, context->log_mgr_);
                    context->txn_ = nullptr;
                }
                *txn_id = INVALID_TXN_ID;
                break;
            }    
            case T_Transaction_rollback:
            {
                if (context->txn_ != nullptr) {
                    txn_mgr_->abort(context->txn_, context->log_mgr_);
                    context->txn_ = nullptr;
                }
                *txn_id = INVALID_TXN_ID;
                break;
            }    
            case T_Transaction_abort:
            {
                if (context->txn_ != nullptr) {
                    txn_mgr_->abort(context->txn_, context->log_mgr_);
                    context->txn_ = nullptr;
                }
                *txn_id = INVALID_TXN_ID;
                break;
            }
            case T_Checkpoint:
            {
                // 1. Capture active transactions and their latest log positions.
                Transaction *exclude_txn =
                    context != nullptr && context->txn_ != nullptr && !context->txn_->get_txn_mode() ? context->txn_ : nullptr;
                auto active_txns = txn_mgr_->CollectActiveTxnCheckpointInfo(exclude_txn);
                // 2. Flush pending log buffer
                if (context->log_mgr_ != nullptr) {
                    context->log_mgr_->flush_log_to_disk();
                }
                // 3. Write checkpoint log record
                CheckpointLogRecord ckpt_record(active_txns);
                lsn_t ckpt_lsn = INVALID_LSN;
                if (context->log_mgr_ != nullptr) {
                    ckpt_lsn = context->log_mgr_->add_log_to_buffer(&ckpt_record);
                    context->log_mgr_->flush_log_to_disk();
                }
                // 4. Flush all data pages to disk
                for (auto &entry : sm_manager_->fhs_) {
                    entry.second->flush();
                }
                for (auto &entry : sm_manager_->ihs_) {
                    entry.second->flush();
                }
                // 5. Write restart file with checkpoint LSN
                if (ckpt_lsn != INVALID_LSN) {
                    std::ofstream chkpt_file("db.chkpt", std::ios::trunc);
                    chkpt_file << ckpt_lsn;
                    chkpt_file.close();
                }
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;
        }

    } else if(auto x = std::dynamic_pointer_cast<ExplainPlan>(plan)) {
        for (auto &line : x->lines_) {
            std::string out = line + "\n";
            memcpy(context->data_send_ + *(context->offset_), out.c_str(), out.length());
            *(context->offset_) += out.length();
        }
    } else if(auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
        switch (x->set_knob_type_)
        {
        case ast::SetKnobType::EnableNestLoop: {
            planner_->set_enable_nestedloop_join(x->bool_value_);
            break;
        }
        case ast::SetKnobType::EnableSortMerge: {
            planner_->set_enable_sortmerge_join(x->bool_value_);
            break;
        }
        default: {
            throw RMDBError("Not implemented!\n");
            break;
        }
        }
    }
}

// 执行select语句并将结果返回客户端。
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, 
                            Context *context) {
    executorTreeRoot->beginTuple();

    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.output_name.empty() ? sel_col.col_name : sel_col.output_name);
    }

    std::vector<ColMeta> output_cols;
    std::vector<size_t> output_col_idxs;
    output_cols.reserve(sel_cols.size());
    output_col_idxs.reserve(sel_cols.size());
    const auto &root_cols = executorTreeRoot->cols();
    for (const auto &sel_col : sel_cols) {
        auto pos = executorTreeRoot->get_col(root_cols, sel_col);
        output_cols.push_back(*pos);
        if (!sel_col.output_name.empty()) {
            output_cols.back().name = sel_col.output_name;
        }
        output_col_idxs.push_back(static_cast<size_t>(pos - root_cols.begin()));
        const auto &col = output_cols.back();
        if (col.offset < 0 || col.len < 0 ||
            static_cast<size_t>(col.offset + col.len) > executorTreeRoot->tupleLen()) {
            throw RMDBError("invalid output column offset");
        }
    }

    if (context->result_writer_ != nullptr) {
        context->result_writer_->BeginResult(output_cols);
        rmdb::u64 row_count = 0;
        std::vector<const char *> cells(output_cols.size());
        for (; !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
            auto tuple = executorTreeRoot->ReadTupleView();
            if (!tuple) {
                break;
            }
            for (size_t i = 0; i < output_cols.size(); ++i) {
                cells[i] = tuple->cell_at(output_cols[i], output_col_idxs[i]);
            }
            context->result_writer_->WriteRow(output_cols, cells);
            ++row_count;
        }
        context->result_writer_->EndResult(row_count);
        return;
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (; !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto tuple = executorTreeRoot->ReadTupleView();
        if (!tuple) {
            break;
        }
        std::string client_row;
        client_row.reserve(output_cols.size() * (kOutputColumnWidth + 3) + 2);
        for (size_t i = 0; i < output_cols.size(); ++i) {
            const auto &col = output_cols[i];
            append_cell_to_rows(tuple->cell_at(col, output_col_idxs[i]), col, &client_row);
        }
        append_client_row(&client_row, context);
        num_rec++;
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec){
    exec->Next();
}


#include <array>
#include <cstring>
#include <unordered_set>
#include <utility>

#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

namespace {

class SnapshotIndexEntryCursor {
   public:
    void bind(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name) {
        txn_mgr_ = txn_mgr;
        txn_ = txn;
        tab_name_ = std::move(tab_name);
        table_info_ = txn_mgr_ == nullptr ? nullptr : txn_mgr_->GetOrCreateTableVersionInfo(tab_name_);
        reset();
    }

    void reset() {
        current_page_no_ = RM_NO_PAGE;
        page_info_ = nullptr;
        current_cache_ = nullptr;
        current_page_clean_visible_ = false;
        last_tuple_hint_.Reset();
        ++cache_generation_;
        if (cache_generation_ == 0) {
            for (auto &entry : page_cache_) {
                entry.generation = 0;
            }
            cache_generation_ = 1;
        }
    }

    TransactionManager::IndexEntryVisibilityState classify(const Rid &rid) {
        last_tuple_hint_.Reset();
        if (txn_mgr_ == nullptr) {
            return TransactionManager::IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
        }
        if (rid.page_no != current_page_no_ || current_cache_ == nullptr) {
            current_page_no_ = rid.page_no;
            current_cache_ = lookup_page_info(rid.page_no);
            page_info_ = current_cache_ == nullptr ? nullptr : current_cache_->page_info;
        } else if (page_info_ == nullptr && table_info_ != nullptr) {
            bool page_may_need_info = table_info_->tracks_exact_dirty_page(rid.page_no)
                                          ? table_info_->is_page_dirty_exact(rid.page_no)
                                          : table_info_->maybe_has_page_version_info(rid.page_no);
            if (page_may_need_info) {
                current_cache_ = lookup_page_info(rid.page_no);
                page_info_ = current_cache_ == nullptr ? nullptr : current_cache_->page_info;
            }
        }
        if (page_info_ == nullptr || current_cache_ == nullptr) {
            current_page_clean_visible_ = true;
            return TransactionManager::IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
        }
        const rmdb::u64 slot_epoch_before =
            page_info_->visibility_epoch_.load(std::memory_order_acquire);
        if (rid.slot_no >= 0 && page_info_->IsSlotClean(static_cast<slot_offset_t>(rid.slot_no))) {
            const rmdb::u64 slot_epoch_after =
                page_info_->visibility_epoch_.load(std::memory_order_acquire);
            if ((slot_epoch_before & 1U) != 0 && slot_epoch_before == slot_epoch_after) {
                current_page_clean_visible_ = false;
                return TransactionManager::IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
            }
        }
        current_page_clean_visible_ = page_clean_visible(*current_cache_);
        if (current_page_clean_visible_) {
            return TransactionManager::IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
        }
        return txn_mgr_->ClassifySnapshotIndexEntryOnPage(page_info_, rid, txn_, &last_tuple_hint_);
    }

    bool current_page_clean_visible() const { return current_page_clean_visible_; }

    const std::shared_ptr<TransactionManager::TableVersionInfo> &table_info() const {
        return table_info_;
    }

    bool current_page_still_clean_visible(page_id_t page_no) {
        if (!current_page_clean_visible_ || page_no != current_page_no_ || current_cache_ == nullptr) {
            return false;
        }
        if (page_info_ == nullptr) {
            if (table_info_ == nullptr) {
                return true;
            }
            if (table_info_->tracks_exact_dirty_page(page_no)) {
                return !table_info_->is_page_dirty_exact(page_no);
            }
            return !table_info_->maybe_has_page_version_info(page_no);
        }
        current_page_clean_visible_ = page_clean_visible(*current_cache_);
        return current_page_clean_visible_;
    }

   private:
    static constexpr size_t kPageCacheSize = 512;

    struct CachedPageInfo {
        rmdb::u64 generation = 0;
        page_id_t page_no = RM_NO_PAGE;
        bool valid = false;
        rmdb::u64 page_map_epoch = 0;
        TransactionManager::PageVersionInfo *page_info = nullptr;
        rmdb::u64 visibility_epoch = 0;
        bool clean_cached = false;
        bool clean_visible = false;
    };

    static void clear_cached_page_state(CachedPageInfo &entry) {
        entry.visibility_epoch = 0;
        entry.clean_cached = false;
        entry.clean_visible = false;
    }

    CachedPageInfo *lookup_page_info(page_id_t page_no) {
        const auto slot = static_cast<size_t>(page_no) & (kPageCacheSize - 1);
        auto &entry = page_cache_[slot];
        if (entry.generation == cache_generation_ && entry.valid && entry.page_no == page_no) {
            if (entry.page_info == nullptr) {
                if (table_info_ == nullptr) {
                    return &entry;
                }
                if (table_info_->tracks_exact_dirty_page(page_no)) {
                    if (!table_info_->is_page_dirty_exact(page_no)) {
                        return &entry;
                    }
                } else if (!table_info_->maybe_has_page_version_info(page_no)) {
                    return &entry;
                }
            } else {
                return &entry;
            }
            if (table_info_ != nullptr && !table_info_->tracks_exact_dirty_page(page_no)) {
                rmdb::u64 current_epoch = table_info_->page_map_epoch_.load(std::memory_order_acquire);
                if (entry.page_map_epoch == current_epoch) {
                    return &entry;
                }
            }
        }
        clear_cached_page_state(entry);
        entry.generation = cache_generation_;
        entry.page_no = page_no;
        entry.valid = true;
        entry.page_map_epoch = table_info_ == nullptr
                                   ? 0
                                   : table_info_->page_map_epoch_.load(std::memory_order_acquire);
        entry.page_info = nullptr;
        if (table_info_ == nullptr) {
            return &entry;
        }
        if (table_info_->tracks_exact_dirty_page(page_no)) {
            if (!table_info_->is_page_dirty_exact(page_no)) {
                return &entry;
            }
        } else if (!table_info_->maybe_has_page_version_info(page_no)) {
            return &entry;
        }
        rmdb::u64 page_map_epoch = 0;
        entry.page_info = txn_mgr_->GetPageVersionInfoOnTableRaw(table_info_, page_no, &page_map_epoch);
        entry.page_map_epoch = page_map_epoch;
        return &entry;
    }

    bool page_clean_visible(CachedPageInfo &entry) {
        if (entry.page_info == nullptr) {
            return true;
        }
        rmdb::u64 epoch_before = entry.page_info->visibility_epoch_.load(std::memory_order_acquire);
        if ((epoch_before & 1U) == 0) {
            entry.clean_cached = false;
            entry.clean_visible = false;
            entry.visibility_epoch = epoch_before;
            return false;
        }
        if (entry.clean_cached && entry.visibility_epoch == epoch_before) {
            const bool cached_visible = entry.clean_visible;
            const rmdb::u64 epoch_after =
                entry.page_info->visibility_epoch_.load(std::memory_order_acquire);
            if (epoch_before == epoch_after) {
                return cached_visible;
            }
            entry.clean_cached = false;
            entry.clean_visible = false;
            entry.visibility_epoch = epoch_after;
            return false;
        }
        bool clean = txn_mgr_->IsSnapshotPageCleanVisible(entry.page_info, txn_);
        rmdb::u64 epoch_after = entry.page_info->visibility_epoch_.load(std::memory_order_acquire);
        if (epoch_before != epoch_after || (epoch_after & 1U) == 0) {
            entry.clean_cached = false;
            entry.clean_visible = false;
            entry.visibility_epoch = epoch_after;
            return false;
        }
        entry.clean_cached = true;
        entry.clean_visible = clean;
        entry.visibility_epoch = epoch_after;
        return clean;
    }

    TransactionManager *txn_mgr_ = nullptr;
    Transaction *txn_ = nullptr;
    std::string tab_name_;
    std::shared_ptr<TransactionManager::TableVersionInfo> table_info_;
    page_id_t current_page_no_ = RM_NO_PAGE;
    TransactionManager::PageVersionInfo *page_info_ = nullptr;
    std::array<CachedPageInfo, kPageCacheSize> page_cache_;
    rmdb::u64 cache_generation_ = 1;
    CachedPageInfo *current_cache_ = nullptr;
    bool current_page_clean_visible_ = false;
    TransactionManager::IndexEntryTupleHint last_tuple_hint_;
};

rmdb::u64 rid_key(const Rid &rid) {
    return (static_cast<rmdb::u64>(static_cast<rmdb::u32>(rid.page_no)) << 32) |
           static_cast<rmdb::u32>(rid.slot_no);
}

}  // namespace

class VisibleIndexCursor::Impl {
   public:
    void bind(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
              const std::vector<std::string> *index_col_names, const IndexMeta *index_meta,
              RmFileHandle *file_handle) {
        txn_mgr_ = txn_mgr;
        txn_ = txn;
        tab_name_ = std::move(tab_name);
        index_col_names_ = index_col_names;
        index_meta_ = index_meta;
        snapshot_entry_.bind(txn_mgr_, txn_, tab_name_);
        heap_page_cursor_.bind(file_handle);
        visible_key_scratch_.resize(index_meta_ == nullptr ? 0 : index_meta_->col_tot_len);
        reset();
    }

    void reset() {
        snapshot_entry_.reset();
        heap_page_cursor_.reset();
        history_loaded_ = false;
        history_rids_.clear();
        history_keys_.clear();
    }

    void finish() { heap_page_cursor_.reset(); }

    void reset_for_pool() {
        reset();
        txn_mgr_ = nullptr;
        txn_ = nullptr;
        tab_name_.clear();
        index_col_names_ = nullptr;
        index_meta_ = nullptr;
        heap_page_cursor_.bind(nullptr);
        visible_key_scratch_.clear();
    }

    const std::vector<Rid> &history_rids() {
        load_history();
        return history_rids_;
    }

    bool is_historical(const Rid &rid) {
        load_history();
        return history_keys_.find(rid_key(rid)) != history_keys_.end();
    }

    void deduplicate_and_append_history(std::vector<Rid> *candidates) {
        if (candidates == nullptr) {
            return;
        }
        load_history();
        std::unordered_set<rmdb::u64> seen;
        seen.reserve(candidates->size() + history_rids_.size());
        size_t output = 0;
        for (const auto &rid : *candidates) {
            if (!seen.insert(rid_key(rid)).second) {
                continue;
            }
            (*candidates)[output++] = rid;
        }
        candidates->resize(output);
        for (const auto &rid : history_rids_) {
            if (seen.insert(rid_key(rid)).second) {
                candidates->push_back(rid);
            }
        }
    }

    VisibleIndexCursor::ReadResult read_current(const Rid &rid, const char *index_key, RmRecord *out_record,
                                                bool materialize_index_only, bool require_matching_key) {
        auto state = snapshot_entry_.classify(rid);
        if (state == TransactionManager::IndexEntryVisibilityState::INVISIBLE) {
            return {};
        }
        if (state == TransactionManager::IndexEntryVisibilityState::CURRENT_KEY_VISIBLE && !materialize_index_only) {
            return {VisibleIndexCursor::ReadState::IndexOnly, false};
        }
        if (!read_visible(rid, out_record)) {
            return {VisibleIndexCursor::ReadState::Invisible, true};
        }
        if (require_matching_key && !record_matches_index_key(rid, index_key, *out_record)) {
            return {VisibleIndexCursor::ReadState::Invisible, true};
        }
        return {VisibleIndexCursor::ReadState::Materialized, true};
    }

    VisibleIndexCursor::ReadResult read_visible_entry(const Rid &rid, RmRecord *out_record) {
        if (!read_visible(rid, out_record)) {
            return {VisibleIndexCursor::ReadState::Invisible, true};
        }
        return {VisibleIndexCursor::ReadState::Materialized, true};
    }

    bool current_page_clean_visible() const { return snapshot_entry_.current_page_clean_visible(); }

    bool current_page_still_clean_visible(page_id_t page_no) {
        return snapshot_entry_.current_page_still_clean_visible(page_no);
    }

   private:
    void load_history() {
        if (history_loaded_) {
            return;
        }
        history_loaded_ = true;
        if (txn_mgr_ == nullptr || txn_ == nullptr || index_meta_ == nullptr) {
            return;
        }
        auto candidates = txn_mgr_->LookupSnapshotIndexHistory(tab_name_, *index_meta_, txn_->get_read_ts());
        history_rids_.reserve(candidates.size());
        history_keys_.reserve(candidates.size());
        for (const auto &rid : candidates) {
            if (history_keys_.insert(rid_key(rid)).second) {
                history_rids_.push_back(rid);
            }
        }
    }

    bool read_visible(const Rid &rid, RmRecord *out_record) {
        return txn_mgr_ != nullptr && out_record != nullptr &&
               txn_mgr_->GetVisibleTupleInto(tab_name_, snapshot_entry_.table_info(), rid, txn_, out_record,
                                             nullptr, &heap_page_cursor_);
    }

    bool record_matches_index_key(const Rid &rid, const char *index_key, const RmRecord &record) {
        if (index_meta_ == nullptr || index_key == nullptr || record.data == nullptr) {
            return false;
        }
        char *visible_key = rmdb::build_index_key_into(*index_meta_, record.data, rid, &visible_key_scratch_);
        return std::memcmp(visible_key, index_key, index_meta_->col_tot_len) == 0;
    }

    TransactionManager *txn_mgr_{nullptr};
    Transaction *txn_{nullptr};
    std::string tab_name_;
    const std::vector<std::string> *index_col_names_{nullptr};
    const IndexMeta *index_meta_{nullptr};
    SnapshotIndexEntryCursor snapshot_entry_;
    RmRecordPageCursor heap_page_cursor_;
    std::string visible_key_scratch_;
    bool history_loaded_{false};
    std::vector<Rid> history_rids_;
    std::unordered_set<rmdb::u64> history_keys_;
};

std::unique_ptr<VisibleIndexCursor::Impl> VisibleIndexCursor::acquire_impl() {
    auto &pool = impl_pool();
    if (pool.empty()) {
        return std::make_unique<Impl>();
    }
    auto impl = std::move(pool.back());
    pool.pop_back();
    return impl;
}

void VisibleIndexCursor::release_impl(std::unique_ptr<Impl> impl) {
    if (impl == nullptr) {
        return;
    }
    impl->reset_for_pool();
    auto &pool = impl_pool();
    if (pool.size() < 8) {
        pool.push_back(std::move(impl));
    }
}

std::vector<std::unique_ptr<VisibleIndexCursor::Impl>> &VisibleIndexCursor::impl_pool() {
    thread_local std::vector<std::unique_ptr<Impl>> pool;
    return pool;
}

VisibleIndexCursor::VisibleIndexCursor() : impl_(acquire_impl()) {}

VisibleIndexCursor::VisibleIndexCursor(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
                                       const std::vector<std::string> *index_col_names,
                                       const IndexMeta *index_meta, RmFileHandle *file_handle)
    : VisibleIndexCursor() {
    bind(txn_mgr, txn, std::move(tab_name), index_col_names, index_meta, file_handle);
}

VisibleIndexCursor::~VisibleIndexCursor() { release_impl(std::move(impl_)); }
VisibleIndexCursor::VisibleIndexCursor(VisibleIndexCursor &&) noexcept = default;
VisibleIndexCursor &VisibleIndexCursor::operator=(VisibleIndexCursor &&other) noexcept {
    if (this != &other) {
        release_impl(std::move(impl_));
        impl_ = std::move(other.impl_);
    }
    return *this;
}

void VisibleIndexCursor::bind(TransactionManager *txn_mgr, Transaction *txn, std::string tab_name,
                              const std::vector<std::string> *index_col_names, const IndexMeta *index_meta,
                              RmFileHandle *file_handle) {
    impl_->bind(txn_mgr, txn, std::move(tab_name), index_col_names, index_meta, file_handle);
}

void VisibleIndexCursor::reset() { impl_->reset(); }
void VisibleIndexCursor::finish() { impl_->finish(); }
const std::vector<Rid> &VisibleIndexCursor::history_rids() { return impl_->history_rids(); }
bool VisibleIndexCursor::is_historical(const Rid &rid) { return impl_->is_historical(rid); }
void VisibleIndexCursor::deduplicate_and_append_history(std::vector<Rid> *candidates) {
    impl_->deduplicate_and_append_history(candidates);
}
VisibleIndexCursor::ReadResult VisibleIndexCursor::read_current(const Rid &rid, const char *index_key,
                                                                RmRecord *out_record,
                                                                bool materialize_index_only,
                                                                bool require_matching_key) {
    return impl_->read_current(rid, index_key, out_record, materialize_index_only, require_matching_key);
}
VisibleIndexCursor::ReadResult VisibleIndexCursor::read_visible_entry(const Rid &rid, RmRecord *out_record) {
    return impl_->read_visible_entry(rid, out_record);
}
bool VisibleIndexCursor::current_page_clean_visible() const { return impl_->current_page_clean_visible(); }
bool VisibleIndexCursor::current_page_still_clean_visible(page_id_t page_no) {
    return impl_->current_page_still_clean_visible(page_no);
}


#include <cerrno>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "errors.h"
#include "index/ix.h"
#include "common/common.h"
#include "record/rm_defs.h"
#include "record/rm_scan.h"
#include "system/sm_manager.h"

namespace rmdb {
namespace {

class CsvDecodeError : public std::exception {
   public:
    CsvDecodeError(size_t column, std::string reason) : column_(column), reason_(std::move(reason)) {}

    const char *what() const noexcept override { return reason_.c_str(); }
    size_t column() const { return column_; }

   private:
    size_t column_;
    std::string reason_;
};

std::string normalize_load_file_name(std::string file_name) {
    if (file_name.size() >= 2) {
        char first = file_name.front();
        char last = file_name.back();
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            file_name = file_name.substr(1, file_name.size() - 2);
        }
    }
    return file_name;
}

std::string error_detail(const RMDBError &error) {
    std::string detail = error.what();
    constexpr std::string_view prefix = "Error: ";
    if (detail.compare(0, prefix.size(), prefix.data(), prefix.size()) == 0) {
        detail.erase(0, prefix.size());
    }
    return detail;
}

[[noreturn]] void throw_load_error(const std::string &tab_name, const std::string &file_name,
                                   size_t line, size_t column, const std::string &column_name,
                                   const std::string &reason) {
    std::string message = "load failed table=" + tab_name + " file=" + file_name +
                          " line=" + std::to_string(line);
    if (column > 0) {
        message += " column=" + std::to_string(column);
        if (!column_name.empty()) {
            message += "(" + column_name + ")";
        }
    }
    message += ": " + reason;
    throw RMDBError(message);
}

void split_csv_line_views(std::string *line, std::vector<std::string_view> *fields) {
    if (!line->empty() && line->back() == '\r') {
        line->pop_back();
    }
    fields->clear();
    size_t read_pos = 0;
    size_t write_pos = 0;

    while (true) {
        size_t column = fields->size() + 1;
        size_t field_start = write_pos;
        if (read_pos < line->size() && (*line)[read_pos] == '"') {
            ++read_pos;
            bool closed = false;
            while (read_pos < line->size()) {
                char ch = (*line)[read_pos++];
                if (ch != '"') {
                    (*line)[write_pos++] = ch;
                    continue;
                }
                if (read_pos < line->size() && (*line)[read_pos] == '"') {
                    (*line)[write_pos++] = '"';
                    ++read_pos;
                    continue;
                }
                closed = true;
                break;
            }
            if (!closed) {
                throw CsvDecodeError(column, "unterminated quoted field");
            }
            if (read_pos < line->size() && (*line)[read_pos] != ',') {
                throw CsvDecodeError(column, "unexpected character after quoted field");
            }
        } else {
            while (read_pos < line->size() && (*line)[read_pos] != ',') {
                char ch = (*line)[read_pos++];
                if (ch == '"') {
                    throw CsvDecodeError(column, "quote in unquoted field");
                }
                (*line)[write_pos++] = ch;
            }
        }

        fields->emplace_back(line->data() + field_start, write_pos - field_start);
        if (read_pos == line->size()) {
            return;
        }
        ++read_pos;
        if (read_pos == line->size()) {
            fields->emplace_back(line->data() + write_pos, 0);
            return;
        }
    }
}

std::string_view trim_numeric_field(std::string_view field) {
    while (!field.empty() && std::isspace(static_cast<unsigned char>(field.front()))) {
        field.remove_prefix(1);
    }
    while (!field.empty() && std::isspace(static_cast<unsigned char>(field.back()))) {
        field.remove_suffix(1);
    }
    return field;
}

int parse_csv_int(std::string_view field) {
    field = trim_numeric_field(field);
    if (field.empty()) {
        throw RMDBError("invalid int value");
    }
    bool negative = false;
    usize pos = 0;
    if (field[pos] == '+' || field[pos] == '-') {
        negative = field[pos] == '-';
        ++pos;
    }
    if (pos == field.size()) {
        throw RMDBError("invalid int value");
    }
    i64 value = 0;
    for (; pos < field.size(); ++pos) {
        unsigned char ch = static_cast<unsigned char>(field[pos]);
        if (!std::isdigit(ch)) {
            throw RMDBError("invalid int value");
        }
        value = value * 10 + static_cast<int>(ch - '0');
        i64 limit = negative ? -(static_cast<i64>(std::numeric_limits<int>::min()))
                             : std::numeric_limits<int>::max();
        if (value > limit) {
            throw RMDBError("int value out of range");
        }
    }
    return static_cast<int>(negative ? -value : value);
}

void write_csv_field_to_record(std::string_view field, const ColMeta &col, char *record,
                               std::string *parse_scratch) {
    char *dst = record + col.offset;
    if (col.type == TYPE_INT) {
        int value = parse_csv_int(field);
        std::memcpy(dst, &value, sizeof(value));
    } else if (col.type == TYPE_FLOAT) {
        field = trim_numeric_field(field);
        parse_scratch->assign(field.data(), field.size());
        char *end = nullptr;
        errno = 0;
        float value = std::strtof(parse_scratch->c_str(), &end);
        if (end == parse_scratch->c_str() || *end != '\0' || errno == ERANGE || !std::isfinite(value)) {
            throw RMDBError("invalid float value");
        }
        std::memcpy(dst, &value, sizeof(value));
    } else if (col.type == TYPE_STRING) {
        if (field.size() > static_cast<size_t>(col.len)) {
            throw RMDBError("string length " + std::to_string(field.size()) +
                            " exceeds limit " + std::to_string(col.len));
        }
        std::memset(dst, 0, static_cast<size_t>(col.len));
        if (!field.empty()) {
            std::memcpy(dst, field.data(), field.size());
        }
    } else {
        throw RMDBError("unsupported column type");
    }
}

void validate_header(std::string *line, const TabMeta &tab, const std::string &file_name) {
    std::vector<std::string_view> fields;
    fields.reserve(tab.cols.size());
    try {
        split_csv_line_views(line, &fields);
    } catch (const CsvDecodeError &error) {
        throw_load_error(tab.name, file_name, 1, error.column(), "", error.what());
    }
    if (fields.size() != tab.cols.size()) {
        throw_load_error(tab.name, file_name, 1, 0, "",
                         "header column count " + std::to_string(fields.size()) +
                             " does not match table column count " + std::to_string(tab.cols.size()));
    }
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i] != tab.cols[i].name) {
            throw_load_error(tab.name, file_name, 1, i + 1, tab.cols[i].name,
                             "header name mismatch");
        }
    }
}

size_t load_rows(RmFileHandle *fh, const TabMeta &tab, const std::string &file_name, std::ifstream *input) {
    std::string line;
    std::vector<std::string_view> fields;
    fields.reserve(tab.cols.size());
    std::string parse_scratch;
    parse_scratch.reserve(64);
    size_t physical_line = 1;
    BufferAccessStrategy cold_write_strategy(BufferAccessClass::ColdWrite);

    return fh->bulk_insert_records([&](char *record) -> bool {
        while (std::getline(*input, line)) {
            ++physical_line;
            if (line.empty() || line == "\r") {
                continue;
            }
            try {
                split_csv_line_views(&line, &fields);
            } catch (const CsvDecodeError &error) {
                std::string column_name = error.column() <= tab.cols.size() ? tab.cols[error.column() - 1].name : "";
                throw_load_error(tab.name, file_name, physical_line, error.column(), column_name, error.what());
            }
            if (fields.size() != tab.cols.size()) {
                throw_load_error(tab.name, file_name, physical_line, 0, "",
                                 "column count " + std::to_string(fields.size()) +
                                     " does not match table column count " + std::to_string(tab.cols.size()));
            }
            for (size_t i = 0; i < fields.size(); ++i) {
                try {
                    write_csv_field_to_record(fields[i], tab.cols[i], record, &parse_scratch);
                } catch (const RMDBError &error) {
                    throw_load_error(tab.name, file_name, physical_line, i + 1, tab.cols[i].name,
                                     error_detail(error));
                }
            }
            return true;
        }
        if (input->bad()) {
            throw_load_error(tab.name, file_name, physical_line + 1, 0, "", "I/O error while reading csv");
        }
        return false;
    }, &cold_write_strategy);
}

using IndexEntries = std::vector<std::pair<std::string, Rid>>;

void collect_index_entries(RmFileHandle *fh,
                           const std::vector<IndexMeta> &indexes,
                           std::vector<IndexEntries> *all_entries) {
    if (fh == nullptr || all_entries == nullptr || indexes.empty()) {
        return;
    }
    auto header = fh->get_file_hdr();
    size_t estimated_rows = static_cast<size_t>(std::max(
        0, header.num_pages - RM_FIRST_RECORD_PAGE)) *
        static_cast<size_t>(header.num_records_per_page);
    all_entries->clear();
    all_entries->resize(indexes.size());
    std::vector<std::string> keys;
    keys.reserve(indexes.size());
    for (size_t i = 0; i < indexes.size(); ++i) {
        (*all_entries)[i].reserve(estimated_rows);
        keys.emplace_back(indexes[i].col_tot_len, '\0');
    }
    for (RmScan scan(fh, BufferAccessClass::IndexBuild); !scan.is_end(); scan.next()) {
        scan.with_current_slot([&](const char *slot) {
            for (size_t i = 0; i < indexes.size(); ++i) {
                rmdb::build_index_key_into(
                    indexes[i], slot, scan.rid(), &keys[i]);
                (*all_entries)[i].emplace_back(keys[i], scan.rid());
            }
            return true;
        });
    }
}

void reset_indexes_to_empty(SmManager *sm_manager, const std::string &tab_name,
                            const std::vector<IndexMeta> &indexes) {
    IxManager *ix_manager = sm_manager->get_ix_manager();
    for (const auto &index : indexes) {
        std::string index_name = ix_manager->get_index_name(tab_name, index);
        auto handle = sm_manager->ihs_.find(index_name);
        if (handle != sm_manager->ihs_.end()) {
            ix_manager->close_index(handle->second.get());
            sm_manager->ihs_.erase(handle);
        }
    }
    for (const auto &index : indexes) {
        if (ix_manager->exists(tab_name, index)) {
            ix_manager->destroy_index(tab_name, index);
        }
        ix_manager->create_index(tab_name, index);
        auto handle = ix_manager->open_index(tab_name, index);
        sm_manager->ihs_.emplace(ix_manager->get_index_name(tab_name, index), std::move(handle));
    }
}

void build_existing_indexes(SmManager *sm_manager, RmFileHandle *fh, const TabMeta &tab,
                            bool *indexes_mutated) {
    if (tab.indexes.empty()) {
        return;
    }
    std::vector<IndexEntries> all_entries;
    collect_index_entries(fh, tab.indexes, &all_entries);
    for (size_t i = 0; i < tab.indexes.size(); ++i) {
        const auto &index = tab.indexes[i];
        std::string index_name = sm_manager->get_ix_manager()->get_index_name(tab.name, index);
        sm_manager->ihs_.at(index_name)->prepare_bulk_load(all_entries[i], index.unique);
    }
    for (size_t i = 0; i < tab.indexes.size(); ++i) {
        const auto &index = tab.indexes[i];
        std::string index_name = sm_manager->get_ix_manager()->get_index_name(tab.name, index);
        auto *handle = sm_manager->ihs_.at(index_name).get();
        *indexes_mutated = true;
        handle->bulk_load(all_entries[i], index.unique, true);
        handle->flush();
    }
}

}  // namespace

size_t load_csv_into_table(SmManager *sm_manager, const std::string &tab_name, const std::string &file_name) {
    if (!sm_manager->db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    RmFileHandle *fh = sm_manager->fhs_.at(tab_name).get();
    RmFileHdr file_hdr = fh->get_file_hdr();
    if (file_hdr.num_pages != RM_FIRST_RECORD_PAGE || file_hdr.first_free_page_no != RM_NO_PAGE) {
        throw RMDBError("load requires an empty table: " + tab_name);
    }

    std::string normalized_file_name = normalize_load_file_name(file_name);
    std::ifstream input(normalized_file_name);
    if (!input.is_open()) {
        throw RMDBError("failed to open csv file: " + normalized_file_name);
    }

    std::string header;
    if (!std::getline(input, header)) {
        if (input.bad()) {
            throw_load_error(tab_name, normalized_file_name, 1, 0, "", "I/O error while reading csv header");
        }
        throw_load_error(tab_name, normalized_file_name, 1, 0, "", "missing csv header");
    }
    validate_header(&header, tab, normalized_file_name);
    size_t inserted = load_rows(fh, tab, normalized_file_name, &input);
    bool indexes_mutated = false;
    try {
        fh->flush();
        build_existing_indexes(sm_manager, fh, tab, &indexes_mutated);
    } catch (...) {
        std::exception_ptr load_error = std::current_exception();
        try {
            if (indexes_mutated) {
                reset_indexes_to_empty(sm_manager, tab_name, tab.indexes);
            }
            fh->reset_empty_bulk_load();
        } catch (...) {
            throw InternalError("failed to rollback indexed bulk load");
        }
        std::rethrow_exception(load_error);
    }
    return inserted;
}

}  // namespace rmdb
