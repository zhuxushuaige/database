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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "execution_defs.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"
#include "optimizer/planner.h"

class Planner;

class QlManager {
   private:
    SmManager *sm_manager_;
    TransactionManager *txn_mgr_;
    Planner *planner_;

   public:
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr, Planner *planner)
        : sm_manager_(sm_manager),  txn_mgr_(txn_mgr), planner_(planner) {}

    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                        Context *context);

    void run_dml(std::unique_ptr<AbstractExecutor> exec);
};

#include <set>

#include "executor_abstract.h"

class UnionExecutor : public AbstractExecutor {
   private:
    std::vector<std::unique_ptr<AbstractExecutor>> children_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<RmRecord> tuples_;
    size_t cursor_ = 0;

    void copy_cell(RmRecord *dst, const ColMeta &dst_col, const TupleView &src, const ColMeta &src_col,
                   size_t src_idx) const {
        char *dst_data = dst->data + dst_col.offset;
        const char *src_data = src.cell_at(src_col, src_idx);
        if (dst_col.type == src_col.type) {
            memset(dst_data, 0, dst_col.len);
            memcpy(dst_data, src_data, std::min(dst_col.len, src_col.len));
            return;
        }
        if (dst_col.type == TYPE_FLOAT && src_col.type == TYPE_INT) {
            rmdb::store_unaligned<float>(dst_data,
                                         static_cast<float>(rmdb::load_unaligned<int>(src_data)));
            return;
        }
        throw IncompatibleTypeError(coltype2str(dst_col.type), coltype2str(src_col.type));
    }

   public:
    UnionExecutor(std::vector<std::unique_ptr<AbstractExecutor>> children, std::vector<ColMeta> cols) {
        children_ = std::move(children);
        cols_ = std::move(cols);
        len_ = cols_.empty() ? 0 : cols_.back().offset + cols_.back().len;
    }

    void beginTuple() override {
        tuples_.clear();
        std::set<std::string> seen;
        for (auto &child : children_) {
            child->beginTuple();
            const auto &src_cols = child->cols();
            if (src_cols.size() != cols_.size()) {
                throw RMDBError("union column count mismatch");
            }
            for (; !child->is_end(); child->nextTuple()) {
                auto src = child->ReadTupleView();
                if (!src) {
                    break;
                }
                RmRecord out(static_cast<int>(len_));
                for (size_t i = 0; i < cols_.size(); ++i) {
                    copy_cell(&out, cols_[i], *src, src_cols[i], i);
                }
                std::string key(out.data, len_);
                if (seen.insert(key).second) {
                    tuples_.push_back(std::move(out));
                }
            }
        }
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
    std::string getType() override { return "UnionExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }
};

#include <cstddef>
#include <string>

class SmManager;

namespace rmdb {

size_t load_csv_into_table(SmManager *sm_manager, const std::string &tab_name, const std::string &file_name);

}  // namespace rmdb
