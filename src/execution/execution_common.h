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

#include <cstring>
#include <optional>
#include <vector>

#include "transaction/transaction.h"
#include "common/common.h"

struct ExecutionJoinKeyPart {
    size_t left_idx = 0;
    ColMeta left_col;
    size_t right_idx = 0;
    ColMeta right_col;
};

inline auto ExecutorColumnMatches(const ColMeta &column, const TabCol &target) -> bool {
    return column.tab_name == target.tab_name && column.name == target.col_name;
}

inline const ColMeta *FindExecutorColumn(const std::vector<ColMeta> &cols, const TabCol &target,
                                         size_t *index = nullptr) {
    for (size_t i = 0; i < cols.size(); ++i) {
        if (ExecutorColumnMatches(cols[i], target)) {
            if (index != nullptr) {
                *index = i;
            }
            return &cols[i];
        }
    }
    return nullptr;
}

inline auto BuildEquiJoinKeyPart(const std::vector<ColMeta> &left_cols, const std::vector<ColMeta> &right_cols,
                                 const Condition &condition) -> std::optional<ExecutionJoinKeyPart> {
    if (condition.is_rhs_val || condition.op != OP_EQ) {
        return std::nullopt;
    }

    size_t left_idx = 0;
    size_t right_idx = 0;
    const ColMeta *left_col = FindExecutorColumn(left_cols, condition.lhs_col, &left_idx);
    const ColMeta *right_col = FindExecutorColumn(right_cols, condition.rhs_col, &right_idx);
    if (left_col == nullptr || right_col == nullptr) {
        left_col = FindExecutorColumn(left_cols, condition.rhs_col, &left_idx);
        right_col = FindExecutorColumn(right_cols, condition.lhs_col, &right_idx);
    }
    if (left_col == nullptr || right_col == nullptr || left_col->type != right_col->type ||
        left_col->len != right_col->len) {
        return std::nullopt;
    }
    return ExecutionJoinKeyPart{left_idx, *left_col, right_idx, *right_col};
}

inline std::vector<ExecutionJoinKeyPart> BuildEquiJoinKeyParts(const std::vector<ColMeta> &left_cols,
                                                               const std::vector<ColMeta> &right_cols,
                                                               const std::vector<Condition> &conditions) {
    std::vector<ExecutionJoinKeyPart> parts;
    parts.reserve(conditions.size());
    for (const auto &cond : conditions) {
        auto part = BuildEquiJoinKeyPart(left_cols, right_cols, cond);
        if (part.has_value()) {
            parts.push_back(std::move(*part));
        }
    }
    return parts;
}

inline void ApplyUndoLogToTuple(const TabMeta *schema, const UndoLog &undo_log, RmRecord *record, TupleMeta *meta) {
    if (undo_log.tuple_image_ != nullptr) {
        *record = *undo_log.tuple_image_;
    } else if (schema != nullptr && !undo_log.modified_fields_.empty()) {
        size_t value_idx = 0;
        for (size_t col_idx = 0; col_idx < undo_log.modified_fields_.size() && col_idx < schema->cols.size();
             ++col_idx) {
            if (!undo_log.modified_fields_[col_idx]) {
                continue;
            }
            if (value_idx >= undo_log.tuple_.size()) {
                break;
            }
            const auto &col = schema->cols[col_idx];
            const auto &val = undo_log.tuple_[value_idx++];
            if (val.raw != nullptr) {
                std::memcpy(record->data + col.offset, val.raw->data, col.len);
            }
        }
    }
    meta->ts_ = undo_log.ts_;
    meta->is_deleted_ = undo_log.is_deleted_;
}

inline auto ReconstructTuple(const TabMeta *schema, const RmRecord &base_tuple, const TupleMeta &base_meta,
                             const std::vector<UndoLog> &undo_logs) -> std::optional<RmRecord> {
    TupleMeta meta = base_meta;
    RmRecord record = base_tuple;
    for (const auto &undo_log : undo_logs) {
        ApplyUndoLogToTuple(schema, undo_log, &record, &meta);
    }
    if (meta.is_deleted_) {
        return std::nullopt;
    }
    return record;
}

inline auto IsMvccIsolationLevel(IsolationLevel level) -> bool {
    return level == IsolationLevel::SNAPSHOT_ISOLATION || level == IsolationLevel::SERIALIZABLE;
}

inline auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction *txn) -> bool {
    if (txn == nullptr || tuple_ts == INVALID_TS) {
        return false;
    }
    if (tuple_ts >= TXN_START_ID) {
        txn_id_t owner = tuple_ts - TXN_START_ID;
        return owner != txn->get_transaction_id();
    }
    return tuple_ts > txn->get_read_ts();
}

inline bool UseMvccReadVisibility(Transaction *txn) {
    return txn != nullptr && IsMvccIsolationLevel(txn->get_isolation_level());
}
