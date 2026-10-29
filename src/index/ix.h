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

#include "ix_scan.h"
#include "ix_manager.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "system/sm.h"

namespace rmdb {

struct IndexBinding {
    const IndexMeta *meta;
    IxIndexHandle *ih;
};

template <typename SmManagerT>
inline IxIndexHandle *resolve_index_handle(SmManagerT *sm_manager, const std::string &tab_name,
                                           const IndexMeta &index) {
    return sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index)).get();
}

template <typename SmManagerT>
inline std::vector<IndexBinding> bind_table_indexes(SmManagerT *sm_manager, const std::string &tab_name,
                                                    const TabMeta &tab) {
    std::vector<IndexBinding> bindings;
    bindings.reserve(tab.indexes.size());
    for (const auto &index : tab.indexes) {
        bindings.push_back(IndexBinding{&index, resolve_index_handle(sm_manager, tab_name, index)});
    }
    return bindings;
}

inline void build_index_logical_key_into(const IndexMeta &index, const char *record_data, char *key) {
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        memcpy(key + offset, record_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
}

inline void append_index_rid_suffix(const IndexMeta &index, const Rid &rid, char *key) {
    if (index.unique) {
        return;
    }
    int offset = index.logical_col_tot_len();
    memcpy(key + offset, &rid.page_no, sizeof(int));
    offset += sizeof(int);
    memcpy(key + offset, &rid.slot_no, sizeof(int));
}

inline void build_index_key_into(const IndexMeta &index, const char *record_data, char *key) {
    build_index_logical_key_into(index, record_data, key);
    if (!index.unique) {
        memset(key + index.logical_col_tot_len(), 0, sizeof(Rid));
    }
}

inline void build_index_key_into(const IndexMeta &index, const char *record_data, const Rid &rid, char *key) {
    build_index_logical_key_into(index, record_data, key);
    append_index_rid_suffix(index, rid, key);
}

inline char *build_index_key_into(const IndexMeta &index, const char *record_data, std::string *key) {
    if (key->size() != static_cast<size_t>(index.col_tot_len)) {
        key->resize(index.col_tot_len);
    }
    build_index_key_into(index, record_data, key->data());
    return key->data();
}

inline char *build_index_key_into(const IndexMeta &index, const char *record_data, const Rid &rid, std::string *key) {
    if (key->size() != static_cast<size_t>(index.col_tot_len)) {
        key->resize(index.col_tot_len);
    }
    build_index_key_into(index, record_data, rid, key->data());
    return key->data();
}

inline std::unique_ptr<char[]> build_index_key(const IndexMeta &index, const char *record_data) {
    auto key = std::make_unique<char[]>(index.col_tot_len);
    build_index_key_into(index, record_data, key.get());
    return key;
}

inline std::unique_ptr<char[]> build_index_key(const IndexMeta &index, const char *record_data, const Rid &rid) {
    auto key = std::make_unique<char[]>(index.col_tot_len);
    build_index_key_into(index, record_data, rid, key.get());
    return key;
}

struct IndexKeyColRef {
    const ColMeta *col = nullptr;
    int offset = 0;

    explicit operator bool() const { return col != nullptr; }
};

inline IndexKeyColRef find_index_logical_col(const IndexMeta &index, const std::string &col_name) {
    int offset = 0;
    for (const auto &col : index.cols) {
        if (col.name == col_name) {
            return {&col, offset};
        }
        offset += col.len;
    }
    return {};
}

inline bool index_covers_col(const IndexMeta &index, const std::string &col_name) {
    return static_cast<bool>(find_index_logical_col(index, col_name));
}

inline bool index_covers_cols(const IndexMeta &index, const std::vector<TabCol> &cols,
                              const std::string &visible_name) {
    for (const auto &col : cols) {
        if (col.col_name.empty() || (!col.tab_name.empty() && col.tab_name != visible_name)) {
            continue;
        }
        if (!index_covers_col(index, col.col_name)) {
            return false;
        }
    }
    return true;
}

inline bool index_covers_conditions(const IndexMeta &index, const std::vector<Condition> &conds,
                                    const std::string &visible_name) {
    for (const auto &cond : conds) {
        if (cond.lhs_col.tab_name == visible_name && !index_covers_col(index, cond.lhs_col.col_name)) {
            return false;
        }
        if (!cond.is_rhs_val && cond.rhs_col.tab_name == visible_name &&
            !index_covers_col(index, cond.rhs_col.col_name)) {
            return false;
        }
    }
    return true;
}

inline bool index_covers_required_and_conditions(const IndexMeta &index, const std::vector<TabCol> &required_cols,
                                                 const std::vector<Condition> &conds,
                                                 const std::string &visible_name) {
    return index_covers_cols(index, required_cols, visible_name) &&
           index_covers_conditions(index, conds, visible_name);
}

inline int compare_index_raw_value(const char *lhs, const char *rhs, ColType type, int len) {
    if (type == TYPE_INT) {
        int l = load_unaligned<int>(lhs);
        int r = load_unaligned<int>(rhs);
        return (l > r) - (l < r);
    }
    if (type == TYPE_FLOAT) {
        float l = load_unaligned<float>(lhs);
        float r = load_unaligned<float>(rhs);
        return (l > r) - (l < r);
    }
    return std::memcmp(lhs, rhs, len);
}

inline void fill_index_col_min(char *dst, ColType type, int len) {
    if (type == TYPE_INT) {
        int value = std::numeric_limits<int>::min();
        std::memcpy(dst, &value, sizeof(int));
    } else if (type == TYPE_FLOAT) {
        float value = -std::numeric_limits<float>::max();
        std::memcpy(dst, &value, sizeof(float));
    } else {
        std::memset(dst, 0, len);
    }
}

inline void fill_index_col_max(char *dst, ColType type, int len) {
    if (type == TYPE_INT) {
        int value = std::numeric_limits<int>::max();
        std::memcpy(dst, &value, sizeof(int));
    } else if (type == TYPE_FLOAT) {
        float value = std::numeric_limits<float>::max();
        std::memcpy(dst, &value, sizeof(float));
    } else {
        std::memset(dst, 0xff, len);
    }
}

inline void fill_index_key_min_suffix(const IndexMeta &index, int start_offset, char *key) {
    int offset = 0;
    for (const auto &col : index.cols) {
        if (offset + col.len > start_offset) {
            int fill_offset = std::max(offset, start_offset);
            if (fill_offset == offset) {
                fill_index_col_min(key + offset, col.type, col.len);
            } else {
                std::memset(key + fill_offset, 0, offset + col.len - fill_offset);
            }
        }
        offset += col.len;
    }
    if (!index.unique && offset < index.col_tot_len) {
        int min_value = std::numeric_limits<int>::min();
        std::memcpy(key + offset, &min_value, sizeof(int));
        offset += sizeof(int);
        std::memcpy(key + offset, &min_value, sizeof(int));
    }
}

inline void fill_index_key_max_suffix(const IndexMeta &index, int start_offset, char *key) {
    int offset = 0;
    for (const auto &col : index.cols) {
        if (offset + col.len > start_offset) {
            int fill_offset = std::max(offset, start_offset);
            if (fill_offset == offset) {
                fill_index_col_max(key + offset, col.type, col.len);
            } else {
                std::memset(key + fill_offset, 0xff, offset + col.len - fill_offset);
            }
        }
        offset += col.len;
    }
    if (!index.unique && offset < index.col_tot_len) {
        int max_value = std::numeric_limits<int>::max();
        std::memcpy(key + offset, &max_value, sizeof(int));
        offset += sizeof(int);
        std::memcpy(key + offset, &max_value, sizeof(int));
    }
}

enum class IndexBoundLookup {
    LeafBegin,
    LowerBound,
    UpperBound,
    LeafEnd,
};

struct IndexRangeSpec {
    std::string lower_key;
    std::string upper_key;
    IndexBoundLookup lower_lookup = IndexBoundLookup::LeafBegin;
    IndexBoundLookup upper_lookup = IndexBoundLookup::LeafEnd;
    int equality_prefix_cols = 0;
    int equality_prefix_len = 0;
    int scan_prefix_len = 0;
    bool exact_unique_key = false;
    bool all_conditions_consumed = false;
};

inline IndexRangeSpec build_index_range_spec(const IndexMeta &index, const std::vector<Condition> &conds,
                                             const std::string &visible_name) {
    IndexRangeSpec spec;
    spec.lower_key.assign(static_cast<size_t>(index.col_tot_len), '\0');
    spec.upper_key.assign(static_cast<size_t>(index.col_tot_len), '\0');
    std::vector<bool> consumed_conds(conds.size(), false);

    int offset = 0;
    for (const auto &index_col : index.cols) {
        auto cond_it = std::find_if(conds.begin(), conds.end(), [&](const Condition &cond) {
            return cond.is_rhs_val && cond.op == OP_EQ && cond.rhs_val.raw != nullptr &&
                   cond.lhs_col.tab_name == visible_name && cond.lhs_col.col_name == index_col.name;
        });
        if (cond_it == conds.end()) {
            break;
        }
        consumed_conds[static_cast<size_t>(std::distance(conds.begin(), cond_it))] = true;
        std::memcpy(&spec.lower_key[static_cast<size_t>(offset)], cond_it->rhs_val.raw->data, index_col.len);
        std::memcpy(&spec.upper_key[static_cast<size_t>(offset)], cond_it->rhs_val.raw->data, index_col.len);
        offset += index_col.len;
        spec.equality_prefix_cols++;
        spec.equality_prefix_len = offset;
    }

    struct RangeBound {
        const char *value = nullptr;
        bool strict = false;
        bool set = false;
        size_t cond_idx = 0;
    };
    RangeBound lower;
    RangeBound upper;
    int range_col_offset = spec.equality_prefix_len;
    const ColMeta *range_col = nullptr;
    if (spec.equality_prefix_cols < index.col_num) {
        range_col = &index.cols[static_cast<size_t>(spec.equality_prefix_cols)];
        for (size_t cond_idx = 0; cond_idx < conds.size(); ++cond_idx) {
            const auto &cond = conds[cond_idx];
            if (!cond.is_rhs_val || cond.rhs_val.raw == nullptr || cond.lhs_col.tab_name != visible_name ||
                cond.lhs_col.col_name != range_col->name) {
                continue;
            }
            if (cond.op == OP_GT || cond.op == OP_GE) {
                if (!lower.set ||
                    compare_index_raw_value(cond.rhs_val.raw->data, lower.value, range_col->type, range_col->len) > 0 ||
                    (compare_index_raw_value(cond.rhs_val.raw->data, lower.value, range_col->type, range_col->len) == 0 &&
                     cond.op == OP_GT && !lower.strict)) {
                    lower = {cond.rhs_val.raw->data, cond.op == OP_GT, true, cond_idx};
                }
            } else if (cond.op == OP_LT || cond.op == OP_LE) {
                if (!upper.set ||
                    compare_index_raw_value(cond.rhs_val.raw->data, upper.value, range_col->type, range_col->len) < 0 ||
                    (compare_index_raw_value(cond.rhs_val.raw->data, upper.value, range_col->type, range_col->len) == 0 &&
                     cond.op == OP_LT && !upper.strict)) {
                    upper = {cond.rhs_val.raw->data, cond.op == OP_LT, true, cond_idx};
                }
            }
        }
    }

    if (lower.set && range_col != nullptr) {
        if (!lower.strict) {
            consumed_conds[lower.cond_idx] = true;
        }
        std::memcpy(&spec.lower_key[static_cast<size_t>(range_col_offset)], lower.value, range_col->len);
        int suffix_offset = range_col_offset + range_col->len;
        fill_index_key_min_suffix(index, suffix_offset, spec.lower_key.data());
        spec.lower_lookup = IndexBoundLookup::LowerBound;
    } else if (spec.equality_prefix_len > 0) {
        fill_index_key_min_suffix(index, spec.equality_prefix_len, spec.lower_key.data());
        spec.lower_lookup = IndexBoundLookup::LowerBound;
    }

    if (upper.set && range_col != nullptr) {
        consumed_conds[upper.cond_idx] = true;
        std::memcpy(&spec.upper_key[static_cast<size_t>(range_col_offset)], upper.value, range_col->len);
        int suffix_offset = range_col_offset + range_col->len;
        if (upper.strict) {
            fill_index_key_min_suffix(index, suffix_offset, spec.upper_key.data());
            spec.upper_lookup = IndexBoundLookup::LowerBound;
        } else {
            fill_index_key_max_suffix(index, suffix_offset, spec.upper_key.data());
            spec.upper_lookup = IndexBoundLookup::UpperBound;
        }
    }

    spec.scan_prefix_len = spec.equality_prefix_len;
    spec.exact_unique_key = index.unique && spec.equality_prefix_cols == index.col_num;
    spec.all_conditions_consumed = std::all_of(consumed_conds.begin(), consumed_conds.end(),
                                               [](bool consumed) { return consumed; });
    return spec;
}

// Immutable, catalog-bound part of an index access path. Literal addresses are
// deliberately not cached: cloned template plans own different Value objects,
// so execution binds them through condition_index on every invocation.
class CompiledAccessProgram {
   public:
    struct Predicate {
        size_t condition_index{0};
        int lhs_offset{0};
        int rhs_offset{0};
        int len{0};
        ColType type{TYPE_INT};
        CompOp op{OP_EQ};
        bool rhs_is_literal{false};
    };

    struct RangeColumn {
        int key_offset{0};
        int len{0};
        ColType type{TYPE_INT};
        std::vector<size_t> lower_conditions;
        std::vector<size_t> upper_conditions;
    };

    static std::shared_ptr<const CompiledAccessProgram> BuildIndex(
        const IndexMeta &index, const std::vector<Condition> &conditions,
        const std::string &visible_name, rmdb::u64 schema_epoch) {
        auto program = std::shared_ptr<CompiledAccessProgram>(new CompiledAccessProgram());
        program->schema_epoch_ = schema_epoch;
        program->index_id_ = index.index_id;
        program->logical_key_len_ = index.logical_col_tot_len();
        program->predicates_.reserve(conditions.size());
        for (size_t i = 0; i < conditions.size(); ++i) {
            const auto &condition = conditions[i];
            if (condition.lhs_col.tab_name != visible_name) {
                return nullptr;
            }
            auto lhs = find_index_logical_col(index, condition.lhs_col.col_name);
            if (!lhs) {
                return nullptr;
            }
            Predicate predicate;
            predicate.condition_index = i;
            predicate.lhs_offset = lhs.offset;
            predicate.len = lhs.col->len;
            predicate.type = lhs.col->type;
            predicate.op = condition.op;
            predicate.rhs_is_literal = condition.is_rhs_val;
            if (!condition.is_rhs_val) {
                if (condition.rhs_col.tab_name != visible_name) {
                    return nullptr;
                }
                auto rhs = find_index_logical_col(index, condition.rhs_col.col_name);
                if (!rhs || rhs.col->type != lhs.col->type || rhs.col->len != lhs.col->len) {
                    return nullptr;
                }
                predicate.rhs_offset = rhs.offset;
            }
            program->predicates_.push_back(predicate);
        }
        int key_offset = 0;
        for (const auto &index_col : index.cols) {
            auto it = std::find_if(conditions.begin(), conditions.end(), [&](const Condition &condition) {
                return condition.is_rhs_val && condition.op == OP_EQ &&
                       condition.rhs_val.raw != nullptr &&
                       condition.lhs_col.tab_name == visible_name &&
                       condition.lhs_col.col_name == index_col.name;
            });
            if (it == conditions.end()) {
                break;
            }
            program->equality_conditions_.push_back(
                static_cast<size_t>(std::distance(conditions.begin(), it)));
            ++program->equality_prefix_cols_;
            key_offset += index_col.len;
        }
        program->exact_unique_key_ = index.unique && program->equality_prefix_cols_ == index.col_num;
        if (program->equality_prefix_cols_ < index.col_num) {
            const auto &range_col = index.cols[static_cast<size_t>(program->equality_prefix_cols_)];
            RangeColumn range;
            range.key_offset = key_offset;
            range.len = range_col.len;
            range.type = range_col.type;
            for (size_t i = 0; i < conditions.size(); ++i) {
                const auto &condition = conditions[i];
                if (!condition.is_rhs_val || condition.rhs_val.raw == nullptr ||
                    condition.lhs_col.tab_name != visible_name ||
                    condition.lhs_col.col_name != range_col.name) {
                    continue;
                }
                if (condition.op == OP_GT || condition.op == OP_GE) {
                    range.lower_conditions.push_back(i);
                } else if (condition.op == OP_LT || condition.op == OP_LE) {
                    range.upper_conditions.push_back(i);
                }
            }
            program->range_column_ = std::move(range);
        }
        return program;
    }

    bool exact_unique_key() const { return exact_unique_key_; }
    int equality_prefix_cols() const { return equality_prefix_cols_; }

    bool BindCoveringOutput(const IndexMeta &index, const std::vector<ColMeta> &columns,
                            std::vector<int> *offsets) const {
        if (offsets == nullptr || index.index_id != index_id_ ||
            index.logical_col_tot_len() != logical_key_len_) {
            return false;
        }
        offsets->clear();
        offsets->reserve(columns.size());
        for (const auto &column : columns) {
            auto key_column = find_index_logical_col(index, column.name);
            if (!key_column) {
                offsets->clear();
                return false;
            }
            offsets->push_back(key_column.offset);
        }
        return true;
    }

    bool MaterializeRange(const IndexMeta &index, const std::vector<Condition> &conditions,
                          IndexRangeSpec *spec) const {
        if (spec == nullptr || conditions.size() != predicates_.size() ||
            index.index_id != index_id_ || index.logical_col_tot_len() != logical_key_len_) {
            return false;
        }
        *spec = {};
        spec->lower_key.assign(static_cast<size_t>(index.col_tot_len), '\0');
        spec->upper_key.assign(static_cast<size_t>(index.col_tot_len), '\0');
        std::vector<bool> consumed(conditions.size(), false);
        int offset = 0;
        for (size_t condition_index : equality_conditions_) {
            if (condition_index >= conditions.size() || conditions[condition_index].rhs_val.raw == nullptr) {
                return false;
            }
            const auto &index_col = index.cols[static_cast<size_t>(spec->equality_prefix_cols)];
            std::memcpy(spec->lower_key.data() + offset, conditions[condition_index].rhs_val.raw->data,
                        index_col.len);
            std::memcpy(spec->upper_key.data() + offset, conditions[condition_index].rhs_val.raw->data,
                        index_col.len);
            consumed[condition_index] = true;
            offset += index_col.len;
            ++spec->equality_prefix_cols;
            spec->equality_prefix_len = offset;
        }

        auto choose_bound = [&](const std::vector<size_t> &candidates, bool lower) -> std::optional<size_t> {
            std::optional<size_t> best;
            for (size_t candidate : candidates) {
                if (candidate >= conditions.size() || conditions[candidate].rhs_val.raw == nullptr) return {};
                if (!best.has_value()) {
                    best = candidate;
                    continue;
                }
                int cmp = compare_index_raw_value(conditions[candidate].rhs_val.raw->data,
                                                  conditions[*best].rhs_val.raw->data,
                                                  range_column_->type, range_column_->len);
                bool candidate_strict = conditions[candidate].op == (lower ? OP_GT : OP_LT);
                bool best_strict = conditions[*best].op == (lower ? OP_GT : OP_LT);
                if ((lower && cmp > 0) || (!lower && cmp < 0) ||
                    (cmp == 0 && candidate_strict && !best_strict)) {
                    best = candidate;
                }
            }
            return best;
        };

        std::optional<size_t> lower;
        std::optional<size_t> upper;
        if (range_column_.has_value()) {
            lower = choose_bound(range_column_->lower_conditions, true);
            upper = choose_bound(range_column_->upper_conditions, false);
        }
        if (lower.has_value()) {
            const auto &condition = conditions[*lower];
            bool strict = condition.op == OP_GT;
            if (!strict) consumed[*lower] = true;
            std::memcpy(spec->lower_key.data() + range_column_->key_offset,
                        condition.rhs_val.raw->data, range_column_->len);
            fill_index_key_min_suffix(index, range_column_->key_offset + range_column_->len,
                                      spec->lower_key.data());
            spec->lower_lookup = IndexBoundLookup::LowerBound;
        } else if (spec->equality_prefix_len > 0) {
            fill_index_key_min_suffix(index, spec->equality_prefix_len, spec->lower_key.data());
            spec->lower_lookup = IndexBoundLookup::LowerBound;
        }
        if (upper.has_value()) {
            const auto &condition = conditions[*upper];
            bool strict = condition.op == OP_LT;
            consumed[*upper] = true;
            std::memcpy(spec->upper_key.data() + range_column_->key_offset,
                        condition.rhs_val.raw->data, range_column_->len);
            int suffix = range_column_->key_offset + range_column_->len;
            if (strict) {
                fill_index_key_min_suffix(index, suffix, spec->upper_key.data());
                spec->upper_lookup = IndexBoundLookup::LowerBound;
            } else {
                fill_index_key_max_suffix(index, suffix, spec->upper_key.data());
                spec->upper_lookup = IndexBoundLookup::UpperBound;
            }
        }
        spec->scan_prefix_len = spec->equality_prefix_len;
        spec->exact_unique_key = exact_unique_key_;
        spec->all_conditions_consumed = std::all_of(consumed.begin(), consumed.end(), [](bool value) {
            return value;
        });
        return true;
    }

    bool Matches(const IndexMeta &index, size_t condition_count, rmdb::u64 schema_epoch) const {
        return schema_epoch_ == schema_epoch && index_id_ == index.index_id &&
               logical_key_len_ == index.logical_col_tot_len() && predicates_.size() == condition_count;
    }

    bool EvaluateIndexKey(const char *key, const std::vector<Condition> &conditions) const {
        if (key == nullptr || conditions.size() != predicates_.size()) {
            return false;
        }
        for (const auto &predicate : predicates_) {
            const auto &condition = conditions[predicate.condition_index];
            const char *rhs = nullptr;
            if (predicate.rhs_is_literal) {
                if (!condition.is_rhs_val || condition.rhs_val.raw == nullptr) {
                    return false;
                }
                rhs = condition.rhs_val.raw->data;
            } else {
                rhs = key + predicate.rhs_offset;
            }
            int cmp = compare_index_raw_value(key + predicate.lhs_offset, rhs,
                                              predicate.type, predicate.len);
            if (!Compare(cmp, predicate.op)) {
                return false;
            }
        }
        return true;
    }

   private:
    static bool Compare(int cmp, CompOp op) {
        switch (op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
        }
        return false;
    }

    rmdb::u64 schema_epoch_{0};
    rmdb::u64 index_id_{0};
    int logical_key_len_{0};
    int equality_prefix_cols_{0};
    bool exact_unique_key_{false};
    std::vector<size_t> equality_conditions_;
    std::optional<RangeColumn> range_column_;
    std::vector<Predicate> predicates_;
};

class CompiledMutationProgram {
   public:
    struct SetSlot {
        int lhs_offset{0};
        int len{0};
        ColType lhs_type{TYPE_INT};
        SetOp op{SetOp::ASSIGN};
        bool reversed{false};
        bool rhs_is_col{false};
        int rhs_offset{0};
        ColType rhs_type{TYPE_INT};
    };

    static std::shared_ptr<const CompiledMutationProgram> BuildUpdate(
        const TabMeta &table, const std::vector<SetClause> &clauses, rmdb::u64 schema_epoch) {
        auto program = std::shared_ptr<CompiledMutationProgram>(new CompiledMutationProgram());
        program->schema_epoch_ = schema_epoch;
        program->slots_.reserve(clauses.size());
        program->index_touched_.assign(table.indexes.size(), false);
        for (const auto &clause : clauses) {
            auto lhs = table.get_col(clause.lhs.col_name);
            SetSlot slot;
            slot.lhs_offset = lhs->offset;
            slot.len = lhs->len;
            slot.lhs_type = lhs->type;
            slot.op = clause.op;
            slot.reversed = clause.reversed_operands;
            slot.rhs_is_col = clause.rhs_is_col;
            if (clause.rhs_is_col) {
                auto rhs = table.get_col(clause.rhs_col.col_name);
                slot.rhs_offset = rhs->offset;
                slot.rhs_type = rhs->type;
            }
            program->slots_.push_back(slot);
            for (size_t i = 0; i < table.indexes.size(); ++i) {
                if (index_covers_col(table.indexes[i], clause.lhs.col_name)) {
                    program->index_touched_[i] = true;
                    program->touches_index_ = true;
                    program->key_conflict_required_ =
                        program->key_conflict_required_ || table.indexes[i].unique;
                }
            }
            program->key_conflict_required_ = program->key_conflict_required_ || clause.lhs.col_name == "id";
        }
        return program;
    }

    bool Matches(size_t clause_count, size_t index_count, rmdb::u64 schema_epoch) const {
        return schema_epoch_ == schema_epoch && slots_.size() == clause_count &&
               index_touched_.size() == index_count;
    }
    const std::vector<SetSlot> &slots() const { return slots_; }
    const std::vector<bool> &index_touched() const { return index_touched_; }
    bool touches_index() const { return touches_index_; }
    bool key_conflict_required() const { return key_conflict_required_; }

   private:
    rmdb::u64 schema_epoch_{0};
    std::vector<SetSlot> slots_;
    std::vector<bool> index_touched_;
    bool touches_index_{false};
    bool key_conflict_required_{false};
};

}  // namespace rmdb
