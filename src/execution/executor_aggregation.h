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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

#include <map>
#include <array>
#include <limits>

#if 0  // Legacy aggregate executor from the template; target uses AggregateExecutor below.
struct HavingCond {
    AggType agg_type;
    bool is_star;
    TabCol col;
    CompOp op;
    Value rhs_val;
};

struct GroupResult {
    std::vector<char> group_key;
    std::vector<float> agg_values;    // computed aggregate values (float for all, cast on output)
    int count;                         // tuple count for this group
};

class AggregationExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<AggOp> agg_ops_;
    std::vector<TabCol> group_by_cols_;
    std::vector<HavingCond> having_conds_;
    std::vector<ColMeta> output_cols_;
    size_t output_len_;

    // Pre-computed results
    std::vector<GroupResult> results_;
    size_t current_idx_;

    // Group-by column metadata (from child executor)
    std::vector<ColMeta> gb_col_metas_;
    int gb_key_len_;

    // Aggregate column metadata (input columns)
    std::vector<ColMeta> agg_input_metas_;

    // For COUNT-specific tracking
    struct AggInfo {
        AggType type;
        bool is_star;
        ColType input_type;
        int input_len;
    };
    std::vector<AggInfo> agg_infos_;

    // Compare group keys
    static bool group_key_less(const std::vector<char> &a, const std::vector<char> &b,
                                const std::vector<ColMeta> &metas) {
        int offset = 0;
        for (auto &m : metas) {
            int cmp = compare_raw_data(a.data() + offset, b.data() + offset, m.type, m.len);
            if (cmp != 0) return cmp < 0;
            offset += m.len;
        }
        return false;
    }

    float extract_val(const char *rec_data, const ColMeta &cm) {
        const char *ptr = rec_data + cm.offset;
        if (cm.type == TYPE_INT) return static_cast<float>(*(int *)ptr);
        if (cm.type == TYPE_FLOAT) return *(float *)ptr;
        return 0.0f;
    }

    bool eval_having(const GroupResult &grp) {
        for (auto &hc : having_conds_) {
            // Find matching aggregate value
            float lhs_val = 0.0f;
            bool found = false;
            for (size_t i = 0; i < agg_ops_.size(); ++i) {
                if (agg_ops_[i].agg_type == hc.agg_type &&
                    agg_ops_[i].is_star == hc.is_star &&
                    (hc.is_star || (agg_ops_[i].col.tab_name == hc.col.tab_name &&
                                    agg_ops_[i].col.col_name == hc.col.col_name))) {
                    lhs_val = grp.agg_values[i];
                    found = true;
                    break;
                }
            }
            if (!found) continue;

            float rhs_val;
            if (hc.rhs_val.type == TYPE_INT) rhs_val = static_cast<float>(hc.rhs_val.int_val);
            else rhs_val = hc.rhs_val.float_val;

            // For COUNT, lhs_val is already the count
            // For AVG, need to divide by count
            if (hc.agg_type == AGG_AVG) {
                lhs_val = (grp.count > 0) ? lhs_val / grp.count : 0;
            }

            int cmp = (lhs_val > rhs_val) - (lhs_val < rhs_val);
            if (!eval_compare(cmp, hc.op)) return false;
        }
        return true;
    }

    void build_output_schema() {
        output_cols_.clear();
        int offset = 0;

        // Group-by columns first
        for (auto &gbc : gb_col_metas_) {
            ColMeta cm = gbc;
            cm.offset = offset;
            output_cols_.push_back(cm);
            offset += cm.len;
        }

        // Aggregate result columns
        for (size_t i = 0; i < agg_ops_.size(); ++i) {
            ColMeta cm;
            cm.tab_name = "";
            cm.name = agg_ops_[i].alias;
            cm.offset = offset;

            switch (agg_ops_[i].agg_type) {
                case AGG_COUNT:
                    cm.type = TYPE_INT;
                    cm.len = sizeof(int);
                    break;
                case AGG_MAX:
                case AGG_MIN:
                    cm.type = agg_infos_[i].input_type;
                    cm.len = agg_infos_[i].input_len;
                    break;
                case AGG_SUM:
                case AGG_AVG:
                    cm.type = TYPE_FLOAT;
                    cm.len = sizeof(float);
                    break;
            }
            output_cols_.push_back(cm);
            offset += cm.len;
        }
        output_len_ = offset;
    }

   public:
    AggregationExecutor(std::unique_ptr<AbstractExecutor> prev,
                        std::vector<AggOp> agg_ops,
                        std::vector<TabCol> group_by_cols,
                        std::vector<HavingCond> having_conds)
        : prev_(std::move(prev)), agg_ops_(std::move(agg_ops)),
          group_by_cols_(std::move(group_by_cols)),
          having_conds_(std::move(having_conds)),
          current_idx_(0), gb_key_len_(0) {}

    void beginTuple() override {
        auto &prev_cols = prev_->cols();
        results_.clear();
        current_idx_ = 0;

        // Build group-by column metadata
        gb_col_metas_.clear();
        gb_key_len_ = 0;
        for (auto &gb_col : group_by_cols_) {
            auto pos = get_col(prev_cols, gb_col);
            ColMeta cm = *pos;
            cm.offset = gb_key_len_;
            gb_col_metas_.push_back(cm);
            gb_key_len_ += pos->len;
        }

        // Build aggregate input column metadata
        agg_infos_.clear();
        for (auto &agg : agg_ops_) {
            AggInfo info;
            info.type = agg.agg_type;
            info.is_star = agg.is_star;
            if (!agg.is_star) {
                auto pos = get_col(prev_cols, agg.col);
                info.input_type = pos->type;
                info.input_len = pos->len;
            } else {
                info.input_type = TYPE_INT;
                info.input_len = sizeof(int);
            }
            agg_infos_.push_back(info);
        }

        // Collect all tuples and group them
        bool has_group = !group_by_cols_.empty();

        // Map from group key to (agg_values, count)
        // agg_values tracks SUM for SUM/AVG, MAX for MAX, MIN for MIN, count for COUNT
        std::map<std::vector<char>, std::pair<std::vector<float>, int>> groups_map;
        std::vector<std::vector<char>> ordered_keys;

        // Single group (no GROUP BY)
        std::vector<float> single_vals(agg_ops_.size(), 0.0f);
        for (size_t i = 0; i < agg_ops_.size(); ++i) {
            if (agg_ops_[i].agg_type == AGG_MAX) single_vals[i] = -std::numeric_limits<float>::max();
            if (agg_ops_[i].agg_type == AGG_MIN) single_vals[i] = std::numeric_limits<float>::max();
        }
        int single_count = 0;

        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            const char *data = rec->data;

            if (has_group) {
                // Build group key
                std::vector<char> key(gb_key_len_);
                int off = 0;
                for (auto &gbc : gb_col_metas_) {
                    auto pos = get_col(prev_cols, TabCol{gbc.tab_name, gbc.name});
                    memcpy(key.data() + off, data + pos->offset, pos->len);
                    off += pos->len;
                }

                auto it = groups_map.find(key);
                if (it == groups_map.end()) {
                    std::vector<float> vals(agg_ops_.size(), 0.0f);
                    for (size_t i = 0; i < agg_ops_.size(); ++i) {
                        if (agg_ops_[i].agg_type == AGG_MAX) vals[i] = -std::numeric_limits<float>::max();
                        if (agg_ops_[i].agg_type == AGG_MIN) vals[i] = std::numeric_limits<float>::max();
                    }
                    it = groups_map.emplace(key, std::make_pair(vals, 0)).first;
                    ordered_keys.push_back(key);
                }

                auto &[vals, cnt] = it->second;
                cnt++;
                for (size_t i = 0; i < agg_ops_.size(); ++i) {
                    float v = 0.0f;
                    if (!agg_ops_[i].is_star) {
                        auto pos = get_col(prev_cols, agg_ops_[i].col);
                        v = extract_val(data, *pos);
                    }
                    switch (agg_ops_[i].agg_type) {
                        case AGG_COUNT: vals[i] += 1.0f; break;
                        case AGG_SUM:   vals[i] += v; break;
                        case AGG_MAX:   vals[i] = std::max(vals[i], v); break;
                        case AGG_MIN:   vals[i] = std::min(vals[i], v); break;
                        case AGG_AVG:   vals[i] += v; break;
                    }
                }
            } else {
                single_count++;
                for (size_t i = 0; i < agg_ops_.size(); ++i) {
                    float v = 0.0f;
                    if (!agg_ops_[i].is_star) {
                        auto pos = get_col(prev_cols, agg_ops_[i].col);
                        v = extract_val(data, *pos);
                    }
                    switch (agg_ops_[i].agg_type) {
                        case AGG_COUNT: single_vals[i] += 1.0f; break;
                        case AGG_SUM:   single_vals[i] += v; break;
                        case AGG_MAX:   single_vals[i] = std::max(single_vals[i], v); break;
                        case AGG_MIN:   single_vals[i] = std::min(single_vals[i], v); break;
                        case AGG_AVG:   single_vals[i] += v; break;
                    }
                }
            }
        }

        // Build output schema
        build_output_schema();

        // Finalize and store results
        if (has_group) {
            for (auto &key : ordered_keys) {
                auto &entry = groups_map[key];
                GroupResult gr;
                gr.group_key = key;
                gr.count = entry.second;
                gr.agg_values = entry.first;

                // Finalize AVG
                for (size_t i = 0; i < agg_ops_.size(); ++i) {
                    if (agg_ops_[i].agg_type == AGG_AVG && gr.count > 0) {
                        gr.agg_values[i] /= gr.count;
                    }
                }

                // Apply HAVING filter
                if (eval_having(gr)) {
                    results_.push_back(std::move(gr));
                }
            }
        } else {
            // Single group
            if (single_count > 0 || !agg_ops_.empty()) {
                GroupResult gr;
                gr.count = single_count;
                gr.agg_values = single_vals;
                for (size_t i = 0; i < agg_ops_.size(); ++i) {
                    if (agg_ops_[i].agg_type == AGG_AVG && single_count > 0) {
                        gr.agg_values[i] /= single_count;
                    }
                }
                results_.push_back(std::move(gr));
            }
        }
    }

    void nextTuple() override { current_idx_++; }

    std::unique_ptr<RmRecord> Next() override {
        auto &gr = results_[current_idx_];
        auto rec = std::make_unique<RmRecord>(output_len_);
        memset(rec->data, 0, output_len_);

        // Copy group-by values
        int key_off = 0;
        for (auto &gbc : gb_col_metas_) {
            memcpy(rec->data + gbc.offset, gr.group_key.data() + key_off, gbc.len);
            key_off += gbc.len;
        }

        // Copy aggregate values
        for (size_t i = 0; i < agg_ops_.size(); ++i) {
            auto &out_cm = output_cols_[gb_col_metas_.size() + i];
            float val = gr.agg_values[i];
            switch (agg_ops_[i].agg_type) {
                case AGG_COUNT: {
                    int int_val = static_cast<int>(val);
                    *(int *)(rec->data + out_cm.offset) = int_val;
                    break;
                }
                case AGG_MAX:
                case AGG_MIN: {
                    if (out_cm.type == TYPE_INT) {
                        *(int *)(rec->data + out_cm.offset) = static_cast<int>(val);
                    } else {
                        *(float *)(rec->data + out_cm.offset) = val;
                    }
                    break;
                }
                case AGG_SUM:
                case AGG_AVG: {
                    *(float *)(rec->data + out_cm.offset) = val;
                    break;
                }
            }
        }

        return rec;
    }

    bool is_end() const override { return current_idx_ >= results_.size(); }
    size_t tupleLen() const override { return output_len_; }
    const std::vector<ColMeta> &cols() const override { return output_cols_; }
    Rid &rid() override { return _abstract_rid; }
};

#include <cmath>
#include <cstring>
#include <unordered_map>
#include <unordered_set>

#include "executor_abstract.h"

#endif

class AggregateExecutor : public AbstractExecutor {
   private:
    class AdaptiveDistinct {
       public:
        bool Insert(const char *data, size_t length) {
            if (overflow_ != nullptr) {
                return overflow_->emplace(data, length).second;
            }
            for (size_t i = 0; i < inline_size_; ++i) {
                const auto &value = inline_values_[i];
                if (value.size() == length &&
                    std::memcmp(value.data(), data, length) == 0) {
                    return false;
                }
            }
            if (inline_size_ < inline_values_.size()) {
                inline_values_[inline_size_++].assign(data, length);
                return true;
            }
            overflow_ = std::make_unique<std::unordered_set<std::string>>();
            overflow_->reserve(inline_values_.size() * 4U);
            for (size_t i = 0; i < inline_size_; ++i) {
                overflow_->insert(std::move(inline_values_[i]));
            }
            return overflow_->emplace(data, length).second;
        }

       private:
        static constexpr size_t kInlineCapacity = 24;
        std::array<std::string, kInlineCapacity> inline_values_{};
        size_t inline_size_{0};
        std::unique_ptr<std::unordered_set<std::string>> overflow_;
    };

    struct ValueCell {
        ColType type = TYPE_INT;
        int len = 0;
        int int_val = 0;
        float float_val = 0;
        std::string str_val;
        bool initialized = false;
    };

    struct AggSlot {
        ast::AggType agg_type = ast::AGG_NONE;
        bool count_star = false;
        bool distinct = false;
        ColMeta input_col;
        size_t input_idx = 0;
        ColType input_type = TYPE_INT;
        int input_len = 0;
    };

    struct AggState {
        ColType type = TYPE_INT;
        int int_val = 0;
        float float_val = 0.0f;
        double float_sum = 0.0;
        std::string str_val;
        int str_len = 0;
        int count = 0;
        bool initialized = false;
        std::unique_ptr<AdaptiveDistinct> distinct_values;
    };

    struct GroupColSlot {
        TabCol target;
        ColMeta input_col;
        size_t input_idx = 0;
    };

    struct GroupState {
        std::string raw_key;
        std::vector<ValueCell> group_cells;
        std::vector<AggState> aggs;
    };

    struct OutputSlot {
        bool is_agg = false;
        size_t slot_idx = 0;
        ColMeta output_col;
    };

    struct HavingSlot {
        size_t agg_idx = 0;
        ast::SvCompOp op = ast::SV_OP_EQ;
        ValueCell rhs;
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<std::shared_ptr<ast::SelectItem>> select_items_;
    std::vector<TabCol> group_cols_;
    std::vector<std::shared_ptr<ast::HavingExpr>> having_conds_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<RmRecord> tuples_;
    size_t cursor_ = 0;

    std::vector<AggSlot> agg_slots_;
    std::vector<GroupColSlot> group_slots_;
    std::vector<OutputSlot> output_slots_;
    std::vector<HavingSlot> having_slots_;
    bool single_agg_fast_path_ = false;

    static bool same_agg_expr(const std::shared_ptr<ast::SelectItem> &lhs,
                              const std::shared_ptr<ast::SelectItem> &rhs) {
        if (lhs->agg_type != rhs->agg_type || lhs->count_star != rhs->count_star ||
            lhs->distinct != rhs->distinct) {
            return false;
        }
        if (lhs->count_star) {
            return true;
        }
        if (lhs->col == nullptr || rhs->col == nullptr) {
            return lhs->col == rhs->col;
        }
        return lhs->col->tab_name == rhs->col->tab_name && lhs->col->col_name == rhs->col->col_name;
    }

    size_t ensure_agg_slot(const std::shared_ptr<ast::SelectItem> &item) {
        for (size_t i = 0; i < agg_slots_.size(); ++i) {
            if (same_agg_expr(compiled_items_[i], item)) {
                return i;
            }
        }

        AggSlot slot;
        slot.agg_type = item->agg_type;
        slot.count_star = item->count_star;
        slot.distinct = item->distinct;
        if (!slot.count_star) {
            TabCol input{item->col->tab_name, item->col->col_name};
            auto pos = get_col(prev_->cols(), input);
            slot.input_col = *pos;
            slot.input_idx = static_cast<size_t>(pos - prev_->cols().begin());
            slot.input_type = slot.input_col.type;
            slot.input_len = slot.input_col.len;
        }
        agg_slots_.push_back(slot);
        compiled_items_.push_back(item);
        return agg_slots_.size() - 1;
    }

    ValueCell read_value_cell(const TupleView &view, const ColMeta &col, size_t col_idx) const {
        ValueCell cell;
        cell.type = col.type;
        cell.len = col.len;
        cell.initialized = true;
        const char *data = view.cell_at(col, col_idx);
        if (col.type == TYPE_INT) {
            cell.int_val = rmdb::load_unaligned<int>(data);
        } else if (col.type == TYPE_FLOAT) {
            cell.float_val = rmdb::load_unaligned<float>(data);
        } else {
            cell.str_val.assign(data, static_cast<size_t>(col.len));
        }
        return cell;
    }

    ValueCell rhs_cell(const std::shared_ptr<ast::Value> &value) const {
        ValueCell cell;
        cell.initialized = true;
        if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(value)) {
            cell.type = TYPE_INT;
            cell.len = sizeof(int);
            cell.int_val = int_lit->val;
        } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(value)) {
            cell.type = TYPE_FLOAT;
            cell.len = sizeof(float);
            cell.float_val = float_lit->val;
        } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(value)) {
            cell.type = TYPE_STRING;
            cell.len = static_cast<int>(str_lit->val.size());
            cell.str_val = str_lit->val;
        }
        return cell;
    }

    int compare_cells(const ValueCell &lhs, const ValueCell &rhs) const {
        if (lhs.type == TYPE_FLOAT || rhs.type == TYPE_FLOAT) {
            float l = lhs.type == TYPE_FLOAT ? lhs.float_val : static_cast<float>(lhs.int_val);
            float r = rhs.type == TYPE_FLOAT ? rhs.float_val : static_cast<float>(rhs.int_val);
            return (l > r) - (l < r);
        }
        if (lhs.type == TYPE_INT) {
            return (lhs.int_val > rhs.int_val) - (lhs.int_val < rhs.int_val);
        }
        size_t lhs_len = strnlen(lhs.str_val.data(), lhs.str_val.size());
        size_t rhs_len = strnlen(rhs.str_val.data(), rhs.str_val.size());
        int cmp = std::memcmp(lhs.str_val.data(), rhs.str_val.data(), std::min(lhs_len, rhs_len));
        if (cmp != 0) {
            return (cmp > 0) - (cmp < 0);
        }
        return (lhs_len > rhs_len) - (lhs_len < rhs_len);
    }

    bool compare_having(int cmp, ast::SvCompOp op) const {
        switch (op) {
            case ast::SV_OP_EQ: return cmp == 0;
            case ast::SV_OP_NE: return cmp != 0;
            case ast::SV_OP_LT: return cmp < 0;
            case ast::SV_OP_GT: return cmp > 0;
            case ast::SV_OP_LE: return cmp <= 0;
            case ast::SV_OP_GE: return cmp >= 0;
        }
        return false;
    }

    ValueCell finalized_agg_cell(const AggState &state, const AggSlot &slot) const {
        ValueCell cell;
        cell.initialized = state.initialized;
        if (slot.agg_type == ast::AGG_COUNT) {
            cell.type = TYPE_INT;
            cell.len = sizeof(int);
            cell.int_val = state.int_val;
            cell.initialized = true;
            return cell;
        }
        if (slot.agg_type == ast::AGG_AVG) {
            cell.type = TYPE_FLOAT;
            cell.len = sizeof(float);
            cell.float_val = state.count > 0 ? state.float_val / static_cast<float>(state.count) : state.float_val;
            return cell;
        }
        if (slot.agg_type == ast::AGG_SUM) {
            cell.type = state.type;
            cell.len = state.type == TYPE_FLOAT ? sizeof(float) : sizeof(int);
            cell.int_val = state.int_val;
            cell.float_val = state.type == TYPE_FLOAT ? static_cast<float>(state.float_sum) : state.float_val;
            if (cell.type == TYPE_FLOAT && !std::isfinite(cell.float_val)) {
                throw RMDBError("SUM(FLOAT) result must be finite");
            }
            return cell;
        }
        cell.type = state.type;
        cell.len = state.type == TYPE_STRING ? state.str_len : (state.type == TYPE_FLOAT ? sizeof(float) : sizeof(int));
        cell.int_val = state.int_val;
        cell.float_val = state.float_val;
        cell.str_val = state.str_val;
        return cell;
    }

    bool pass_having(const GroupState &state) const {
        for (const auto &cond : having_slots_) {
            ValueCell lhs = finalized_agg_cell(state.aggs[cond.agg_idx], agg_slots_[cond.agg_idx]);
            if (!compare_having(compare_cells(lhs, cond.rhs), cond.op)) {
                return false;
            }
        }
        return true;
    }

    void update_agg(AggState *state, const AggSlot &slot, const TupleView &view) const {
        if (slot.agg_type == ast::AGG_COUNT) {
            if (slot.distinct) {
                const char *data = view.cell_at(slot.input_col, slot.input_idx);
                if (state->distinct_values == nullptr) {
                    state->distinct_values = std::make_unique<AdaptiveDistinct>();
                }
                if (!state->distinct_values->Insert(
                        data, static_cast<size_t>(slot.input_len))) {
                    return;
                }
            }
            state->type = TYPE_INT;
            state->int_val++;
            state->initialized = true;
            return;
        }

        const char *data = view.cell_at(slot.input_col, slot.input_idx);
        if (slot.agg_type == ast::AGG_SUM) {
            state->type = slot.input_type == TYPE_FLOAT ? TYPE_FLOAT : TYPE_INT;
            if (slot.input_type == TYPE_FLOAT) {
                state->float_sum += static_cast<double>(rmdb::load_unaligned<float>(data));
            } else {
                state->int_val += rmdb::load_unaligned<int>(data);
            }
            state->initialized = true;
            return;
        }
        if (slot.agg_type == ast::AGG_AVG) {
            state->type = TYPE_FLOAT;
            state->float_val += slot.input_type == TYPE_FLOAT
                                    ? rmdb::load_unaligned<float>(data)
                                    : static_cast<float>(rmdb::load_unaligned<int>(data));
            state->count++;
            state->initialized = true;
            return;
        }

        bool replace = !state->initialized;
        if (!replace) {
            int cmp = 0;
            if (slot.input_type == TYPE_FLOAT) {
                float lhs = rmdb::load_unaligned<float>(data);
                cmp = (lhs > state->float_val) - (lhs < state->float_val);
            } else if (slot.input_type == TYPE_INT) {
                int lhs = rmdb::load_unaligned<int>(data);
                cmp = (lhs > state->int_val) - (lhs < state->int_val);
            } else {
                cmp = std::memcmp(data, state->str_val.data(), static_cast<size_t>(slot.input_len));
            }
            replace = (slot.agg_type == ast::AGG_MAX && cmp > 0) ||
                      (slot.agg_type == ast::AGG_MIN && cmp < 0);
        }
        if (!replace) {
            return;
        }
        state->type = slot.input_type;
        if (slot.input_type == TYPE_FLOAT) {
            state->float_val = rmdb::load_unaligned<float>(data);
        } else if (slot.input_type == TYPE_INT) {
            state->int_val = rmdb::load_unaligned<int>(data);
        } else {
            state->str_val.assign(data, static_cast<size_t>(slot.input_len));
            state->str_len = slot.input_len;
        }
        state->initialized = true;
    }

    void init_empty_aggs(GroupState *state) const {
        for (size_t i = 0; i < agg_slots_.size(); ++i) {
            if (agg_slots_[i].agg_type == ast::AGG_COUNT) {
                state->aggs[i].type = TYPE_INT;
                state->aggs[i].int_val = 0;
                state->aggs[i].initialized = true;
            }
        }
    }

    std::string build_group_key(const TupleView &view, std::vector<ValueCell> *group_cells) const {
        std::string key;
        size_t total_len = 0;
        for (const auto &slot : group_slots_) {
            total_len += static_cast<size_t>(slot.input_col.len);
        }
        key.reserve(total_len);
        group_cells->clear();
        group_cells->reserve(group_slots_.size());
        for (const auto &slot : group_slots_) {
            const char *data = view.cell_at(slot.input_col, slot.input_idx);
            key.append(data, static_cast<size_t>(slot.input_col.len));
            group_cells->push_back(read_value_cell(view, slot.input_col, slot.input_idx));
        }
        return key;
    }

    void write_value_cell(RmRecord *rec, const ColMeta &col, const ValueCell &cell) const {
        if (col.type == TYPE_INT) {
            rmdb::store_unaligned<int>(rec->data + col.offset, cell.int_val);
        } else if (col.type == TYPE_FLOAT) {
            float value = cell.type == TYPE_FLOAT ? cell.float_val : static_cast<float>(cell.int_val);
            rmdb::store_unaligned<float>(rec->data + col.offset, value);
        } else {
            memset(rec->data + col.offset, 0, col.len);
            if (!cell.str_val.empty()) {
                memcpy(rec->data + col.offset, cell.str_val.data(),
                       std::min(static_cast<size_t>(col.len), cell.str_val.size()));
            }
        }
    }

    void append_output_tuple(const GroupState &state) {
        tuples_.emplace_back(static_cast<int>(len_));
        auto *out = &tuples_.back();
        for (const auto &slot : output_slots_) {
            ValueCell cell;
            if (slot.is_agg) {
                cell = finalized_agg_cell(state.aggs[slot.slot_idx], agg_slots_[slot.slot_idx]);
            } else {
                cell = state.group_cells[slot.slot_idx];
            }
            write_value_cell(out, slot.output_col, cell);
        }
    }

    void compile_slots(const std::vector<TabCol> &output_cols) {
        compiled_items_.clear();
        agg_slots_.clear();
        group_slots_.clear();
        output_slots_.clear();
        having_slots_.clear();
        cols_.clear();

        const auto &prev_cols = prev_->cols();
        for (const auto &group_col : group_cols_) {
            auto pos = get_col(prev_cols, group_col);
            group_slots_.push_back({group_col, *pos, static_cast<size_t>(pos - prev_cols.begin())});
        }

        len_ = 0;
        for (size_t i = 0; i < select_items_.size(); ++i) {
            const auto &item = select_items_[i];
            ColMeta col;
            col.tab_name = "";
            col.name = output_cols[i].col_name;
            col.offset = static_cast<int>(len_);

            OutputSlot out_slot;
            out_slot.is_agg = item->is_agg;
            if (item->is_agg) {
                size_t agg_idx = ensure_agg_slot(item);
                out_slot.slot_idx = agg_idx;
                const auto &agg = agg_slots_[agg_idx];
                if (agg.agg_type == ast::AGG_COUNT) {
                    col.type = TYPE_INT;
                    col.len = sizeof(int);
                } else if (agg.agg_type == ast::AGG_AVG) {
                    col.type = TYPE_FLOAT;
                    col.len = sizeof(float);
                } else {
                    col.type = agg.input_col.type;
                    col.len = agg.input_col.len;
                }
            } else {
                bool found = false;
                for (size_t group_idx = 0; group_idx < group_slots_.size(); ++group_idx) {
                    if (group_slots_[group_idx].target.tab_name == item->col->tab_name &&
                        group_slots_[group_idx].target.col_name == item->col->col_name) {
                        out_slot.slot_idx = group_idx;
                        col.type = group_slots_[group_idx].input_col.type;
                        col.len = group_slots_[group_idx].input_col.len;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    auto pos = get_col(prev_cols, {item->col->tab_name, item->col->col_name});
                    col.type = pos->type;
                    col.len = pos->len;
                }
            }
            len_ += static_cast<size_t>(col.len);
            cols_.push_back(col);
            out_slot.output_col = col;
            output_slots_.push_back(out_slot);
        }

        for (const auto &cond : having_conds_) {
            HavingSlot having;
            having.agg_idx = ensure_agg_slot(cond->lhs);
            having.op = cond->op;
            having.rhs = rhs_cell(cond->rhs);
            having_slots_.push_back(std::move(having));
        }

        single_agg_fast_path_ = group_slots_.empty() && having_slots_.empty() && select_items_.size() == 1 &&
                                output_slots_.size() == 1 && output_slots_[0].is_agg && agg_slots_.size() == 1;
    }

    void consume_single_agg_input() {
        AggState state;
        const auto &slot = agg_slots_[0];
        bool saw_row = false;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto tuple = prev_->ReadTupleView();
            if (!tuple) {
                break;
            }
            saw_row = true;
            update_agg(&state, slot, *tuple);
        }
        if (!saw_row && slot.agg_type == ast::AGG_COUNT) {
            state.type = TYPE_INT;
            state.int_val = 0;
            state.initialized = true;
        }
        tuples_.emplace_back(static_cast<int>(len_));
        write_value_cell(&tuples_.back(), cols_[0], finalized_agg_cell(state, slot));
    }

    void consume_input() {
        if (single_agg_fast_path_) {
            consume_single_agg_input();
            return;
        }
        if (group_slots_.empty()) {
            GroupState state;
            state.aggs.resize(agg_slots_.size());
            bool saw_row = false;
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                auto tuple = prev_->ReadTupleView();
                if (!tuple) {
                    break;
                }
                saw_row = true;
                for (size_t i = 0; i < agg_slots_.size(); ++i) {
                    update_agg(&state.aggs[i], agg_slots_[i], *tuple);
                }
            }
            if (!saw_row) {
                init_empty_aggs(&state);
            }
            if (pass_having(state)) {
                append_output_tuple(state);
            }
            return;
        }

        std::unordered_map<std::string, size_t> group_index;
        std::vector<GroupState> groups;
        std::vector<ValueCell> group_cells;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto tuple = prev_->ReadTupleView();
            if (!tuple) {
                break;
            }
            std::string key = build_group_key(*tuple, &group_cells);
            auto iter = group_index.find(key);
            if (iter == group_index.end()) {
                GroupState state;
                state.raw_key = key;
                state.group_cells = group_cells;
                state.aggs.resize(agg_slots_.size());
                groups.push_back(std::move(state));
                iter = group_index.emplace(groups.back().raw_key, groups.size() - 1).first;
            }
            auto &state = groups[iter->second];
            for (size_t i = 0; i < agg_slots_.size(); ++i) {
                update_agg(&state.aggs[i], agg_slots_[i], *tuple);
            }
        }

        for (const auto &state : groups) {
            if (pass_having(state)) {
                append_output_tuple(state);
            }
        }
    }

    std::vector<std::shared_ptr<ast::SelectItem>> compiled_items_;

   public:
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev,
                      std::vector<std::shared_ptr<ast::SelectItem>> select_items,
                      std::vector<TabCol> group_cols,
                      std::vector<std::shared_ptr<ast::HavingExpr>> having_conds,
                      std::vector<TabCol> output_cols) {
        prev_ = std::move(prev);
        select_items_ = std::move(select_items);
        group_cols_ = std::move(group_cols);
        having_conds_ = std::move(having_conds);
        compile_slots(output_cols);
    }

    void beginTuple() override {
        tuples_.clear();
        consume_input();
        cursor_ = 0;
    }

    void nextTuple() override {
        if (cursor_ < tuples_.size()) {
            cursor_++;
        }
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "AggregateExecutor"; }
    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }
    Rid &rid() override { return _abstract_rid; }

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
};

