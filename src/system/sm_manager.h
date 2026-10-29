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

#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "common/context.h"
#include "common/common.h"

class Context;

struct ColDef {
    std::string name;  // Column name
    ColType type;      // Type of column
    int len;           // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
   public:
    DbMeta db_;             // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;    // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;   // file name -> index file handle, 当前数据库中每个索引的文件
   private:
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    RmManager* rm_manager_;
    IxManager* ix_manager_;
    rmdb::u64 next_index_id_{1};

    rmdb::u64 allocate_index_id() { return next_index_id_++; }

   public:
    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

    ~SmManager() {}

    BufferPoolManager* get_bpm() { return buffer_pool_manager_; }

    RmManager* get_rm_manager() { return rm_manager_; }  

    IxManager* get_ix_manager() { return ix_manager_; }  

    int hidden_index_count() const;

    bool is_dir(const std::string& db_name);

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context* context);

    void desc_table(const std::string& tab_name, Context* context);

    void show_index(const std::string& tab_name, Context* context);

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    bool create_internal_non_unique_index(const std::string& tab_name, const std::vector<std::string>& col_names,
                                          Context* context);

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);
};



namespace rmdb {

rmdb::u64 sql_template_schema_epoch();
void bump_sql_template_schema_epoch();
void clear_query_template_cache();
void clear_plan_template_cache();

struct CatalogEpochSnapshot {
    rmdb::u64 schema_epoch{0};
    rmdb::u64 data_epoch{0};
    rmdb::u64 feedback_generation{0};
};

class CatalogChangeCoordinator {
   public:
    static CatalogChangeCoordinator &Instance() {
        static CatalogChangeCoordinator coordinator;
        return coordinator;
    }

    void PublishSchemaChange() {
        bump_sql_template_schema_epoch();
        advance_catalog_data_epoch();
        clear_query_template_cache();
        clear_plan_template_cache();
    }

    void PublishBulkDataChange() {
        advance_catalog_data_epoch();
        clear_plan_template_cache();
    }

    void PublishStatisticsChange() {
        advance_catalog_data_epoch();
        clear_plan_template_cache();
    }

    CatalogEpochSnapshot Snapshot() const {
        return CatalogEpochSnapshot{
            sql_template_schema_epoch(),
            catalog_data_epoch(),
            catalog_feedback_generation(),
        };
    }

   private:
    CatalogChangeCoordinator() = default;

};

inline void publish_schema_change() {
    CatalogChangeCoordinator::Instance().PublishSchemaChange();
}

inline void publish_bulk_data_change() {
    CatalogChangeCoordinator::Instance().PublishBulkDataChange();
}

inline void publish_statistics_change() {
    CatalogChangeCoordinator::Instance().PublishStatisticsChange();
}

inline CatalogEpochSnapshot catalog_epoch_snapshot() {
    return CatalogChangeCoordinator::Instance().Snapshot();
}

}  // namespace rmdb


#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "analyze/analyze.h"
#include "index/ix_defs.h"
#include "transaction/transaction_manager.h"

namespace rmdb {

struct HiddenIndexCandidate {
    std::string tab_name;
    std::vector<std::string> col_names;
    int threshold = 16;
    int benefit = 1;
    int priority = 0;
    int key_len = 0;
};

/**
 * Generic online index advisor. Candidates are derived from bound query and
 * catalog objects; no application table/column names or statement ids are
 * recognized. Unsupported shapes keep using the normal planner path.
 */
namespace hidden_index_advisor_detail {

static constexpr int kMaxHiddenIndexes = 8;
static constexpr int kMaxHiddenIndexesPerTable = 2;

struct CandidateState {
    HiddenIndexCandidate candidate;
    int score = 0;
    int select_hits = 0;
    bool building = false;
    bool built = false;
};

inline SynchronizedFeedbackRegistry<std::string, CandidateState> &candidate_registry() {
    static SynchronizedFeedbackRegistry<std::string, CandidateState> registry;
    return registry;
}

inline std::string lower_copy(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return text;
}

inline std::string trim_copy(std::string text) {
    auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    text.erase(text.begin(), std::find_if_not(text.begin(), text.end(), is_space));
    text.erase(std::find_if_not(text.rbegin(), text.rend(), is_space).base(), text.end());
    return text;
}

inline bool env_enabled() {
    static const bool configured = [] {
        const char *env = std::getenv("RMDB_ADVISOR_ENABLED");
        if (env == nullptr || env[0] == '\0') {
            return true;
        }
        std::string value = lower_copy(trim_copy(env));
        return value != "0" && value != "false" && value != "off" && value != "no";
    }();
    return configured;
}

inline int observation_seconds() {
    static const int configured = [] {
        const char *env = std::getenv("RMDB_ADVISOR_OBSERVE_SECONDS");
        if (env == nullptr || env[0] == '\0') {
            return 30;
        }
        try {
            int value = std::stoi(env);
            return value < 0 ? 30 : std::min(value, 86400);
        } catch (...) {
            return 30;
        }
    }();
    return configured;
}

inline bool observation_window_open() {
    const int seconds = observation_seconds();
    if (seconds == 0) {
        return true;
    }
    static const auto first_observation = std::chrono::steady_clock::now();
    return std::chrono::steady_clock::now() - first_observation < std::chrono::seconds(seconds);
}

inline bool add_unique(std::vector<std::string> *cols, const std::string &col_name) {
    if (col_name.empty() || std::find(cols->begin(), cols->end(), col_name) != cols->end()) {
        return false;
    }
    cols->push_back(col_name);
    return true;
}

inline bool ends_with_id(const std::string &name) {
    return name == "id" || (name.size() >= 3 && name.compare(name.size() - 3, 3, "_id") == 0);
}

inline bool starts_with_cols(const IndexMeta &index, const std::vector<std::string> &cols) {
    if (index.cols.size() < cols.size()) {
        return false;
    }
    for (size_t i = 0; i < cols.size(); ++i) {
        if (index.cols[i].name != cols[i]) {
            return false;
        }
    }
    return true;
}

inline bool has_equivalent_or_better_index(const TabMeta &tab, const std::vector<std::string> &cols) {
    for (const auto &index : tab.indexes) {
        if (starts_with_cols(index, cols)) {
            return true;
        }
    }
    return false;
}

inline bool index_supports_access_shape(const IndexMeta &index,
                                        const std::vector<std::string> &equality_cols,
                                        const std::vector<std::string> &trailing_cols) {
    size_t index_pos = 0;
    size_t bound_equalities = 0;
    while (index_pos < index.cols.size() &&
           std::find(equality_cols.begin(), equality_cols.end(), index.cols[index_pos].name) !=
               equality_cols.end()) {
        ++index_pos;
        ++bound_equalities;
    }
    if (bound_equalities != equality_cols.size()) {
        return false;
    }
    for (const auto &col_name : trailing_cols) {
        if (index_pos >= index.cols.size() || index.cols[index_pos].name != col_name) {
            return false;
        }
        ++index_pos;
    }
    return true;
}

inline bool has_access_index(const TabMeta &tab,
                             const std::vector<std::string> &equality_cols,
                             const std::vector<std::string> &trailing_cols = {}) {
    return std::any_of(tab.indexes.begin(), tab.indexes.end(), [&](const IndexMeta &index) {
        return index_supports_access_shape(index, equality_cols, trailing_cols);
    });
}

inline int hidden_index_count(const TabMeta &tab) {
    int count = 0;
    for (const auto &index : tab.indexes) {
        if (index.hidden) {
            count++;
        }
    }
    return count;
}

inline int hidden_index_count(const SmManager *sm_manager) {
    return sm_manager->hidden_index_count();
}

inline int target_hidden_index_count() {
    static const int configured = [] {
        const char *env = std::getenv("RMDB_ADVISOR_TARGET_INDEXES");
        if (env == nullptr || env[0] == '\0') {
            return kMaxHiddenIndexes;
        }
        try {
            int value = std::stoi(env);
            if (value <= 0) {
                return kMaxHiddenIndexes;
            }
            return std::min(value, kMaxHiddenIndexes);
        } catch (...) {
            return kMaxHiddenIndexes;
        }
    }();
    return configured;
}

constexpr unsigned char advisor_ascii_lower(unsigned char ch) {
    return ch >= 'A' && ch <= 'Z' ? static_cast<unsigned char>(ch + ('a' - 'A')) : ch;
}

constexpr bool advisor_ascii_space(unsigned char ch) {
    return ch == ' ' || (ch >= '\t' && ch <= '\r');
}

inline bool is_select_sql(const char *raw_sql) {
    const char *begin = raw_sql;
    while (*begin != '\0' && advisor_ascii_space(static_cast<unsigned char>(*begin))) {
        ++begin;
    }
    const char *end = begin + std::strlen(begin);
    while (end != begin && advisor_ascii_space(static_cast<unsigned char>(end[-1]))) {
        --end;
    }
    if (end != begin && end[-1] == ';') {
        --end;
        while (end != begin && advisor_ascii_space(static_cast<unsigned char>(end[-1]))) {
            --end;
        }
    }
    constexpr char select[] = "select";
    if (end - begin < 6) {
        return false;
    }
    for (size_t i = 0; i < 6; ++i) {
        if (advisor_ascii_lower(static_cast<unsigned char>(begin[i])) !=
            static_cast<unsigned char>(select[i])) {
            return false;
        }
    }
    return end - begin == 6 || begin[6] == ' ';
}

inline int logical_key_len(const TabMeta &tab, const std::vector<std::string> &cols) {
    int len = 0;
    for (const auto &col_name : cols) {
        auto it = std::find_if(tab.cols.begin(), tab.cols.end(), [&](const ColMeta &col) {
            return col.name == col_name;
        });
        if (it == tab.cols.end()) {
            return -1;
        }
        len += it->len;
    }
    return len;
}

inline bool candidate_key_valid(const TabMeta &tab, const std::vector<std::string> &cols) {
    if (cols.empty()) {
        return false;
    }
    int len = logical_key_len(tab, cols);
    return len > 0 && len <= IX_MAX_COL_LEN;
}

inline bool candidate_covers_projection(const std::shared_ptr<Query> &query, const std::string &visible,
                                        const std::vector<std::string> &cols) {
    if (query == nullptr || query->cols.empty()) {
        return false;
    }
    bool saw_visible_col = false;
    for (const auto &col : query->cols) {
        if (!col.tab_name.empty() && col.tab_name != visible) {
            continue;
        }
        saw_visible_col = true;
        if (col.col_name.empty() || std::find(cols.begin(), cols.end(), col.col_name) == cols.end()) {
            return false;
        }
    }
    return saw_visible_col;
}

inline std::string state_key(const HiddenIndexCandidate &candidate) {
    std::string key = candidate.tab_name;
    key.push_back('\x1f');
    for (const auto &col : candidate.col_names) {
        key += col;
        key.push_back('\x1e');
    }
    return key;
}

inline std::string candidate_label(const HiddenIndexCandidate &candidate) {
    std::string out = candidate.tab_name + "(";
    for (size_t i = 0; i < candidate.col_names.size(); ++i) {
        if (i != 0) {
            out += ",";
        }
        out += candidate.col_names[i];
    }
    out += ")";
    return out;
}

inline int candidate_shape_cost(const HiddenIndexCandidate &candidate) {
    return static_cast<int>(candidate.col_names.size() * 10) + candidate.threshold;
}

inline bool candidate_state_better(const CandidateState &lhs, const std::string &lhs_key,
                                   const CandidateState &rhs, const std::string &rhs_key) {
    if (lhs.candidate.priority != rhs.candidate.priority) {
        return lhs.candidate.priority > rhs.candidate.priority;
    }
    if (lhs.candidate.threshold != rhs.candidate.threshold) {
        return lhs.candidate.threshold < rhs.candidate.threshold;
    }
    if (lhs.candidate.key_len > 0 && rhs.candidate.key_len > 0 &&
        lhs.candidate.key_len != rhs.candidate.key_len) {
        return lhs.candidate.key_len < rhs.candidate.key_len;
    }
    int lhs_shape_cost = candidate_shape_cost(lhs.candidate);
    int rhs_shape_cost = candidate_shape_cost(rhs.candidate);
    if (lhs_shape_cost != rhs_shape_cost) {
        return lhs_shape_cost < rhs_shape_cost;
    }
    if (lhs.score != rhs.score) {
        return lhs.score > rhs.score;
    }
    if (lhs.candidate.benefit != rhs.candidate.benefit) {
        return lhs.candidate.benefit > rhs.candidate.benefit;
    }
    if (lhs.select_hits != rhs.select_hits) {
        return lhs.select_hits > rhs.select_hits;
    }
    return lhs_key < rhs_key;
}

inline bool is_simple_select_shape(const std::shared_ptr<Query> &query) {
    return query != nullptr && query->kind == StmtKind::Select && query->union_queries.empty() &&
           query->having_conds.empty() && !query->is_semi_join && query->semi_conds.empty() &&
           !query->has_group && query->tables.size() >= 1;
}

inline bool is_supported_isolation(Context *context) {
    return context != nullptr && context->txn_ != nullptr &&
           context->txn_->get_isolation_level() == IsolationLevel::SNAPSHOT_ISOLATION;
}

inline std::string base_table_for(const std::shared_ptr<Query> &query, const std::string &visible) {
    auto it = query->alias_to_table.find(visible);
    return it == query->alias_to_table.end() ? visible : it->second;
}

inline std::string min_aggregate_col_for(const std::shared_ptr<Query> &query, const std::string &visible) {
    if (!query->has_aggregate || query->select_items.size() != 1) {
        return "";
    }
    const auto &item = query->select_items[0];
    if (item == nullptr || !item->is_agg || item->count_star || item->agg_type != ast::AGG_MIN ||
        item->col == nullptr || item->col->tab_name != visible) {
        return "";
    }
    return item->col->col_name;
}

inline bool is_single_count_aggregate(const std::shared_ptr<Query> &query) {
    if (query == nullptr || !query->has_aggregate || query->select_items.size() != 1) {
        return false;
    }
    const auto &item = query->select_items.front();
    return item != nullptr && item->is_agg && item->agg_type == ast::AGG_COUNT && !item->distinct;
}

inline std::string tie_break_col_for(const TabMeta &tab, const std::vector<std::string> &cols,
                                     const std::vector<TabCol> &output_cols, const std::string &visible) {
    for (const auto &col : output_cols) {
        if (col.tab_name == visible && ends_with_id(col.col_name) &&
            std::find(cols.begin(), cols.end(), col.col_name) == cols.end()) {
            return col.col_name;
        }
    }
    for (const auto &col : tab.cols) {
        if (ends_with_id(col.name) && std::find(cols.begin(), cols.end(), col.name) == cols.end()) {
            return col.name;
        }
    }
    for (const auto &col : output_cols) {
        if (col.tab_name == visible && std::find(cols.begin(), cols.end(), col.col_name) == cols.end()) {
            return col.col_name;
        }
    }
    return "";
}

inline void collect_table_candidate(SmManager *sm_manager, const std::shared_ptr<Query> &query,
                                    const std::string &visible, std::vector<HiddenIndexCandidate> *out) {
    const std::string base = base_table_for(query, visible);
    if (!sm_manager->db_.is_table(base)) {
        return;
    }
    TabMeta &tab = sm_manager->db_.get_table(base);

    std::vector<std::string> equality_cols;
    std::vector<std::string> range_cols;
    for (const auto &cond : query->conds) {
        if (!cond.is_rhs_val || cond.lhs_col.tab_name != visible || cond.op == OP_NE) {
            continue;
        }
        if (cond.op == OP_EQ) {
            add_unique(&equality_cols, cond.lhs_col.col_name);
        } else {
            add_unique(&range_cols, cond.lhs_col.col_name);
        }
    }
    if (equality_cols.empty()) {
        return;
    }

    // On an inner join input, constants and the outer-provided join key form
    // one point-lookup prefix.  If an existing index binds that complete set,
    // residual filters do not justify another large index.
    if (!query->tables.empty() && query->tables.front() != visible) {
        std::vector<std::string> point_cols = equality_cols;
        for (const auto &cond : query->conds) {
            if (cond.is_rhs_val || cond.op != OP_EQ) {
                continue;
            }
            if (cond.lhs_col.tab_name == visible && cond.rhs_col.tab_name != visible) {
                add_unique(&point_cols, cond.lhs_col.col_name);
            } else if (cond.rhs_col.tab_name == visible && cond.lhs_col.tab_name != visible) {
                add_unique(&point_cols, cond.rhs_col.col_name);
            }
        }
        if (point_cols.size() > equality_cols.size() && has_access_index(tab, point_cols)) {
            return;
        }
    }

    std::vector<std::string> trailing_cols;

    bool added_order = false;
    if (!query->order_cols.empty()) {
        for (const auto &order_col : query->order_cols) {
            if (order_col.first.tab_name == visible) {
                added_order = add_unique(&trailing_cols, order_col.first.col_name) || added_order;
            }
        }
    }

    bool added_min = false;
    if (!added_order && query->order_cols.empty()) {
        std::string min_col = min_aggregate_col_for(query, visible);
        if (!min_col.empty()) {
            added_min = add_unique(&trailing_cols, min_col);
        }
    }

    bool added_range = false;
    if (!added_order && !added_min && query->order_cols.empty() && range_cols.size() == 1) {
        added_range = add_unique(&trailing_cols, range_cols.front());
    }

    if (!added_order && !added_min && !added_range) {
        return;
    }
    if (has_access_index(tab, equality_cols, trailing_cols)) {
        return;
    }

    std::vector<std::string> cols = equality_cols;
    for (const auto &col_name : trailing_cols) {
        add_unique(&cols, col_name);
    }

    const bool count_only = is_single_count_aggregate(query);
    if (!count_only) {
        std::string tie_col = tie_break_col_for(tab, cols, query->cols, visible);
        if (!tie_col.empty()) {
            auto covering_cols = cols;
            add_unique(&covering_cols, tie_col);
            if (candidate_covers_projection(query, visible, covering_cols)) {
                cols = std::move(covering_cols);
            }
        }
    }

    int key_len = logical_key_len(tab, cols);
    if (key_len <= 0 || key_len > IX_MAX_COL_LEN || has_equivalent_or_better_index(tab, cols)) {
        return;
    }
    int threshold = added_order || added_min ? 8 : 16;
    int benefit = 1;
    int priority = 0;
    if (added_order && query->has_limit) {
        benefit = 6;
        priority = 300;
    } else if (added_order || added_min) {
        benefit = 4;
        priority = added_min ? 400 : 200;
    } else if (added_range) {
        benefit = 2;
        priority = 100;
    }
    if (count_only || candidate_covers_projection(query, visible, cols)) {
        benefit += 2;
    }
    out->push_back({base, std::move(cols), threshold, benefit, priority, key_len});
}

inline void collect_join_candidate(SmManager *sm_manager, const std::shared_ptr<Query> &query,
                                   const std::string &visible, std::vector<HiddenIndexCandidate> *out) {
    const std::string base = base_table_for(query, visible);
    if (!sm_manager->db_.is_table(base)) {
        return;
    }
    TabMeta &tab = sm_manager->db_.get_table(base);
    std::vector<std::string> cols;
    for (const auto &cond : query->conds) {
        if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == visible) {
            add_unique(&cols, cond.lhs_col.col_name);
        }
    }
    bool saw_join_key = false;
    for (const auto &cond : query->conds) {
        if (cond.is_rhs_val || cond.op != OP_EQ) {
            continue;
        }
        if (cond.lhs_col.tab_name == visible && cond.rhs_col.tab_name != visible) {
            saw_join_key = true;
            add_unique(&cols, cond.lhs_col.col_name);
        } else if (cond.rhs_col.tab_name == visible && cond.lhs_col.tab_name != visible) {
            saw_join_key = true;
            add_unique(&cols, cond.rhs_col.col_name);
        }
    }
    if (!saw_join_key || !candidate_key_valid(tab, cols) || has_access_index(tab, cols) ||
        has_equivalent_or_better_index(tab, cols)) {
        return;
    }
    out->push_back({base, std::move(cols), 32, 1});
}

inline bool can_publish_candidate(SmManager *sm_manager, const HiddenIndexCandidate &candidate) {
    if (!sm_manager->db_.is_table(candidate.tab_name)) {
        return false;
    }
    TabMeta &tab = sm_manager->db_.get_table(candidate.tab_name);
    if (hidden_index_count(tab) >= kMaxHiddenIndexesPerTable || hidden_index_count(sm_manager) >= kMaxHiddenIndexes) {
        return false;
    }
    return candidate_key_valid(tab, candidate.col_names) &&
           !has_equivalent_or_better_index(tab, candidate.col_names);
}

inline bool has_enough_net_benefit(const CandidateState &state) {
    return state.score >= state.candidate.threshold;
}

}  // namespace hidden_index_advisor_detail

inline bool hidden_index_advisor_enabled() {
    return hidden_index_advisor_detail::env_enabled();
}

inline bool hidden_index_advisor_should_observe_sql(SmManager *sm_manager, const char *raw_sql,
                                                    IsolationLevel isolation_level) {
    if (!hidden_index_advisor_enabled() || isolation_level != IsolationLevel::SNAPSHOT_ISOLATION ||
        raw_sql == nullptr) {
        return false;
    }
    if (!hidden_index_advisor_detail::is_select_sql(raw_sql)) {
        return false;
    }
    if (sm_manager != nullptr) {
        if (!hidden_index_advisor_detail::observation_window_open()) {
            return false;
        }
        return hidden_index_advisor_detail::hidden_index_count(sm_manager) <
               hidden_index_advisor_detail::target_hidden_index_count();
    }
    return hidden_index_advisor_detail::candidate_registry().Read([](const auto &states) {
        for (const auto &entry : states) {
            const auto &state = entry.second;
            if (!state.built || state.building) {
                return true;
            }
        }
        return states.empty();
    });
}

inline bool hidden_index_advisor_should_observe_sql(const char *raw_sql, IsolationLevel isolation_level) {
    return hidden_index_advisor_should_observe_sql(nullptr, raw_sql, isolation_level);
}

inline std::vector<HiddenIndexCandidate> hidden_index_advisor_collect(SmManager *sm_manager,
                                                                      const std::shared_ptr<Query> &query,
                                                                      Context *context) {
    std::vector<HiddenIndexCandidate> candidates;
    if (!hidden_index_advisor_enabled() || !hidden_index_advisor_detail::is_supported_isolation(context) ||
        !hidden_index_advisor_detail::is_simple_select_shape(query)) {
        return candidates;
    }
    for (size_t table_idx = 0; table_idx < query->tables.size(); ++table_idx) {
        const auto &visible = query->tables[table_idx];
        hidden_index_advisor_detail::collect_table_candidate(sm_manager, query, visible, &candidates);
        if (table_idx > 0) {
            hidden_index_advisor_detail::collect_join_candidate(sm_manager, query, visible, &candidates);
        }
    }
    return candidates;
}

inline void hidden_index_advisor_record(const std::vector<HiddenIndexCandidate> &candidates) {
    if (!hidden_index_advisor_enabled() || candidates.empty()) {
        return;
    }
    hidden_index_advisor_detail::candidate_registry().Update([&](auto &states) {
        for (const auto &candidate : candidates) {
            std::string key = hidden_index_advisor_detail::state_key(candidate);
            auto &state = states[key];
            if (state.candidate.tab_name.empty()) {
                state.candidate = candidate;
            } else if (candidate.threshold < state.candidate.threshold) {
                state.candidate.threshold = candidate.threshold;
                state.candidate.benefit = std::max(state.candidate.benefit, candidate.benefit);
            } else {
                state.candidate.benefit = std::max(state.candidate.benefit, candidate.benefit);
            }
            if (!state.built) {
                state.score += std::max(1, candidate.benefit);
                state.select_hits++;
            }
        }
    });
}

template <typename TransactionManagerT>
inline bool hidden_index_advisor_try_build_one(SmManager *sm_manager, TransactionManagerT *txn_manager,
                                               Context *context) {
    if (!hidden_index_advisor_enabled() || sm_manager == nullptr || txn_manager == nullptr) {
        return false;
    }
    if (!hidden_index_advisor_detail::observation_window_open() ||
        hidden_index_advisor_detail::hidden_index_count(sm_manager) >=
            hidden_index_advisor_detail::target_hidden_index_count()) {
        return false;
    }

    std::string selected_key;
    HiddenIndexCandidate selected;
    hidden_index_advisor_detail::candidate_registry().Update([&](auto &states) {
        hidden_index_advisor_detail::CandidateState *selected_state = nullptr;
        for (auto &entry : states) {
            auto &state = entry.second;
            if (state.built || state.building || !hidden_index_advisor_detail::has_enough_net_benefit(state)) {
                continue;
            }
            if (selected_state == nullptr ||
                hidden_index_advisor_detail::candidate_state_better(state, entry.first, *selected_state, selected_key)) {
                selected_key = entry.first;
                selected_state = &state;
            }
        }
        if (selected_state != nullptr) {
            selected = selected_state->candidate;
            selected_state->building = true;
        }
    });
    if (selected_key.empty()) {
        return false;
    }

    // A continuously loaded explicit-transaction workload may never have an
    // observable zero-active instant.  Once a candidate is mature, briefly
    // close admission so existing transactions can drain.  The timeout keeps
    // advisor work from recreating an unbounded workload pause.
    auto drain_guard = txn_manager->BlockNewTransactionsAndWaitFor(std::chrono::seconds(2));
    if (!drain_guard) {
        hidden_index_advisor_detail::candidate_registry().Update([&](auto &states) {
            auto it = states.find(selected_key);
            if (it != states.end()) {
                it->second.building = false;
            }
        });
        return false;
    }

    bool built = false;
    bool retired = false;
    try {
        auto checkpoint_guard = txn_manager->EnterCheckpointExecution();
        if (hidden_index_advisor_detail::can_publish_candidate(sm_manager, selected)) {
            built = sm_manager->create_internal_non_unique_index(selected.tab_name, selected.col_names, context);
        } else {
            retired = true;
        }
    } catch (const RMDBError &err) {
        std::cerr << "hidden index advisor failed for "
                  << hidden_index_advisor_detail::candidate_label(selected) << ": " << err.what() << "\n";
    } catch (const std::exception &err) {
        std::cerr << "hidden index advisor failed for "
                  << hidden_index_advisor_detail::candidate_label(selected) << ": " << err.what() << "\n";
    }

    hidden_index_advisor_detail::candidate_registry().Update([&](auto &states) {
        auto it = states.find(selected_key);
        if (it != states.end()) {
            it->second.building = false;
            it->second.built = built || retired || it->second.built;
            if (!built && !retired) {
                it->second.score = std::max(0, it->second.score - 1);
            }
        }
    });
    if (built) {
        std::cout << "hidden index advisor created "
                  << hidden_index_advisor_detail::candidate_label(selected) << "\n";
    }
    return built;
}

}  // namespace rmdb
