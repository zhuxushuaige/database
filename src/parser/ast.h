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

#include <vector>
#include <string>
#include <memory>
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include "common/common.h"

enum JoinType {
    INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, SEMI_JOIN
};
namespace ast {

enum SvType {
    SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL
};

enum SvCompOp {
    SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE
};

enum OrderByDir {
    OrderBy_DEFAULT,
    OrderBy_ASC,
    OrderBy_DESC
};

enum AggType {
    AGG_NONE,
    AGG_COUNT,
    AGG_MAX,
    AGG_MIN,
    AGG_SUM,
    AGG_AVG
};

enum SetKnobType {
    EnableNestLoop, EnableSortMerge
};

enum SetClauseOp {
    SET_OP_ASSIGN,
    SET_OP_ADD,
    SET_OP_SUB,
    SET_OP_MUL,
    SET_OP_DIV
};

// Base class for tree nodes
struct TreeNode {
    virtual ~TreeNode() = default;  // enable polymorphism
};

struct Help : public TreeNode {
};

struct ShowTables : public TreeNode {
};

struct ShowIndex : public TreeNode {
    std::string tab_name;

    ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct TxnBegin : public TreeNode {
};

struct TxnCommit : public TreeNode {
};

struct TxnAbort : public TreeNode {
};

struct TxnRollback : public TreeNode {
};

struct SetTransactionIsolation : public TreeNode {
    bool serializable;

    SetTransactionIsolation(bool serializable_) : serializable(serializable_) {}
};

struct TypeLen : public TreeNode {
    SvType type;
    int len;

    TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
};

struct Field : public TreeNode {
};

struct ColDef : public Field {
    std::string col_name;
    std::shared_ptr<TypeLen> type_len;

    ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_) :
            col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
};

struct CreateTable : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Field>> fields;

    CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_) :
            tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
    std::string tab_name;

    DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
    std::string tab_name;

    DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    CreateIndex(std::string tab_name_, std::vector<std::string> col_names_) :
            tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct DropIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    DropIndex(std::string tab_name_, std::vector<std::string> col_names_) :
            tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct LoadStmt : public TreeNode {
    std::string file_name;
    std::string tab_name;

    LoadStmt(std::string file_name_, std::string tab_name_)
        : file_name(std::move(file_name_)), tab_name(std::move(tab_name_)) {}
};

struct Expr : public TreeNode {
};

struct Value : public Expr {
};

struct IntLit : public Value {
    int val;

    IntLit(int val_) : val(val_) {}
};

struct FloatLit : public Value {
    float val;

    FloatLit(float val_) : val(val_) {}
};

struct StringLit : public Value {
    std::string val;

    StringLit(std::string val_) : val(std::move(val_)) {}
};

struct BoolLit : public Value {
    bool val;

    BoolLit(bool val_) : val(val_) {}
};

struct ParameterRef : public Value {
    rmdb::u32 ordinal;

    explicit ParameterRef(rmdb::u32 ordinal_) : ordinal(ordinal_) {}
};

struct Col : public Expr {
    std::string tab_name;
    std::string col_name;

    Col(std::string tab_name_, std::string col_name_) :
            tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}
};

struct SelectItem : public TreeNode {
    bool is_agg;
    AggType agg_type;
    std::shared_ptr<Col> col;
    bool count_star;
    bool distinct;
    std::string alias;

    SelectItem(std::shared_ptr<Col> col_, std::string alias_ = "") :
            is_agg(false), agg_type(AGG_NONE), col(std::move(col_)), count_star(false), distinct(false),
            alias(std::move(alias_)) {}

    SelectItem(AggType agg_type_, std::shared_ptr<Col> col_, bool count_star_, std::string alias_ = "",
               bool distinct_ = false) :
            is_agg(true), agg_type(agg_type_), col(std::move(col_)), count_star(count_star_), distinct(distinct_),
            alias(std::move(alias_)) {}
};

struct SetClause : public TreeNode {
    struct ArithmeticStep {
        SetClauseOp op;
        std::shared_ptr<Value> val;

        ArithmeticStep(SetClauseOp op_, std::shared_ptr<Value> val_) : op(op_), val(std::move(val_)) {}
    };

    std::string col_name;
    std::string rhs_col_name;
    std::shared_ptr<Value> val;
    bool rhs_is_col;
    SetClauseOp op;
    bool reversed_operands{false};  // true for e.g. SET col = 10 - col (literal on left)
    std::vector<ArithmeticStep> extra_steps;

    SetClause(std::string col_name_, std::shared_ptr<Value> val_) :
            col_name(std::move(col_name_)), rhs_col_name(), val(std::move(val_)), rhs_is_col(false),
            op(SET_OP_ASSIGN) {}

    SetClause(std::string col_name_, std::string rhs_col_name_) :
            col_name(std::move(col_name_)), rhs_col_name(std::move(rhs_col_name_)), val(nullptr), rhs_is_col(true),
            op(SET_OP_ASSIGN) {}

    SetClause(std::string col_name_, std::string rhs_col_name_, std::shared_ptr<Value> val_, SetClauseOp op_,
              bool reversed_ = false) :
            col_name(std::move(col_name_)), rhs_col_name(std::move(rhs_col_name_)), val(std::move(val_)),
            rhs_is_col(true), op(op_), reversed_operands(reversed_) {}

    void append_step(SetClauseOp op_, std::shared_ptr<Value> val_) {
        extra_steps.emplace_back(op_, std::move(val_));
    }
};

struct BinaryExpr : public TreeNode {
    std::shared_ptr<Col> lhs;
    SvCompOp op;
    std::shared_ptr<Expr> rhs;

    BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) :
            lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct HavingExpr : public TreeNode {
    std::shared_ptr<SelectItem> lhs;
    SvCompOp op;
    std::shared_ptr<Value> rhs;

    HavingExpr(std::shared_ptr<SelectItem> lhs_, SvCompOp op_, std::shared_ptr<Value> rhs_) :
            lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderByItem : public TreeNode {
    std::shared_ptr<Col> col;
    OrderByDir orderby_dir;

    OrderByItem(std::shared_ptr<Col> col_, OrderByDir orderby_dir_) :
            col(std::move(col_)), orderby_dir(orderby_dir_) {}
};

struct OrderBy : public TreeNode
{
    std::vector<std::shared_ptr<OrderByItem>> items;
    OrderBy(std::vector<std::shared_ptr<OrderByItem>> items_) : items(std::move(items_)) {}
};

struct SelectStmt;

struct TableRef : public TreeNode {
    std::string tab_name;
    std::string alias;
    std::vector<std::shared_ptr<SelectStmt>> union_selects;

    TableRef(std::string tab_name_, std::string alias_) :
            tab_name(std::move(tab_name_)), alias(std::move(alias_)) {}

    TableRef(std::vector<std::shared_ptr<SelectStmt>> union_selects_, std::string alias_) :
            tab_name(std::move(alias_)), alias(tab_name), union_selects(std::move(union_selects_)) {}

    std::string visible_name() const { return alias.empty() ? tab_name : alias; }
};

struct FromClause : public TreeNode {
    std::vector<std::shared_ptr<TableRef>> table_refs;
    std::vector<std::shared_ptr<BinaryExpr>> join_conds;
    bool is_semi_join;
    std::vector<std::shared_ptr<BinaryExpr>> semi_conds;

    FromClause() : is_semi_join(false) {}
};

struct InsertStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Value>> vals;

    InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_) :
            tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct DeleteStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<SetClause>> set_clauses;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    UpdateStmt(std::string tab_name_,
               std::vector<std::shared_ptr<SetClause>> set_clauses_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
    std::string left;
    std::string right;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    JoinType type;

    JoinExpr(std::string left_, std::string right_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_) :
            left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
};

struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<Col>> cols;
    std::vector<std::string> tabs;
    std::vector<std::shared_ptr<SelectItem>> select_items;
    std::vector<std::shared_ptr<TableRef>> table_refs;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    std::vector<std::shared_ptr<Col>> group_cols;
    std::vector<std::shared_ptr<HavingExpr>> having_conds;
    std::vector<std::shared_ptr<JoinExpr>> jointree;

    
    bool has_sort;
    std::shared_ptr<OrderBy> order;
    bool has_limit;
    int limit;
    bool is_semi_join;
    std::vector<std::shared_ptr<BinaryExpr>> semi_conds;


    SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
               std::vector<std::string> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::shared_ptr<OrderBy> order_) :
            cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)), 
            order(std::move(order_)) {
                has_sort = (bool)order;
                has_limit = false;
                limit = -1;
                is_semi_join = false;
            }

    SelectStmt(std::vector<std::shared_ptr<SelectItem>> select_items_,
               std::vector<std::shared_ptr<TableRef>> table_refs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::vector<std::shared_ptr<Col>> group_cols_,
               std::vector<std::shared_ptr<HavingExpr>> having_conds_,
               std::shared_ptr<OrderBy> order_,
               int limit_,
               bool is_semi_join_,
               std::vector<std::shared_ptr<BinaryExpr>> semi_conds_) :
            select_items(std::move(select_items_)), table_refs(std::move(table_refs_)), conds(std::move(conds_)),
            group_cols(std::move(group_cols_)), having_conds(std::move(having_conds_)), order(std::move(order_)),
            has_limit(limit_ >= 0), limit(limit_), is_semi_join(is_semi_join_),
            semi_conds(std::move(semi_conds_)) {
                has_sort = (bool)order;
                for (auto &ref : table_refs) {
                    tabs.push_back(ref->tab_name);
                }
                if (select_items.empty()) {
                    cols = {};
                } else {
                    for (auto &item : select_items) {
                        if (!item->is_agg && item->col) {
                            cols.push_back(item->col);
                        }
                    }
                }
            }
};

struct CreateCheckpoint : public TreeNode {
};

struct ExplainStmt : public TreeNode {
    std::shared_ptr<SelectStmt> select;

    ExplainStmt(std::shared_ptr<SelectStmt> select_) : select(std::move(select_)) {}
};

struct UnionStmt : public TreeNode {
    std::vector<std::shared_ptr<SelectStmt>> selects;
    std::string alias;
    bool has_sort;
    std::shared_ptr<OrderBy> order;

    UnionStmt(std::vector<std::shared_ptr<SelectStmt>> selects_, std::string alias_,
              std::shared_ptr<OrderBy> order_) :
            selects(std::move(selects_)), alias(std::move(alias_)), has_sort((bool)order_),
            order(std::move(order_)) {}
};

// set enable_nestloop
struct SetStmt : public TreeNode {
    SetKnobType set_knob_type_;
    bool bool_val_;

    SetStmt(SetKnobType &type, bool bool_value) : 
        set_knob_type_(type), bool_val_(bool_value) { }
};

// Semantic value
struct SemValue {
    int sv_int;
    float sv_float;
    std::string sv_str;
    bool sv_bool;
    OrderByDir sv_orderby_dir;
    std::vector<std::string> sv_strs;

    std::shared_ptr<TreeNode> sv_node;

    SvCompOp sv_comp_op;

    std::shared_ptr<TypeLen> sv_type_len;

    std::shared_ptr<Field> sv_field;
    std::vector<std::shared_ptr<Field>> sv_fields;

    std::shared_ptr<Expr> sv_expr;

    std::shared_ptr<Value> sv_val;
    std::vector<std::shared_ptr<Value>> sv_vals;

    std::shared_ptr<Col> sv_col;
    std::vector<std::shared_ptr<Col>> sv_cols;

    std::shared_ptr<SelectItem> sv_select_item;
    std::vector<std::shared_ptr<SelectItem>> sv_select_items;
    std::shared_ptr<SelectStmt> sv_select_stmt;
    std::vector<std::shared_ptr<SelectStmt>> sv_select_stmts;
    std::shared_ptr<TableRef> sv_table_ref;
    std::vector<std::shared_ptr<TableRef>> sv_table_refs;
    std::shared_ptr<FromClause> sv_from;
    std::shared_ptr<HavingExpr> sv_having;
    std::vector<std::shared_ptr<HavingExpr>> sv_havings;
    std::vector<std::shared_ptr<BinaryExpr>> sv_join_conds;
    std::shared_ptr<OrderByItem> sv_orderby_item;
    std::vector<std::shared_ptr<OrderByItem>> sv_orderby_items;
    AggType sv_agg_type;

    std::shared_ptr<SetClause> sv_set_clause;
    std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

    std::shared_ptr<BinaryExpr> sv_cond;
    std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

    std::shared_ptr<OrderBy> sv_orderby;

    SetKnobType sv_setKnobType;
};

}

#define YYSTYPE ast::SemValue


namespace rmdb {

enum class SqlTemplateLiteralType { kInt, kFloat, kString };

struct SqlTemplateLiteral {
    SqlTemplateLiteralType type = SqlTemplateLiteralType::kInt;
    int int_val = 0;
    float float_val = 0.0f;
    // Borrowed from the SQL statement buffer. SqlTemplateCandidate is statement-local
    // and must be consumed before that buffer is reused.
    std::string_view str_val;
};

struct SqlTemplateKey {
    rmdb::u64 h1 = 0;
    rmdb::u64 h2 = 0;
    rmdb::u32 normalized_len = 0;
    rmdb::u16 literal_count = 0;

    bool operator==(const SqlTemplateKey &other) const {
        return h1 == other.h1 && h2 == other.h2 && normalized_len == other.normalized_len &&
               literal_count == other.literal_count;
    }
};

struct SqlTemplateKeyHash {
    size_t operator()(const SqlTemplateKey &key) const {
        rmdb::u64 mixed = key.h1 ^ (key.h2 + 0x9e3779b97f4a7c15ULL + (key.h1 << 6) + (key.h1 >> 2));
        mixed ^= static_cast<rmdb::u64>(key.normalized_len) << 32;
        mixed ^= key.literal_count;
        return static_cast<size_t>(mixed);
    }
};

class SqlTemplateLiteralList {
   public:
    static constexpr size_t kInlineCapacity = 16;

    bool push_back(SqlTemplateLiteral literal) {
        if (size_ == std::numeric_limits<rmdb::u16>::max()) {
            return false;
        }
        size_t index = size_++;
        if (index < kInlineCapacity) {
            inline_literals_[index] = std::move(literal);
        } else {
            overflow_literals_.push_back(std::move(literal));
        }
        return true;
    }

    size_t size() const { return size_; }

    const SqlTemplateLiteral &operator[](size_t index) const {
        return index < kInlineCapacity ? inline_literals_[index] : overflow_literals_[index - kInlineCapacity];
    }

    SqlTemplateLiteral &operator[](size_t index) {
        return index < kInlineCapacity ? inline_literals_[index] : overflow_literals_[index - kInlineCapacity];
    }

   private:
    rmdb::u16 size_ = 0;
    std::array<SqlTemplateLiteral, kInlineCapacity> inline_literals_{};
    std::vector<SqlTemplateLiteral> overflow_literals_;
};

struct SqlTemplateCandidate {
    SqlTemplateKey key;
    SqlTemplateLiteralList literals;
};

namespace sql_template_detail {

inline std::atomic<rmdb::u64> &schema_epoch_storage() {
    static std::atomic<rmdb::u64> epoch{1};
    return epoch;
}

enum class TemplateKeywordClass {
    kNone,
    kSelect,
    kInsert,
    kUpdate,
    kDelete,
    kSet,
    kTransaction,
    kUnsupported,
    kOther,
};

constexpr unsigned char ascii_lower(unsigned char ch) {
    return ch >= 'A' && ch <= 'Z' ? static_cast<unsigned char>(ch + ('a' - 'A')) : ch;
}

constexpr bool ascii_space(unsigned char ch) {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
}

constexpr bool ascii_alpha(unsigned char ch) {
    ch = ascii_lower(ch);
    return ch >= 'a' && ch <= 'z';
}

constexpr bool ascii_digit(unsigned char ch) { return ch >= '0' && ch <= '9'; }

constexpr bool ascii_alnum(unsigned char ch) { return ascii_alpha(ch) || ascii_digit(ch); }

template <size_t N>
inline bool ascii_iequals(const char *text, size_t len, const char (&word)[N]) {
    if (len != N - 1) {
        return false;
    }
    for (size_t i = 0; i < len; ++i) {
        if (ascii_lower(static_cast<unsigned char>(text[i])) != static_cast<unsigned char>(word[i])) {
            return false;
        }
    }
    return true;
}

// This classifier is on every template-cache lookup. Keep it allocation-free,
// locale-independent, and bounded by token length instead of vocabulary size.
inline TemplateKeywordClass keyword_class(const char *text, size_t len) {
    switch (len) {
        case 2:
            if (ascii_iequals(text, len, "on") || ascii_iequals(text, len, "as") ||
                ascii_iequals(text, len, "by")) return TemplateKeywordClass::kOther;
            break;
        case 3:
            if (ascii_iequals(text, len, "set")) return TemplateKeywordClass::kSet;
            if (ascii_iequals(text, len, "and") || ascii_iequals(text, len, "asc") ||
                ascii_iequals(text, len, "max") || ascii_iequals(text, len, "min") ||
                ascii_iequals(text, len, "sum") || ascii_iequals(text, len, "avg") ||
                ascii_iequals(text, len, "int")) return TemplateKeywordClass::kOther;
            break;
        case 4:
            if (ascii_iequals(text, len, "drop") || ascii_iequals(text, len, "load") ||
                ascii_iequals(text, len, "show") ||
                ascii_iequals(text, len, "help") || ascii_iequals(text, len, "exit") ||
                ascii_iequals(text, len, "true")) return TemplateKeywordClass::kUnsupported;
            if (ascii_iequals(text, len, "into") || ascii_iequals(text, len, "from") ||
                ascii_iequals(text, len, "char") || ascii_iequals(text, len, "join") ||
                ascii_iequals(text, len, "semi") || ascii_iequals(text, len, "desc"))
                return TemplateKeywordClass::kOther;
            break;
        case 5:
            if (ascii_iequals(text, len, "begin") || ascii_iequals(text, len, "abort") ||
                ascii_iequals(text, len, "union") || ascii_iequals(text, len, "false")) return TemplateKeywordClass::kUnsupported;
            if (ascii_iequals(text, len, "table") || ascii_iequals(text, len, "where") ||
                ascii_iequals(text, len, "level") || ascii_iequals(text, len, "float") ||
                ascii_iequals(text, len, "index") || ascii_iequals(text, len, "group") ||
                ascii_iequals(text, len, "limit") || ascii_iequals(text, len, "order") ||
                ascii_iequals(text, len, "count")) return TemplateKeywordClass::kOther;
            break;
        case 6:
            if (ascii_iequals(text, len, "select")) return TemplateKeywordClass::kSelect;
            if (ascii_iequals(text, len, "insert")) return TemplateKeywordClass::kInsert;
            if (ascii_iequals(text, len, "update")) return TemplateKeywordClass::kUpdate;
            if (ascii_iequals(text, len, "delete")) return TemplateKeywordClass::kDelete;
            if (ascii_iequals(text, len, "create") || ascii_iequals(text, len, "commit")) return TemplateKeywordClass::kUnsupported;
            if (ascii_iequals(text, len, "tables") || ascii_iequals(text, len, "values") ||
                ascii_iequals(text, len, "having")) return TemplateKeywordClass::kOther;
            break;
        case 7:
            if (ascii_iequals(text, len, "explain") || ascii_iequals(text, len, "analyze")) return TemplateKeywordClass::kUnsupported;
            break;
        case 8:
            if (ascii_iequals(text, len, "rollback")) return TemplateKeywordClass::kUnsupported;
            if (ascii_iequals(text, len, "snapshot") || ascii_iequals(text, len, "datetime")) return TemplateKeywordClass::kOther;
            break;
        case 9:
            if (ascii_iequals(text, len, "isolation")) return TemplateKeywordClass::kOther;
            break;
        case 11:
            if (ascii_iequals(text, len, "transaction")) return TemplateKeywordClass::kTransaction;
            break;
        case 12:
            if (ascii_iequals(text, len, "serializable")) return TemplateKeywordClass::kOther;
            break;
        case 15:
            if (ascii_iequals(text, len, "enable_nestloop")) return TemplateKeywordClass::kOther;
            break;
        case 16:
            if (ascii_iequals(text, len, "enable_sortmerge")) return TemplateKeywordClass::kOther;
            break;
        case 17:
            if (ascii_iequals(text, len, "static_checkpoint")) return TemplateKeywordClass::kUnsupported;
            break;
        default: break;
    }
    return TemplateKeywordClass::kNone;
}

class TemplateKeyBuilder {
   public:
    void begin_token() {
        if (has_token_) {
            append_byte(0xff);
        }
        has_token_ = true;
    }

    void append_byte(unsigned char byte) {
        h1_ ^= byte;
        h1_ *= 1099511628211ULL;
        h2_ ^= static_cast<rmdb::u64>(byte) + 0x9e3779b97f4a7c15ULL + (h2_ << 6) + (h2_ >> 2);
        ++normalized_len_;
    }

    void append_token(const char *text, size_t len, bool lowercase) {
        begin_token();
        for (size_t i = 0; i < len; ++i) {
            unsigned char ch = static_cast<unsigned char>(text[i]);
            append_byte(lowercase ? ascii_lower(ch) : ch);
        }
    }

    void append_token(const char *text) {
        append_token(text, std::strlen(text), false);
    }

    SqlTemplateKey finish(size_t literal_count) const {
        SqlTemplateKey key;
        key.h1 = h1_;
        key.h2 = h2_;
        key.normalized_len = normalized_len_;
        key.literal_count = static_cast<rmdb::u16>(literal_count);
        return key;
    }

   private:
    rmdb::u64 h1_ = 1469598103934665603ULL;
    rmdb::u64 h2_ = 0x9e3779b97f4a7c15ULL;
    rmdb::u32 normalized_len_ = 0;
    bool has_token_ = false;
};

inline bool read_string_literal_raw(const char *sql, size_t *pos, std::string_view *value) {
    size_t i = *pos + 1;
    while (sql[i] != '\0' && sql[i] != '\'') {
        ++i;
    }
    if (sql[i] == '\0') {
        return false;
    }
    *value = std::string_view(sql + *pos + 1, i - *pos - 1);
    *pos = i + 1;
    return true;
}

inline bool read_numeric_literal_raw(const char *sql, size_t *pos, SqlTemplateLiteral *literal,
                                     const char **placeholder) {
    size_t start = *pos;
    size_t i = start;
    int int_value = 0;
    size_t integral_digits = 0;
    while (ascii_digit(static_cast<unsigned char>(sql[i]))) {
        int_value = int_value * 10 + (sql[i] - '0');
        ++i;
        ++integral_digits;
    }
    bool is_float = false;
    if (sql[i] == '.') {
        is_float = true;
        ++i;
        while (ascii_digit(static_cast<unsigned char>(sql[i]))) {
            ++i;
        }
    }
    if (integral_digits == 0 && i == start + 1) {
        return false;
    }
    if (sql[i] == 'e' || sql[i] == 'E') {
        is_float = true;
        ++i;
        if (sql[i] == '+' || sql[i] == '-') {
            ++i;
        }
        size_t exponent_start = i;
        while (ascii_digit(static_cast<unsigned char>(sql[i]))) {
            ++i;
        }
        if (i == exponent_start) {
            return false;
        }
    }
    if (is_float) {
        literal->type = SqlTemplateLiteralType::kFloat;
        char *end = nullptr;
        literal->float_val = std::strtof(sql + start, &end);
        if (end != sql + i) {
            return false;
        }
        *placeholder = "?f";
    } else {
        literal->type = SqlTemplateLiteralType::kInt;
        literal->int_val = int_value;
        *placeholder = "?i";
    }
    *pos = i;
    return true;
}

}  // namespace sql_template_detail

inline rmdb::u64 sql_template_schema_epoch() {
    return sql_template_detail::schema_epoch_storage().load(std::memory_order_acquire);
}

inline void bump_sql_template_schema_epoch() {
    sql_template_detail::schema_epoch_storage().fetch_add(1, std::memory_order_acq_rel);
}

inline std::optional<SqlTemplateCandidate> make_sql_template_candidate(const char *raw_sql) {
    if (raw_sql == nullptr) {
        return std::nullopt;
    }
    sql_template_detail::TemplateKeyBuilder key_builder;
    SqlTemplateCandidate candidate;
    bool saw_token = false;
    bool first_token_cacheable = false;
    bool unsupported = false;
    bool previous_set = false;
    size_t i = 0;
    while (raw_sql[i] != '\0') {
        unsigned char ch = static_cast<unsigned char>(raw_sql[i]);
        if (sql_template_detail::ascii_space(ch)) {
            ++i;
            continue;
        }
        if ((raw_sql[i] == '-' && raw_sql[i + 1] == '-') ||
            (raw_sql[i] == '/' && raw_sql[i + 1] == '*')) {
            return std::nullopt;
        }
        if (sql_template_detail::ascii_alpha(ch)) {
            size_t start = i++;
            while (raw_sql[i] != '\0') {
                unsigned char next = static_cast<unsigned char>(raw_sql[i]);
                if (!sql_template_detail::ascii_alnum(next) && raw_sql[i] != '_') {
                    break;
                }
                ++i;
            }
            size_t len = i - start;
            auto keyword = sql_template_detail::keyword_class(raw_sql + start, len);
            bool is_keyword = keyword != sql_template_detail::TemplateKeywordClass::kNone;
            if (!saw_token) {
                first_token_cacheable = keyword == sql_template_detail::TemplateKeywordClass::kSelect ||
                                        keyword == sql_template_detail::TemplateKeywordClass::kInsert ||
                                        keyword == sql_template_detail::TemplateKeywordClass::kUpdate ||
                                        keyword == sql_template_detail::TemplateKeywordClass::kDelete;
            }
            if (keyword == sql_template_detail::TemplateKeywordClass::kUnsupported ||
                (previous_set && keyword == sql_template_detail::TemplateKeywordClass::kTransaction)) {
                unsupported = true;
            }
            previous_set = keyword == sql_template_detail::TemplateKeywordClass::kSet;
            key_builder.append_token(raw_sql + start, len, is_keyword);
            saw_token = true;
            continue;
        }
        if (sql_template_detail::ascii_digit(ch) ||
            (raw_sql[i] == '.' && sql_template_detail::ascii_digit(static_cast<unsigned char>(raw_sql[i + 1])))) {
            SqlTemplateLiteral literal;
            const char *placeholder = nullptr;
            if (!sql_template_detail::read_numeric_literal_raw(raw_sql, &i, &literal, &placeholder) ||
                !candidate.literals.push_back(std::move(literal))) {
                return std::nullopt;
            }
            key_builder.append_token(placeholder);
            previous_set = false;
            saw_token = true;
            continue;
        }
        if (raw_sql[i] == '\'') {
            SqlTemplateLiteral literal;
            literal.type = SqlTemplateLiteralType::kString;
            if (!sql_template_detail::read_string_literal_raw(raw_sql, &i, &literal.str_val) ||
                !candidate.literals.push_back(std::move(literal))) {
                return std::nullopt;
            }
            key_builder.append_token("?s");
            previous_set = false;
            saw_token = true;
            continue;
        }
        if (raw_sql[i + 1] != '\0' &&
            ((raw_sql[i] == '>' && raw_sql[i + 1] == '=') ||
             (raw_sql[i] == '<' && (raw_sql[i + 1] == '=' || raw_sql[i + 1] == '>')))) {
            key_builder.append_token(raw_sql + i, 2, false);
            i += 2;
            previous_set = false;
            saw_token = true;
            continue;
        }
        if (raw_sql[i] == '!' && raw_sql[i + 1] == '=') {
            key_builder.append_token(raw_sql + i, 2, false);
            i += 2;
            previous_set = false;
            saw_token = true;
            continue;
        }
        switch (raw_sql[i]) {
            case ';': case '(': case ')': case ',': case '*': case '/': case '=': case '<':
            case '>': case '.': case '+': case '-':
            key_builder.append_token(raw_sql + i, 1, false);
            ++i;
            previous_set = false;
            saw_token = true;
            continue;
            default: break;
        }
        return std::nullopt;
    }
    if (!saw_token || !first_token_cacheable || unsupported ||
        candidate.literals.size() > std::numeric_limits<rmdb::u16>::max()) {
        return std::nullopt;
    }
    candidate.key = key_builder.finish(candidate.literals.size());
    return candidate;
}

}  // namespace rmdb


namespace rmdb::sql_template_detail {

enum class SlotKind { kValue, kLimitInt };

struct LiteralSlot {
    SlotKind kind = SlotKind::kValue;
    SqlTemplateLiteralType type = SqlTemplateLiteralType::kInt;
    int numeric_sign = 1;
};

inline void collect_value_slot(const std::shared_ptr<ast::Value> &value, std::vector<LiteralSlot> *slots) {
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(value)) {
        LiteralSlot slot;
        slot.type = SqlTemplateLiteralType::kInt;
        slot.numeric_sign = int_lit->val < 0 ? -1 : 1;
        slots->push_back(slot);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(value)) {
        LiteralSlot slot;
        slot.type = SqlTemplateLiteralType::kFloat;
        slot.numeric_sign = float_lit->val < 0.0f ? -1 : 1;
        slots->push_back(slot);
    } else if (std::dynamic_pointer_cast<ast::StringLit>(value)) {
        LiteralSlot slot;
        slot.type = SqlTemplateLiteralType::kString;
        slots->push_back(slot);
    }
}

inline bool has_bool_value(const std::shared_ptr<ast::Value> &value) {
    return std::dynamic_pointer_cast<ast::BoolLit>(value) != nullptr;
}

inline void collect_expr_slots(const std::shared_ptr<ast::Expr> &expr, std::vector<LiteralSlot> *slots) {
    if (auto value = std::dynamic_pointer_cast<ast::Value>(expr)) {
        collect_value_slot(value, slots);
    }
}

inline void collect_binary_slots(const std::vector<std::shared_ptr<ast::BinaryExpr>> &conds,
                                 std::vector<LiteralSlot> *slots) {
    for (const auto &cond : conds) {
        collect_expr_slots(cond->rhs, slots);
    }
}

inline void collect_having_slots(const std::vector<std::shared_ptr<ast::HavingExpr>> &conds,
                                 std::vector<LiteralSlot> *slots) {
    for (const auto &cond : conds) {
        collect_value_slot(cond->rhs, slots);
    }
}

inline void collect_select_slots(const std::shared_ptr<ast::SelectStmt> &select,
                                 std::vector<LiteralSlot> *slots) {
    collect_binary_slots(select->conds, slots);
    collect_having_slots(select->having_conds, slots);
    collect_binary_slots(select->semi_conds, slots);
    if (select->has_limit) {
        LiteralSlot slot;
        slot.kind = SlotKind::kLimitInt;
        slot.type = SqlTemplateLiteralType::kInt;
        slot.numeric_sign = select->limit < 0 ? -1 : 1;
        slots->push_back(slot);
    }
}

inline bool value_supported(const std::shared_ptr<ast::Value> &value) {
    return value != nullptr && !has_bool_value(value);
}

inline bool expr_supported(const std::shared_ptr<ast::Expr> &expr) {
    if (auto value = std::dynamic_pointer_cast<ast::Value>(expr)) {
        return value_supported(value);
    }
    return std::dynamic_pointer_cast<ast::Col>(expr) != nullptr;
}

inline bool binary_supported(const std::vector<std::shared_ptr<ast::BinaryExpr>> &conds) {
    for (const auto &cond : conds) {
        if (cond == nullptr || cond->lhs == nullptr || !expr_supported(cond->rhs)) {
            return false;
        }
    }
    return true;
}

inline bool having_supported(const std::vector<std::shared_ptr<ast::HavingExpr>> &conds) {
    for (const auto &cond : conds) {
        if (cond == nullptr || cond->lhs == nullptr || !value_supported(cond->rhs)) {
            return false;
        }
    }
    return true;
}

inline bool select_supported(const std::shared_ptr<ast::SelectStmt> &select) {
    if (select == nullptr || !binary_supported(select->conds) || !binary_supported(select->semi_conds) ||
        !having_supported(select->having_conds)) {
        return false;
    }
    for (const auto &ref : select->table_refs) {
        if (ref == nullptr || !ref->union_selects.empty()) {
            return false;
        }
    }
    return true;
}

inline bool root_supported(const std::shared_ptr<ast::TreeNode> &root) {
    if (auto insert = std::dynamic_pointer_cast<ast::InsertStmt>(root)) {
        return std::all_of(insert->vals.begin(), insert->vals.end(), value_supported);
    }
    if (auto update = std::dynamic_pointer_cast<ast::UpdateStmt>(root)) {
        if (!binary_supported(update->conds)) {
            return false;
        }
        for (const auto &set_clause : update->set_clauses) {
            if (set_clause == nullptr || (!set_clause->rhs_is_col && !value_supported(set_clause->val)) ||
                (set_clause->rhs_is_col && set_clause->op != ast::SET_OP_ASSIGN && !value_supported(set_clause->val))) {
                return false;
            }
        }
        return true;
    }
    if (auto del = std::dynamic_pointer_cast<ast::DeleteStmt>(root)) {
        return binary_supported(del->conds);
    }
    if (auto select = std::dynamic_pointer_cast<ast::SelectStmt>(root)) {
        return select_supported(select);
    }
    return false;
}

inline void collect_root_slots(const std::shared_ptr<ast::TreeNode> &root, std::vector<LiteralSlot> *slots) {
    if (auto insert = std::dynamic_pointer_cast<ast::InsertStmt>(root)) {
        for (const auto &value : insert->vals) {
            collect_value_slot(value, slots);
        }
    } else if (auto update = std::dynamic_pointer_cast<ast::UpdateStmt>(root)) {
        for (const auto &set_clause : update->set_clauses) {
            if (!set_clause->rhs_is_col || set_clause->op != ast::SET_OP_ASSIGN) {
                collect_value_slot(set_clause->val, slots);
            }
        }
        collect_binary_slots(update->conds, slots);
    } else if (auto del = std::dynamic_pointer_cast<ast::DeleteStmt>(root)) {
        collect_binary_slots(del->conds, slots);
    } else if (auto select = std::dynamic_pointer_cast<ast::SelectStmt>(root)) {
        collect_select_slots(select, slots);
    }
}

inline bool validate_slots(const std::vector<LiteralSlot> &slots, const SqlTemplateLiteralList &literals) {
    if (slots.size() != literals.size()) {
        return false;
    }
    for (size_t i = 0; i < slots.size(); ++i) {
        if (slots[i].type != literals[i].type) {
            return false;
        }
    }
    return true;
}

inline std::shared_ptr<ast::Col> clone_col(const std::shared_ptr<ast::Col> &col) {
    if (col == nullptr) {
        return nullptr;
    }
    return std::make_shared<ast::Col>(col->tab_name, col->col_name);
}

inline std::shared_ptr<ast::Value> clone_value(const std::shared_ptr<ast::Value> &value) {
    if (value == nullptr) {
        return nullptr;
    }
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(value)) {
        return std::make_shared<ast::IntLit>(int_lit->val);
    }
    if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(value)) {
        return std::make_shared<ast::FloatLit>(float_lit->val);
    }
    if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(value)) {
        return std::make_shared<ast::StringLit>(str_lit->val);
    }
    if (auto bool_lit = std::dynamic_pointer_cast<ast::BoolLit>(value)) {
        return std::make_shared<ast::BoolLit>(bool_lit->val);
    }
    return nullptr;
}

inline std::shared_ptr<ast::SelectItem> clone_select_item(const std::shared_ptr<ast::SelectItem> &item) {
    if (item == nullptr) {
        return nullptr;
    }
    if (item->is_agg) {
        return std::make_shared<ast::SelectItem>(item->agg_type, clone_col(item->col), item->count_star, item->alias,
                                                 item->distinct);
    }
    return std::make_shared<ast::SelectItem>(clone_col(item->col), item->alias);
}

inline std::shared_ptr<ast::HavingExpr> clone_having_expr(const std::shared_ptr<ast::HavingExpr> &expr) {
    return std::make_shared<ast::HavingExpr>(clone_select_item(expr->lhs), expr->op, clone_value(expr->rhs));
}

inline std::vector<std::shared_ptr<ast::HavingExpr>> clone_having_exprs(
    const std::vector<std::shared_ptr<ast::HavingExpr>> &exprs) {
    std::vector<std::shared_ptr<ast::HavingExpr>> result;
    result.reserve(exprs.size());
    for (const auto &expr : exprs) {
        result.push_back(clone_having_expr(expr));
    }
    return result;
}

inline std::vector<std::shared_ptr<ast::TableRef>> clone_table_refs(
    const std::vector<std::shared_ptr<ast::TableRef>> &refs) {
    std::vector<std::shared_ptr<ast::TableRef>> result;
    result.reserve(refs.size());
    for (const auto &ref : refs) {
        result.push_back(std::make_shared<ast::TableRef>(ref->tab_name, ref->alias));
    }
    return result;
}

}  // namespace rmdb::sql_template_detail
