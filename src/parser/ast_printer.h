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

#include "ast.h"
#include <cassert>
#include <iostream>
#include <string_view>

namespace ast {

class TreePrinter {
public:
    static void print(const std::shared_ptr<TreeNode> &node) {
        print(std::cout, node);
    }

    static void print(std::ostream &output, const std::shared_ptr<TreeNode> &node) {
        TreePrinter printer(output);
        printer.print_node(node, 0);
    }

private:
    static constexpr int kIndentStep = 2;

    explicit TreePrinter(std::ostream &output) : output_(output) {}

    static std::string offset2string(int offset) {
        return std::string(offset, ' ');
    }

    template<typename T>
    void print_val(const T &val, int offset) {
        output_ << offset2string(offset) << val << '\n';
    }

    template<typename T>
    void print_val_list(const std::vector<T> &vals, int offset) {
        output_ << offset2string(offset) << "LIST\n";
        offset += kIndentStep;
        for (const auto &val : vals) {
            print_val(val, offset);
        }
    }

    static std::string_view type2str(SvType type) {
        switch (type) {
            case SV_TYPE_INT: return "INT";
            case SV_TYPE_FLOAT: return "FLOAT";
            case SV_TYPE_STRING: return "STRING";
            case SV_TYPE_BOOL: return "BOOL";
        }
        assert(false);
        return "UNKNOWN";
    }

    static std::string_view op2str(SvCompOp op) {
        switch (op) {
            case SV_OP_EQ: return "==";
            case SV_OP_NE: return "!=";
            case SV_OP_LT: return "<";
            case SV_OP_GT: return ">";
            case SV_OP_LE: return "<=";
            case SV_OP_GE: return ">=";
        }
        assert(false);
        return "?";
    }

    static std::string_view set_op_name(SetClauseOp op) {
        switch (op) {
            case SET_OP_ASSIGN: return "ASSIGN";
            case SET_OP_ADD: return "ADD";
            case SET_OP_SUB: return "SUB";
            case SET_OP_MUL: return "MUL";
            case SET_OP_DIV: return "DIV";
        }
        assert(false);
        return "UNKNOWN";
    }

    template<typename T>
    void print_node_list(const std::vector<T> &nodes, int offset) {
        output_ << offset2string(offset);
        offset += kIndentStep;
        output_ << "LIST\n";
        for (const auto &node : nodes) {
            print_node(node, offset);
        }
    }

    void print_node(const std::shared_ptr<TreeNode> &node, int offset) {
        output_ << offset2string(offset);
        offset += kIndentStep;
        if (auto x = std::dynamic_pointer_cast<Help>(node)) {
            output_ << "HELP\n";
        } else if (auto x = std::dynamic_pointer_cast<ShowTables>(node)) {
            output_ << "SHOW_TABLES\n";
        } else if (auto x = std::dynamic_pointer_cast<ShowIndex>(node)) {
            output_ << "SHOW_INDEX\n";
            print_val(x->tab_name, offset);
        } else if (auto x = std::dynamic_pointer_cast<CreateTable>(node)) {
            output_ << "CREATE_TABLE\n";
            print_val(x->tab_name, offset);
            print_node_list(x->fields, offset);
        } else if (auto x = std::dynamic_pointer_cast<DropTable>(node)) {
            output_ << "DROP_TABLE\n";
            print_val(x->tab_name, offset);
        } else if (auto x = std::dynamic_pointer_cast<DescTable>(node)) {
            output_ << "DESC_TABLE\n";
            print_val(x->tab_name, offset);
        } else if (auto x = std::dynamic_pointer_cast<CreateIndex>(node)) {
            output_ << "CREATE_INDEX\n";
            print_val(x->tab_name, offset);
            for (const auto &col_name : x->col_names) {
                print_val(col_name, offset);
            }
        } else if (auto x = std::dynamic_pointer_cast<DropIndex>(node)) {
            output_ << "DROP_INDEX\n";
            print_val(x->tab_name, offset);
            for (const auto &col_name : x->col_names) {
                print_val(col_name, offset);
            }
        } else if (auto x = std::dynamic_pointer_cast<LoadStmt>(node)) {
            output_ << "LOAD\n";
            print_val(x->file_name, offset);
            print_val(x->tab_name, offset);
        } else if (auto x = std::dynamic_pointer_cast<ColDef>(node)) {
            output_ << "COL_DEF\n";
            print_val(x->col_name, offset);
            print_node(x->type_len, offset);
        } else if (auto x = std::dynamic_pointer_cast<Col>(node)) {
            output_ << "COL\n";
            print_val(x->tab_name, offset);
            print_val(x->col_name, offset);
        } else if (auto x = std::dynamic_pointer_cast<TypeLen>(node)) {
            output_ << "TYPE_LEN\n";
            print_val(type2str(x->type), offset);
            print_val(x->len, offset);
        } else if (auto x = std::dynamic_pointer_cast<IntLit>(node)) {
            output_ << "INT_LIT\n";
            print_val(x->val, offset);
        } else if (auto x = std::dynamic_pointer_cast<FloatLit>(node)) {
            output_ << "FLOAT_LIT\n";
            print_val(x->val, offset);
        } else if (auto x = std::dynamic_pointer_cast<StringLit>(node)) {
            output_ << "STRING_LIT\n";
            print_val(x->val, offset);
        } else if (auto x = std::dynamic_pointer_cast<SetClause>(node)) {
            output_ << "SET_CLAUSE\n";
            print_val(x->col_name, offset);
            print_val(x->rhs_is_col ? x->rhs_col_name : std::string("<value>"), offset);
            print_val(set_op_name(x->op), offset);
            if (x->val != nullptr) {
                print_node(x->val, offset);
            }
        } else if (auto x = std::dynamic_pointer_cast<BinaryExpr>(node)) {
            output_ << "BINARY_EXPR\n";
            print_node(x->lhs, offset);
            print_val(op2str(x->op), offset);
            print_node(x->rhs, offset);
        } else if (auto x = std::dynamic_pointer_cast<InsertStmt>(node)) {
            output_ << "INSERT\n";
            print_val(x->tab_name, offset);
            print_node_list(x->vals, offset);
        } else if (auto x = std::dynamic_pointer_cast<DeleteStmt>(node)) {
            output_ << "DELETE\n";
            print_val(x->tab_name, offset);
            print_node_list(x->conds, offset);
        } else if (auto x = std::dynamic_pointer_cast<UpdateStmt>(node)) {
            output_ << "UPDATE\n";
            print_val(x->tab_name, offset);
            print_node_list(x->set_clauses, offset);
            print_node_list(x->conds, offset);
        } else if (auto x = std::dynamic_pointer_cast<SelectStmt>(node)) {
            output_ << "SELECT\n";
            print_node_list(x->cols, offset);
            print_val_list(x->tabs, offset);
            print_node_list(x->conds, offset);
        } else if (auto x = std::dynamic_pointer_cast<ExplainStmt>(node)) {
            output_ << "EXPLAIN\n";
            print_node(x->select, offset);
        } else if (auto x = std::dynamic_pointer_cast<TxnBegin>(node)) {
            output_ << "BEGIN\n";
        } else if (auto x = std::dynamic_pointer_cast<TxnCommit>(node)) {
            output_ << "COMMIT\n";
        } else if (auto x = std::dynamic_pointer_cast<TxnAbort>(node)) {
            output_ << "ABORT\n";
        } else if (auto x = std::dynamic_pointer_cast<TxnRollback>(node)) {
            output_ << "ROLLBACK\n";
        } else {
            assert(0);
        }
    }

    std::ostream &output_;
};

}
