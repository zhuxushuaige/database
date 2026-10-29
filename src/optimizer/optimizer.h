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

#include <map>

#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "system/sm.h"
#include "common/context.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"

class Optimizer {
   private:
    Planner *planner_;

    static auto make_other_plan(PlanTag tag, std::string argument = {}) -> std::shared_ptr<Plan> {
        return std::make_shared<OtherPlan>(tag, std::move(argument));
    }

    static auto plan_control_statement(const std::shared_ptr<ast::TreeNode> &node) -> std::shared_ptr<Plan> {
        if (std::dynamic_pointer_cast<ast::Help>(node) != nullptr) {
            return make_other_plan(T_Help);
        }
        if (std::dynamic_pointer_cast<ast::ShowTables>(node) != nullptr) {
            return make_other_plan(T_ShowTable);
        }
        if (auto show_index = std::dynamic_pointer_cast<ast::ShowIndex>(node)) {
            return make_other_plan(T_ShowIndex, show_index->tab_name);
        }
        if (auto desc_table = std::dynamic_pointer_cast<ast::DescTable>(node)) {
            return make_other_plan(T_DescTable, desc_table->tab_name);
        }
        if (std::dynamic_pointer_cast<ast::TxnBegin>(node) != nullptr) {
            return make_other_plan(T_Transaction_begin);
        }
        if (std::dynamic_pointer_cast<ast::TxnAbort>(node) != nullptr) {
            return make_other_plan(T_Transaction_abort);
        }
        if (std::dynamic_pointer_cast<ast::TxnCommit>(node) != nullptr) {
            return make_other_plan(T_Transaction_commit);
        }
        if (std::dynamic_pointer_cast<ast::TxnRollback>(node) != nullptr) {
            return make_other_plan(T_Transaction_rollback);
        }
        if (auto isolation = std::dynamic_pointer_cast<ast::SetTransactionIsolation>(node)) {
            return std::make_shared<SetTransactionIsolationPlan>(
                isolation->serializable ? IsolationLevel::SERIALIZABLE : IsolationLevel::SNAPSHOT_ISOLATION);
        }
        if (auto set_statement = std::dynamic_pointer_cast<ast::SetStmt>(node)) {
            return std::make_shared<SetKnobPlan>(set_statement->set_knob_type_, set_statement->bool_val_);
        }
        if (std::dynamic_pointer_cast<ast::CreateCheckpoint>(node) != nullptr) {
            return make_other_plan(T_Checkpoint);
        }
        return nullptr;
    }

   public:
    explicit Optimizer(Planner *planner) : planner_(planner) {}
    
    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
        auto control_plan = plan_control_statement(query->parse);
        if (control_plan != nullptr) {
            return control_plan;
        }
        return planner_->do_planner(query, context);
    }

};
