/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// 定义了 Portal 阶段的入口，负责把 Plan 转换成 Executor 并调度执行。
#pragma once

#include <cerrno>
#include <cstring>
#include <string>
#include "optimizer/plan.h"
#include "execution/executor_abstract.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_update.h"
#include "execution/executor_insert.h"
#include "execution/executor_delete.h"
#include "execution/execution_sort.h"
#include "common/common.h"

/*
 * ======================== Portal：执行阶段入口 ========================
 *
 * Portal 位于 Optimizer 和 Executor 之间：
 *
 *     Optimizer 生成 Plan
 *             ↓
 *     Portal 判断语句类型，并把 Plan 转换成 Executor
 *             ↓
 *     QlManager / Executor 真正执行数据库操作
 *
 * Parser 负责“读懂 SQL 字符串”，Portal 则负责“落实执行计划”，
 * 两者处于 SQL 生命周期的不同阶段。
 *
 * 本文件的三个关键部分：
 *
 *   portalTag              ：区分不同语句的执行方式；
 *   PortalStmt             ：保存一条即将执行的语句；
 *   Portal::start/run      ：创建执行器并调度执行。
 * ==================================================================
 */
// portalTag 定义SQL语句的类型，帮助 Portal::run 决定采用哪种执行方式。
typedef enum portalTag{
    // 无效或尚未识别的查询类型。
    PORTAL_Invalid_Query = 0,/// 无效或者尚未识别的查询类型
    PORTAL_ONE_SELECT,// 普通的SELECT查询
    PORTAL_EXPLAIN_ANALYZE, // 执行查询并返回计划和统计信息
    PORTAL_AGGREGATE_SELECT, // 含 COUNT、MAX 等聚合函数的 SELECT
    PORTAL_DML_WITHOUT_SELECT,// INSERT、UPDATE、DELETE 等不直接返回结果集的 DML
    PORTAL_MULTI_QUERY,// CREATE TABLE、DROP TABLE、CREATE INDEX 等可能包含多个内部步骤的操作
    PORTAL_CMD_UTILITY //BEGIN、COMMIT、SET 等工具或会话命令
} portalTag;

/*
类型名称：PortalStmt
类型作用：Portal 阶段的一条“可执行语句描述”，包含分类信息、输出列、执行器树和原始计划
*/
struct PortalStmt {
    // 当前语句所属的 Portal 类型。
    portalTag tag;
    // SELECT 最终需要返回给客户端的列。
    std::vector<TabCol> sel_cols;
    // 执行器树的根节点；从根节点运行即可驱动整棵执行器树。
    std::unique_ptr<AbstractExecutor> root;
    // 保留原始计划，聚合、EXPLAIN、DDL 和工具命令执行时仍会使用。
    std::shared_ptr<Plan> plan;
    // 构造函数
    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) :
            tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

/*
类型名称：Portal
类型作用：Portal 阶段的执行器创建和调度入口
*/
class Portal
{
   private:
    // Portal 创建执行器时需要通过 SmManager 取得表、记录文件和索引等信息。
    SmManager *sm_manager_;


   public:
    // SmManager 的生命周期由服务端统一管理，Portal 这里只保存普通指针，不负责释放。
    Portal(SmManager *sm_manager) : sm_manager_(sm_manager){}
    ~Portal(){}
    /*
    函数名称：start
    函数作用：根据 Plan 的真实类型创建 PortalStmt
    函数参数：
        plan：shared_ptr<Plan> 指向基类 Plan，dynamic_pointer_cast 用于判断它实际是
              AggregatePlan、DDLPlan、DMLPlan 等哪一种派生计划。
        context：当前会话的上下文信息，包含事务、锁和执行环境等。
    函数返回值：返回一个 shared_ptr<PortalStmt>，表示即将执行的语句描述
    */
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context)
    {
        // 聚合查询由 QlManager::select_aggregate 根据 AggregatePlan 单独执行。
        if (auto x = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
            std::unique_ptr<AbstractExecutor> input;
            if (x->subplan_ != nullptr) {
                input = convert_plan_executor(x->subplan_, context);
            }
            return std::make_shared<PortalStmt>(PORTAL_AGGREGATE_SELECT, std::vector<TabCol>(),
                                                std::move(input), plan);
        // EXPLAIN ANALYZE 需要保留查询对象和计划，用于执行并输出分析结果。
        } else if (auto x = std::dynamic_pointer_cast<ExplainAnalyzePlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_EXPLAIN_ANALYZE, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
        // OtherPlan 表示事务控制、SHOW 等工具类命令。
        } else if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(),plan);
        // SET KNOB 修改优化器或系统运行参数，也属于工具命令。
        } else if(auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
        // 修改会话隔离级别，不需要构建普通记录执行器。
        } else if(auto x = std::dynamic_pointer_cast<SetIsolationLevelPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
        // DDLPlan 表示建表、删表、创建索引等元数据操作。
        } else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(),plan);
        // DMLPlan 表示 SELECT、INSERT、UPDATE、DELETE。
        } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch(x->tag) {
                case T_Load:
                {
                    return std::make_shared<PortalStmt>(
                        PORTAL_MULTI_QUERY, std::vector<TabCol>(),
                        std::unique_ptr<AbstractExecutor>(), plan);
                }
                case T_select:
                {
                    /*
                     * SELECT 的顶层计划应为 ProjectionPlan。
                     * convert_plan_executor 会递归创建 Projection、Scan、Join、Sort 等执行器，
                     * root 最终指向整棵执行器树的根节点。
                     */
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::unique_ptr<AbstractExecutor> root= convert_plan_executor(p, context);
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
                }

                case T_Update:
                {
                    /*
                     * UPDATE 分两步：
                     *   1. 先运行扫描子计划，收集满足 WHERE 条件的所有 Rid；
                     *   2. 把 Rid 列表交给 UpdateExecutor，统一修改这些记录。
                     */
                    std::unique_ptr<AbstractExecutor> scan= convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                    std::unique_ptr<AbstractExecutor> root =std::make_unique<UpdateExecutor>(sm_manager_,
                                                            x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Delete:
                {
                    /*
                     * DELETE 同样先扫描并收集目标 Rid，再创建 DeleteExecutor。
                     * 这样删除阶段明确知道需要删除哪些物理记录。
                     */
                    std::unique_ptr<AbstractExecutor> scan= convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }

                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }

                case T_Insert:
                {
                    // INSERT 不需要先扫描旧记录，直接根据表名和值创建 InsertExecutor。
                    std::unique_ptr<AbstractExecutor> root =
                            std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }


                default:
                    // DMLPlan 出现了 Portal 不认识的 tag，说明程序内部状态异常。
                    throw InternalError("Unexpected field type");
                    break;
            }
        } else {
            // 传入了所有已知 Plan 类型之外的对象。
            throw InternalError("Unexpected field type");
        }
        // 正常分支都会提前 return；该返回值只用于满足函数的完整返回路径。
        return nullptr;
    }

    /*
    函数名称：run
    函数作用：根据 PortalStmt 的 tag 调用 QlManager 的不同执行接口
    函数参数：
        portal：shared_ptr<PortalStmt>，表示即将执行的语句描述
        ql：QlManager 指针，负责调度执行器和管理事务
        txn_id：指向当前事务 ID 的指针，可能在工具命令中被修改
        context：当前会话的上下文信息，包含事务、锁和执行环境等
    函数返回值：无返回值，执行结果通过 Context 传回客户端
    */
    void run(std::shared_ptr<PortalStmt> portal, QlManager* ql, txn_id_t *txn_id, Context *context){
        switch(portal->tag) {
            case PORTAL_ONE_SELECT:
            {
                // 普通 SELECT：遍历执行器树，并格式化输出指定列。
                ql->select_from(std::move(portal->root), std::move(portal->sel_cols), context);
                break;
            }

            case PORTAL_DML_WITHOUT_SELECT:
            {
                // INSERT/UPDATE/DELETE：运行写执行器，不返回普通结果集。
                ql->run_dml(std::move(portal->root));
                break;
            }
            case PORTAL_EXPLAIN_ANALYZE:
            {
                // 取回具体计划中的 Query，执行并输出计划分析信息。
                auto explain_plan = std::dynamic_pointer_cast<ExplainAnalyzePlan>(portal->plan);
                ql->explain_analyze(explain_plan->query_, context);
                break;
            }
            case PORTAL_AGGREGATE_SELECT:
            {
                // 聚合 SELECT 由专用接口处理 COUNT、MAX 等计算。
                auto aggregate_plan = std::dynamic_pointer_cast<AggregatePlan>(portal->plan);
                ql->select_aggregate(aggregate_plan->query_, std::move(portal->root), context);
                break;
            }
            case PORTAL_MULTI_QUERY:
            {
                // DDL 等操作可能拆成多个内部步骤，由 QlManager 统一调度。
                ql->run_mutli_query(portal->plan, context);
                break;
            }
            case PORTAL_CMD_UTILITY:
            {
                // 工具命令可能改变 txn_id、事务状态或会话配置。
                ql->run_cmd_utility(portal->plan, txn_id, context);
                break;
            }
            default:
            {
                // PortalStmt 的 tag 无效，属于程序内部错误。
                throw InternalError("Unexpected field type");
            }
        }
    }

    // 预留的资源释放接口
    void drop(){}

    /*
    函数名称：convert_plan_executor
    函数作用：递归地把 Plan 树转换成 Executor 树
    函数参数：
        plan：shared_ptr<Plan>，表示当前计划节点
        context：Context*，当前会话的上下文信息
    函数返回值：unique_ptr<AbstractExecutor>，表示当前计划节点对应的执行器
    */
    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context)
    {
        // ProjectionExecutor 先运行子执行器，再从记录中保留需要输出的列。
        if(auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)){
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context),
                                                        x->sel_cols_);
        // ScanPlan 根据 tag 选择顺序扫描或索引扫描。
        } else if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            if(x->tag == T_SeqScan) {
                return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            }
            else {
                return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_col_names_, context);
            }
        // JoinPlan 先递归创建左右子执行器，再创建连接执行器。
        } else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            // 优化器选择了索引连接时，使用 IndexNestedLoopJoinExecutor。
            if (x->use_index_join_) {
                return std::make_unique<IndexNestedLoopJoinExecutor>(
                            std::move(left), std::move(right), std::move(x->conds_), x->index_join_cond_);
            }
            // 否则使用普通嵌套循环连接。
            std::unique_ptr<AbstractExecutor> join = std::make_unique<NestedLoopJoinExecutor>(
                                std::move(left),
                                std::move(right), std::move(x->conds_));
            return join;
        // SortExecutor 先取得子执行器结果，再按指定列进行升序或降序排列。
        } else if(auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context),
                                            x->sel_col_, x->is_desc_);
        }
        // 没有匹配到已支持的计划类型时，返回空执行器。
        return nullptr;
    }

};
