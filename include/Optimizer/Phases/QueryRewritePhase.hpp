/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_QUERYREWRITEPHASE_HPP
#define NES_QUERYREWRITEPHASE_HPP

#include <memory>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;
}// namespace NES

namespace NES::Optimizer {

class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

class FilterPushDownRule;
typedef std::shared_ptr<FilterPushDownRule> FilterPushDownRulePtr;

class RenameStreamToProjectOperatorRule;
typedef std::shared_ptr<RenameStreamToProjectOperatorRule> RenameStreamToProjectOperatorRulePtr;

class ProjectBeforeUnionOperatorRule;
typedef std::shared_ptr<ProjectBeforeUnionOperatorRule> ProjectBeforeUnionOperatorRulePtr;

class AttributeSortRule;
typedef std::shared_ptr<AttributeSortRule> AttributeSortRulePtr;

class BinaryOperatorSortRule;
typedef std::shared_ptr<BinaryOperatorSortRule> BinaryOperatorSortRulePtr;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase {
  public:
    static QueryRewritePhasePtr create(bool applyRulesImprovingSharingIdentification);

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

    ~QueryRewritePhase();

  private:
    explicit QueryRewritePhase(bool applyRulesImprovingSharingIdentification);
    bool applyRulesImprovingSharingIdentification;
    FilterPushDownRulePtr filterPushDownRule;
    RenameStreamToProjectOperatorRulePtr renameStreamToProjectOperatorRule;
    ProjectBeforeUnionOperatorRulePtr projectBeforeUnionOperatorRule;
    AttributeSortRulePtr attributeSortRule;
    BinaryOperatorSortRulePtr binaryOperatorSortRule;
};
}// namespace NES::Optimizer
#endif//NES_QUERYREWRITEPHASE_HPP
