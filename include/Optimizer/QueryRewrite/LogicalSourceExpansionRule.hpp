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

#ifndef NES_LOGICALSOURCEEXPANSIONRULE_HPP
#define NES_LOGICALSOURCEEXPANSIONRULE_HPP

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <memory>
#include <set>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class LogicalSourceExpansionRule;
typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

/**
 * @brief This class will expand the logical query graph by adding information about the physical sources and expand the
 * graph by adding additional replicated operators.
 *
 * Note: Apart from replicating the logical source operator we also replicate its down stream operators till we
 * encounter first n-ary operator or we encounter sink operator.
 *
 * Example: a query :                       Sink
 *                                           |
 *                                           Map
 *                                           |
 *                                        Source(Car)
 *
 * will be expanded to:                            Sink
 *                                                /     \
 *                                              /        \
 *                                           Map1        Map2
 *                                           |             |
 *                                      Source(Car1)    Source(Car2)
 *
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2
 *
 *                                                     or
 *
 * a query :                                    Sink
 *                                               |
 *                                             Merge
 *                                             /   \
 *                                            /    Filter
 *                                          /        |
 *                                   Source(Car)   Source(Truck)
 *
 * will be expanded to:                             Sink
 *                                                   |
 *                                                 Merge
 *                                              /  / \   \
 *                                            /  /    \    \
 *                                         /   /       \     \
 *                                       /   /          \      \
 *                                    /    /         Filter1   Filter2
 *                                 /     /               \         \
 *                               /      /                 \          \
 *                         Src(Car1) Src(Car2)       Src(Truck1)  Src(Truck2)
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2 and
 * logical source truck has two physical sources: i.e. truck1 and truck2
 *
 */
class LogicalSourceExpansionRule : public BaseRewriteRule {
  public:
    static LogicalSourceExpansionRulePtr create(StreamCatalogPtr);

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    explicit LogicalSourceExpansionRule(StreamCatalogPtr);
    StreamCatalogPtr streamCatalog;

    /**
     * @brief Returns the duplicated operator and its chain of upstream operators till first n-ary or sink operator is found
     * @param operatorNode : start operator
     * @return a tuple of duplicated operator with its duplicated downstream operator chains and a vector of original head operators
     */
    std::tuple<OperatorNodePtr, std::set<OperatorNodePtr>> getLogicalGraphToDuplicate(OperatorNodePtr operatorNode);
};
}// namespace NES
#endif//NES_LOGICALSOURCEEXPANSIONRULE_HPP
