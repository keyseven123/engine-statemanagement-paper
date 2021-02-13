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

#ifndef NES_SYNTAXBASEDEQUALQUERYMERGERRULE_HPP
#define NES_SYNTAXBASEDEQUALQUERYMERGERRULE_HPP

#include <map>
#include <memory>
#include <vector>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class GlobalQueryNode;
typedef std::shared_ptr<GlobalQueryNode> GlobalQueryNodePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class SyntaxBasedEqualQueryMergerRule;
typedef std::shared_ptr<SyntaxBasedEqualQueryMergerRule> SyntaxBasedEqualQueryMergerRulePtr;

/**
 * @brief SyntaxBasedEqualQueryMergerRule is responsible for merging together all the equivalent chains of Global Query Nodes within the Global Query Plan such that, after running this rule
 * all equivalent operator chains should be merged together and only a single representative operator chain should exists in the Global Query Plan for all of them.
 * Effectively this will prune the global query plan size.
 *
 * Following are the conditions for the two global query node chains to be equivalent:
 *  - For each global query node in the first chain, there should exists an equal global query node in the other chain (except for the node with the sink operator).
 *      - For two global query nodes to be equal, we check that for each logical operator in one global query node their is an equivalent logical operator in the other
 *      global query node.
 *  - The order of global query nodes in both the chains should be same.
 *
 * Following is the example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map1},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * After running the SyntaxBasedEqualQueryMergerRule, the resulting Global Query Plan will look as follow:
 *
 *                                                         GQPRoot
 *                                                         /     \
 *                                                        /       \
 *                                           GQN1({Sink1},{Q1}) GQN5({Sink2},{Q2})
 *                                                        \      /
 *                                                         \   /
 *                                                   GQN2({Map1},{Q1,Q2})
 *                                                           |
 *                                                  GQN3({Filter1},{Q1,Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 */
class SyntaxBasedEqualQueryMergerRule {

  public:
    static SyntaxBasedEqualQueryMergerRulePtr create();

    /**
     * @brief apply L0QueryMerger rule on the globalQuery plan
     * @param globalQueryPlan: the global query plan
     * @return true if successful else false
     */
    bool apply(const GlobalQueryPlanPtr& globalQueryPlan);

  private:
    explicit SyntaxBasedEqualQueryMergerRule();

    /**
     * @brief identify if the query plans are equal or not
     * @param targetQueryPlan : target query plan
     * @param hostQueryPlan : address query plan
     * @param targetHostOperatorMap: map passed as reference to record the matching pair of target and address operators
     * @return true if the query plans are equal
     */
    bool areQueryPlansEqual(QueryPlanPtr targetQueryPlan, QueryPlanPtr hostQueryPlan,
                            std::map<uint64_t, uint64_t>& targetHostOperatorMap);

    bool areOperatorEqual(OperatorNodePtr targetOperator, OperatorNodePtr hostOperator,
                          std::map<uint64_t, uint64_t>& targetHostOperatorMap);

    /**
     * @brief Check if the target GQN can be merged into address GQN
     * For two GQNs to be merged:
     *   - Their set of operators should be equal or they both have sink operators
     *   - Their child and parent GQN nodes should be equal
     * @param targetGQNode : the target GQN node
     * @param hostGQNode : the address GQN node
     * @param targetGQNToHostGQNMap : the map containing list of target and address pairs with eaqual operator sets
     * @return true if the GQN can be merged else false
     */
    bool checkIfGQNCanMerge(const GlobalQueryNodePtr& targetGQNode, const GlobalQueryNodePtr& hostGQNode,
                            std::map<GlobalQueryNodePtr, GlobalQueryNodePtr>& targetGQNToHostGQNMap);

    /**
     * @brief Check if the two set of GQNs are equal
     * @param targetGQNs : the target GQNs
     * @param hostGQNs : the source GQNs
     * @return false if not equal else true
     */
    bool areGQNodesEqual(const std::vector<NodePtr>& targetGQNs, const std::vector<NodePtr>& hostGQNs);

    std::vector<GlobalQueryNodePtr> processedNodes;
};
}// namespace NES
#endif//NES_SYNTAXBASEDEQUALQUERYMERGERRULE_HPP
