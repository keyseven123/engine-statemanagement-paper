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

#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>

namespace NES {

LogicalSourceExpansionRule::LogicalSourceExpansionRule(StreamCatalogPtr streamCatalog) : streamCatalog(streamCatalog) {}

LogicalSourceExpansionRulePtr LogicalSourceExpansionRule::create(StreamCatalogPtr streamCatalog) {
    return std::make_shared<LogicalSourceExpansionRule>(LogicalSourceExpansionRule(streamCatalog));
}

QueryPlanPtr LogicalSourceExpansionRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("LogicalSourceExpansionRule: Apply Logical source expansion rule for the query.");
    NES_DEBUG("LogicalSourceExpansionRule: Get all logical source operators in the query for plan=" << queryPlan->toString());

    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    for (auto& sourceOperator : sourceOperators) {
        SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
        NES_DEBUG("LogicalSourceExpansionRule: Get the number of physical source locations in the topology.");
        std::vector<TopologyNodePtr> sourceLocations =
            streamCatalog->getSourceNodesForLogicalStream(sourceDescriptor->getStreamName());

        if (sourceLocations.size() > 1) {
            NES_DEBUG("LogicalSourceExpansionRule: Found " << sourceLocations.size()
                                                           << " physical source locations in the topology.");
            OperatorNodePtr operatorNode;
            std::set<OperatorNodePtr> originalRootOperators;

            NES_TRACE("LogicalSourceExpansionRule: Find the logical sub-graph to be duplicated for the source operator.");

            std::tie(operatorNode, originalRootOperators) = getLogicalGraphToDuplicate(sourceOperator);
            NES_TRACE("LogicalSourceExpansionRule: Create " << sourceLocations.size() - 1
                                                            << " duplicated logical sub-graph and add to original graph");

            for (uint32_t i = 0; i < sourceLocations.size() - 1; i++) {
                NES_TRACE("LogicalSourceExpansionRule: Create duplicated logical sub-graph");
                OperatorNodePtr copyOfGraph = operatorNode->duplicate();
                NES_TRACE("LogicalSourceExpansionRule: Get all root nodes of the duplicated logical sub-graph to connect with "
                          "original graph");
                std::vector<NodePtr> rootNodes = copyOfGraph->getAllRootNodes();
                NES_TRACE("LogicalSourceExpansionRule: Get all nodes in the duplicated logical sub-graph");
                std::vector<NodePtr> family = copyOfGraph->getAndFlattenAllAncestors();

                NES_TRACE("LogicalSourceExpansionRule: For each original head operator find the duplicate operator and connect "
                          "it to the original graph");
                for (auto& originalRootOperator : originalRootOperators) {

                    NES_TRACE("LogicalSourceExpansionRule: Search the duplicate operator equal to original head operator");
                    auto found = std::find_if(family.begin(), family.end(), [&](NodePtr member) {
                        return member->as<OperatorNode>()->getId() == originalRootOperator->getId();
                    });

                    if (found != family.end()) {
                        NES_TRACE("LogicalSourceExpansionRule: Found the duplicate operator equal to the original head operator");
                        NES_TRACE(
                            "LogicalSourceExpansionRule: Add the duplicate operator to the parent of the original head operator");
                        for (auto& parent : originalRootOperator->getParents()) {
                            NES_TRACE("LogicalSourceExpansionRule: Assign the duplicate operator a new operator id.");
                            (*found)->as<OperatorNode>()->setId(UtilityFunctions::getNextOperatorId());
                            parent->addChild(*found);
                        }
                        family.erase(found);
                    }
                }

                NES_TRACE("LogicalSourceExpansionRule: Assign all operators in the duplicated operator graph a new operator id.");
                for (auto& member : family) {
                    member->as<OperatorNode>()->setId(UtilityFunctions::getNextOperatorId());
                }
            }
        }
    }
    NES_INFO("LogicalSourceExpansionRule: Finished applying LogicalSourceExpansionRule plan=" << queryPlan->toString());
    return queryPlan;
}

std::tuple<OperatorNodePtr, std::set<OperatorNodePtr>>
LogicalSourceExpansionRule::getLogicalGraphToDuplicate(OperatorNodePtr operatorNode) {
    NES_DEBUG("LogicalSourceExpansionRule: Get the logical graph to duplicate.");
    if (operatorNode->instanceOf<SinkLogicalOperatorNode>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()
        || operatorNode->instanceOf<MergeLogicalOperatorNode>() || operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        NES_TRACE("LogicalSourceExpansionRule: Found the first binary or sink operator.");
        return std::tuple<OperatorNodePtr, std::set<OperatorNodePtr>>();
    }

    NES_TRACE("LogicalSourceExpansionRule: Create duplicate of the input operator.");
    OperatorNodePtr copyOfOperator = operatorNode->copy();
    std::set<OperatorNodePtr> originalRootOperator;

    NES_TRACE("LogicalSourceExpansionRule: For each parent look if their ancestor has a n-ary operator or a sink operator.");
    for (auto& parent : operatorNode->getParents()) {
        OperatorNodePtr duplicatedParentOperator;
        std::set<OperatorNodePtr> rootOperators;

        NES_TRACE("LogicalSourceExpansionRule: Get the duplicated parent operator and its ancestors.");
        std::tie(duplicatedParentOperator, rootOperators) = getLogicalGraphToDuplicate(parent->as<OperatorNode>());

        if (duplicatedParentOperator) {
            NES_TRACE("LogicalSourceExpansionRule: Got a duplicated parent operator. Add the duplicate as parent to the copy of "
                      "input operator.");
            copyOfOperator->addParent(duplicatedParentOperator);
            NES_TRACE("LogicalSourceExpansionRule: Add the original head operators to the list of head operators for the input "
                      "operator");

            originalRootOperator.insert(rootOperators.begin(), rootOperators.end());
        } else {
            NES_TRACE("LogicalSourceExpansionRule: Parent operator was either n-ary or was of type sink.");
            NES_TRACE("LogicalSourceExpansionRule: Add the input operator as original head operator of the duplicated sub-graph");
            originalRootOperator.insert(operatorNode);
        }
    }
    return std::make_tuple(copyOfOperator, originalRootOperator);
}

}// namespace NES
