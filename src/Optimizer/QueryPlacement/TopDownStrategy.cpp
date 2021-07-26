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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<TopDownStrategy> TopDownStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                         TopologyPtr topology,
                                                         TypeInferencePhasePtr typeInferencePhase,
                                                         StreamCatalogPtr streamCatalog) {
    return std::make_unique<TopDownStrategy>(TopDownStrategy(std::move(globalExecutionPlan),
                                                             std::move(topology),
                                                             std::move(typeInferencePhase),
                                                             std::move(streamCatalog)));
}

TopDownStrategy::TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase,
                                 StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool TopDownStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const QueryId queryId = queryPlan->getQueryId();
    try {
        NES_INFO("TopDownStrategy: Performing placement of the input query plan with id " << queryId);
        NES_INFO("TopDownStrategy: And query plan \n" << queryPlan->toString());

        NES_DEBUG("TopDownStrategy: Get all source operators");
        const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
        if (sourceOperators.empty()) {
            NES_ERROR("TopDownStrategy: No source operators found in the query plan wih id: " << queryId);
            throw Exception("TopDownStrategy: No source operators found in the query plan wih id: " + std::to_string(queryId));
        }

        NES_DEBUG("TopDownStrategy: map pinned operators to the physical location");
        mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);

        NES_DEBUG("TopDownStrategy: Get all sink operators");
        const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
        if (sinkOperators.empty()) {
            NES_ERROR("TopDownStrategy: No sink operators found in the query plan wih id: " << queryId);
            throw Exception("TopDownStrategy: No sink operators found in the query plan wih id: " + std::to_string(queryId));
        }

        NES_DEBUG("TopDownStrategy: place query plan with id : " << queryId);
        placeQueryPlan(queryPlan);
        NES_DEBUG("TopDownStrategy: Add system generated operators for query with id : " << queryId);
        addNetworkSourceAndSinkOperators(queryPlan);
        NES_DEBUG("TopDownStrategy: clear the temporary map : " << queryId);
        operatorToExecutionNodeMap.clear();
        pinnedOperatorLocationMap.clear();
        NES_DEBUG(
            "TopDownStrategy: Run type inference phase for query plans in global execution plan for query with id : " << queryId);

        NES_DEBUG("TopDownStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
        return runTypeInferencePhase(queryId);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

bool TopDownStrategy::partiallyUpdateGlobalExecutionPlan(const QueryPlanPtr& queryPlan) {
    const QueryId queryId = queryPlan->getQueryId();
    try {
        NES_INFO(
            "TopDownStrategy::partiallyUpdateGlobalExecutionPlan: Performing partial placement of the input query plan with id "
            << queryId);
        NES_INFO("TopDownStrategy::partiallyUpdateGlobalExecutionPlan: And query plan \n" << queryPlan->toString());
        auto nodesWithQueryDeployed = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
        for (const auto& nodeWithQueryDeployed : nodesWithQueryDeployed) {
            for (const auto& querySubPlan : nodeWithQueryDeployed->getQuerySubPlans(queryId)) {
                auto nodes = QueryPlanIterator(querySubPlan).snapshot();
                for (const auto& node : nodes) {
                    const std::shared_ptr<LogicalOperatorNode>& asLogicalOperator = node->as<LogicalOperatorNode>();
                    operatorToExecutionNodeMap[asLogicalOperator->getId()] = nodeWithQueryDeployed;
                }
            }
        }
        for (const auto& sinkOperator : queryPlan->getSinkOperators()) {
            if (operatorToExecutionNodeMap.contains(sinkOperator->getId())) {
                pinnedOperatorLocationMap[sinkOperator->getId()] =
                    operatorToExecutionNodeMap[sinkOperator->getId()]->getTopologyNode();
            }
        }

        std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
        std::vector<TopologyNodePtr> mergedGraphSourceNodes;

        pinSinkOperator(queryId, queryPlan->getSourceOperators(), mapOfSourceToTopologyNodes, mergedGraphSourceNodes);

        for (const auto& sourceOperator : queryPlan->getSourceOperators()) {
            pinnedOperatorLocationMap[sourceOperator->getId()] =
                operatorToExecutionNodeMap[sourceOperator->getId()]->getTopologyNode();
        }

        NES_DEBUG("TopDownStrategy::partiallyUpdateGlobalExecutionPlan: place query plan with id : " << queryId);
        placeQueryPlan(queryPlan);
        NES_DEBUG("TopDownStrategy::partiallyUpdateGlobalExecutionPlan: Add system generated operators for query with id : "
                  << queryId);
        addNetworkSourceAndSinkOperators(queryPlan);
        operatorToExecutionNodeMap.clear();
        pinnedOperatorLocationMap.clear();
        NES_DEBUG("TopDownStrategy::partiallyUpdateGlobalExecutionPlan: Run type inference phase for query plans in global "
                  "execution plan for query with id : "
                  << queryId);

        NES_DEBUG("TopDownStrategy::partiallyUpdateGlobalExecutionPlan: Update Global Execution Plan : \n"
                  << globalExecutionPlan->getAsString());
        return runTypeInferencePhase(queryId);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void TopDownStrategy::placeQueryPlan(const QueryPlanPtr& queryPlan) {

    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("TopDownStrategy: Get the all sink operators for performing the placement.");
    const std::vector<SinkLogicalOperatorNodePtr>& sinkOperators = queryPlan->getSinkOperators();

    NES_TRACE("TopDownStrategy: Place all sink operators.");
    for (const auto& sinkOperator : sinkOperators) {
        NES_TRACE("TopDownStrategy: Get the topology node for the sink operator.");
        //TODO: Handle when we assume more than one sink nodes
        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sinkOperator->getId());
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
            throw Exception("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
        }
        placeOperator(queryId, sinkOperator, candidateTopologyNode);
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
}

void TopDownStrategy::placeOperator(QueryId queryId, const OperatorNodePtr& operatorNode, TopologyNodePtr candidateTopologyNode) {
    if (!operatorToExecutionNodeMap.contains(operatorNode->getId())) {
        NES_DEBUG("TopDownStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() || operatorNode->instanceOf<SourceLogicalOperatorNode>())
            && !operatorNode->instanceOf<SinkLogicalOperatorNode>()) {

            NES_TRACE("TopDownStrategy: Received an NAry operator for placement.");
            NES_TRACE("TopDownStrategy: Get the topology nodes where parent operators are placed.");
            std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForParentOperators(operatorNode);
            if (parentTopologyNodes.empty()) {
                NES_WARNING("TopDownStrategy: No topology node found where parent operators are placed.");
                return;
            }

            NES_TRACE("TopDownStrategy: Get the topology nodes where children source operators are to be placed.");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForSourceOperators(operatorNode);

            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            candidateTopologyNode = topology->findCommonNodeBetween(childNodes, parentTopologyNodes);

            if (!candidateTopologyNode) {
                NES_ERROR("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator "
                          << operatorNode->toString());
                throw Exception("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator "
                                + operatorNode->toString());
            }

            if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
                NES_DEBUG("TopDownStrategy: Received Source operator for placement.");

                auto pinnedSourceOperatorLocation = getTopologyNodeForPinnedOperator(operatorNode->getId());
                if (pinnedSourceOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSourceOperatorLocation->containAsParent(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSourceOperatorLocation;
                } else {
                    NES_ERROR("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is to be "
                              "placed.");
                    throw Exception("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is "
                                    "to be placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                    throw Exception("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_DEBUG("TopDownStrategy: Find the next Topology node where operator can be placed");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForSourceOperators(operatorNode);
            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            //FIXME: we are considering only one root node currently
            auto candidateTopologyNodes = topology->findNodesBetween(childNodes, {candidateTopologyNode});
            for (const auto& topologyNodes : candidateTopologyNodes) {
                if (topologyNodes && topologyNodes->getAvailableResources() > 0) {
                    candidateTopologyNode = topologyNodes;
                    NES_DEBUG(
                        "TopDownStrategy: Found NES node for placing the operators with id : " << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: No node available for further placement of operators");
            throw Exception("TopDownStrategy: No node available for further placement of operators");
        }

        NES_TRACE("TopDownStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("TopDownStrategy: Find and prepend operator to the candidate query plan.");
        QueryPlanPtr candidateQueryPlan = addOperatorToCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);

        NES_TRACE("TopDownStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("TopDownStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
            throw Exception("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query "
                            + std::to_string(queryId));
        }
        NES_TRACE("TopDownStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("TopDownStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("TopDownStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1);
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[operatorNode->getId()]->getTopologyNode();
    }

    NES_TRACE("TopDownStrategy: Place the children operators.");
    for (const auto& child : operatorNode->getChildren()) {
        placeOperator(queryId, child->as<OperatorNode>(), candidateTopologyNode);
    }
}

QueryPlanPtr TopDownStrategy::addOperatorToCandidateQueryPlan(QueryId queryId,
                                                              const OperatorNodePtr& candidateOperator,
                                                              const ExecutionNodePtr& executionNode) {

    OperatorNodePtr candidateOperatorCopy = candidateOperator->copy();
    NES_DEBUG("TopDownStrategy: Get candidate query plan for the operator " << candidateOperator << " on execution node with id "
                                                                            << executionNode->getId());
    NES_TRACE("TopDownStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("TopDownStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
        candidateQueryPlan->addRootOperator(candidateOperatorCopy);
        return candidateQueryPlan;
    }

    std::vector<QueryPlanPtr> queryPlansWithParent;
    NES_TRACE("TopDownStrategy: Find query plans with parent operators for the input logical operator.");
    std::vector<NodePtr> parents = candidateOperator->getParents();
    //NOTE: we do not check for child operators as we are performing Top down placement.
    for (auto& parent : parents) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
            return querySubPlan->hasOperatorWithId(parent->as<OperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("TopDownStrategy: Found query plan with parent operator " << parent);
            queryPlansWithParent.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithParent.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        if (queryPlansWithParent.size() > 1) {
            NES_TRACE("TopDownStrategy: Found more than 1 query plan with the parent operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE("TopDownStrategy: Prepare a new query plan and add the root of the query plans with parent operators as "
                      "the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithParent) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
        } else if (queryPlansWithParent.size() == 1) {
            NES_TRACE("TopDownStrategy: Found only 1 query plan with the parent operator of the input logical operator.");
            candidateQueryPlan = queryPlansWithParent[0];
        }

        if (candidateQueryPlan) {
            NES_TRACE("TopDownStrategy: Prepend candidate operator and update the plan.");
            for (const auto& candidateParent : parents) {
                auto parentOperator = candidateQueryPlan->getOperatorWithId(candidateParent->as<OperatorNode>()->getId());
                if (parentOperator) {
                    parentOperator->addChild(candidateOperatorCopy);
                }
            }
            return candidateQueryPlan;
        }
    }
    NES_TRACE("TopDownStrategy: no query plan exists with the parent operator of the input logical operator. Returning an empty "
              "query plan.");
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    candidateQueryPlan->addRootOperator(candidateOperatorCopy);
    return candidateQueryPlan;
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForParentOperators(const OperatorNodePtr& candidateOperator) {

    std::vector<TopologyNodePtr> parentTopologyNodes;
    NES_DEBUG("TopDownStrategy: Get topology nodes with parent operators");
    std::vector<NodePtr> parents = candidateOperator->getParents();
    //iterate over parent operators and get the physical location where operator is placed
    for (auto& parent : parents) {
        const auto& found = operatorToExecutionNodeMap.find(parent->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("TopDownStrategy: unable to find topology for parent operator.");
            return {};
        }
        TopologyNodePtr parentTopologyNode = found->second->getTopologyNode();
        parentTopologyNodes.push_back(parentTopologyNode);
    }
    NES_DEBUG("TopDownStrategy: returning list of topology nodes where parent operators are placed");
    return parentTopologyNodes;
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForSourceOperators(const OperatorNodePtr& candidateOperator) {
    std::vector<TopologyNodePtr> childNodes;

    NES_TRACE("TopDownStrategy::getTopologyNodesForSourceOperators: Get the pinned or closest placed sources nodes for the the "
              "input operator.");

    std::vector<NodePtr> sources = {candidateOperator->as<Node>()};

    while (!sources.empty()) {
        auto childOp = sources.back()->as<OperatorNode>();
        sources.pop_back();
        if (childOp->instanceOf<SourceLogicalOperatorNode>()) {
            childNodes.push_back(getTopologyNodeForPinnedOperator(childOp->getId()));
            continue;
        }
        if (operatorToExecutionNodeMap.contains(childOp->getId())) {
            childNodes.push_back(operatorToExecutionNodeMap[childOp->getId()]->getTopologyNode());
            continue;
        }
        sources.insert(sources.end(), childOp->getChildren().begin(), childOp->getChildren().end());
    }
    if (childNodes.empty()) {
        NES_ERROR("TopDownStrategy::getTopologyNodesForSourceOperators: Unable to find the sources operators to the candidate "
                  "operator");
        throw Exception("TopDownStrategy::getTopologyNodesForSourceOperators: Unable to find the sources operators to the "
                        "candidate operator");
    }
    return childNodes;
}

}// namespace NES::Optimizer
