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

#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/MlHeuristicStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>
#include <z3++.h>
#include <API/Schema.hpp>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy> MlHeuristicStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                TopologyPtr topology,
                                                                TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<MlHeuristicStrategy>(MlHeuristicStrategy(std::move(globalExecutionPlan),
                                                               std::move(topology),
                                                               std::move(typeInferencePhase)));
}

MlHeuristicStrategy::MlHeuristicStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}


bool MlHeuristicStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                 FaultToleranceType faultToleranceType,
                                                 LineageType lineageType,
                                                 const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place operators on the selected path
        performOperatorPlacement(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void MlHeuristicStrategy::performOperatorPlacement(QueryId queryId,
                                                const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("MlHeuristicStrategy: Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("MlHeuristicStrategy: Get the topology node for source operator " << pinnedUpStreamOperator->toString()
                                                                                 << " placement.");

        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->hasProperty(PLACED) && std::any_cast<bool>(pinnedUpStreamOperator->getProperty(PLACED))) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                placeOperator(queryId, downStreamNode->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedUpStreamOperator->getId())) {
                NES_ERROR("MlHeuristicStrategy: Unable to find resources on the physical node for placement of source operator");
                throw Exception(
                    "MlHeuristicStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            placeOperator(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("MlHeuristicStrategy: Finished placing query operators into the global execution plan");
}

void MlHeuristicStrategy::placeOperator(QueryId queryId,
                                     const OperatorNodePtr& operatorNode,
                                     TopologyNodePtr candidateTopologyNode,
                                     const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {
    
    if (operatorNode->hasProperty(PLACED) && std::any_cast<bool>(operatorNode->getProperty(PLACED))) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    if (!operatorToExecutionNodeMap.contains(operatorNode->getId())) {

        NES_DEBUG("MlHeuristicStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
            || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("MlHeuristicStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("MlHeuristicStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
            if (childTopologyNodes.empty()) {
                NES_WARNING(
                    "MlHeuristicStrategy: No topology node isOperatorAPinnedDownStreamOperator where child operators are placed.");
                return;
            }

            NES_TRACE("MlHeuristicStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR(
                    "MlHeuristicStrategy: Unable to find a common ancestor topology node to place the binary operator, operatorId: "
                    << operatorNode->getId());
                topology->print();
                throw Exception("MlHeuristicStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("MlHeuristicStrategy: Received Sink operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
                auto pinnedSinkOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR("MlHeuristicStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                              "placed.");
                    throw Exception(

                        "MlHeuristicStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("MlHeuristicStrategy: Topology node where sink operator is to be placed has no capacity.");
                    throw Exception("MlHeuristicStrategy: Topology node where sink operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {

            NES_DEBUG("MlHeuristicStrategy: Find the next NES node in the path where operator can be placed");
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                if (candidateTopologyNode->getAvailableResources() > 0) {
                    NES_DEBUG("MlHeuristicStrategy: Found NES node for placing the operators with id : "
                              << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("MlHeuristicStrategy: No node available for further placement of operators");
            throw Exception("MlHeuristicStrategy: No node available for further placement of operators");
        }

        NES_TRACE("MlHeuristicStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("MlHeuristicStrategy: Get the candidate query plan where operator is to be appended.");
        QueryPlanPtr candidateQueryPlan = getCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);
        operatorNode->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());
        operatorNode->addProperty(PLACED, true);
        auto operatorCopy = operatorNode->copy();
        if (candidateQueryPlan->getRootOperators().empty()) {
            candidateQueryPlan->appendOperatorAsNewRoot(operatorCopy);
        } else {
            auto children = operatorNode->getChildren();
            for (const auto& child : children) {
                auto rootOperators = candidateQueryPlan->getRootOperators();
                if (candidateQueryPlan->hasOperatorWithId(child->as<OperatorNode>()->getId())) {
                    candidateQueryPlan->getOperatorWithId(child->as<OperatorNode>()->getId())->addParent(operatorCopy);
                }
                auto found =
                    std::find_if(rootOperators.begin(), rootOperators.end(), [child](const OperatorNodePtr& rootOperator) {
                        return rootOperator->getId() == child->as<OperatorNode>()->getId();
                    });
                if (found != rootOperators.end()) {
                    candidateQueryPlan->removeAsRootOperator(*(found));
                    auto updatedRootOperators = candidateQueryPlan->getRootOperators();
                    auto operatorAlreadyExistsAsRoot =
                        std::find_if(updatedRootOperators.begin(),
                                     updatedRootOperators.end(),
                                     [operatorCopy](const OperatorNodePtr& rootOperator) {
                                         return rootOperator->getId() == operatorCopy->as<OperatorNode>()->getId();
                                     });
                    if (operatorAlreadyExistsAsRoot == updatedRootOperators.end()) {
                        candidateQueryPlan->addRootOperator(operatorCopy);
                    }
                }
            }
            if (!candidateQueryPlan->hasOperatorWithId(operatorCopy->getId())) {
                candidateQueryPlan->addRootOperator(operatorCopy);
            }
        }

        NES_TRACE("MlHeuristicStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("MlHeuristicStrategy: failed to create a new QuerySubPlan execution node for query.");
            throw Exception("MlHeuristicStrategy: failed to create a new QuerySubPlan execution node for query.");
        }
        NES_TRACE("MlHeuristicStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("MlHeuristicStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("MlHeuristicStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1);
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[operatorNode->getId()]->getTopologyNode();
    }

    auto isOperatorAPinnedDownStreamOperator = std::find_if(pinnedDownStreamOperators.begin(),
                                                            pinnedDownStreamOperators.end(),
                                                            [operatorNode](const OperatorNodePtr& pinnedDownStreamOperator) {
                                                                return pinnedDownStreamOperator->getId() == operatorNode->getId();
                                                            });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        NES_DEBUG("MlHeuristicStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("MlHeuristicStrategy: Place further upstream operators.");
    for (const auto& parent : operatorNode->getParents()) {
        placeOperator(queryId, parent->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

//bool MlHeuristicStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {
//    const QueryId queryId = queryPlan->getQueryId();
//    try {
//        NES_INFO("MlHeuristicStrategy: Performing placement of the input query plan with id " << queryId);
//        NES_INFO("MlHeuristicStrategy: And query plan \n" << queryPlan->toString());
//
//        NES_DEBUG("MlHeuristicStrategy: Get all source operators");
//        const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
//        if (sourceOperators.empty()) {
//            NES_ERROR("MlHeuristicStrategy: No source operators found in the query plan wih id: " << queryId);
//            throw Exception("MlHeuristicStrategy: No source operators found in the query plan wih id: " + std::to_string(queryId));
//        }
//
//        NES_DEBUG("MlHeuristicStrategy: map pinned operators to the physical location");
//        mapPinnedOperatorToTopologyNodes(queryPlan);
//
//        NES_DEBUG("MlHeuristicStrategy: Get all sink operators");
//        const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
//        if (sinkOperators.empty()) {
//            NES_ERROR("MlHeuristicStrategy: No sink operators found in the query plan wih id: " << queryId);
//            throw Exception("MlHeuristicStrategy: No sink operators found in the query plan wih id: " + std::to_string(queryId));
//        }
//
//        NES_DEBUG("MlHeuristicStrategy: place query plan with id : " << queryId);
//        placeQueryPlanOnTopology(queryPlan);
//        NES_DEBUG("MlHeuristicStrategy: Add system generated operators for query with id : " << queryId);
//        addNetworkSourceAndSinkOperators(queryPlan);
//        NES_DEBUG("MlHeuristicStrategy: clear the temporary map : " << queryId);
//        operatorToExecutionNodeMap.clear();
//        pinnedOperatorLocationMap.clear();
//        NES_DEBUG("MlHeuristicStrategy: Run type inference phase for query plans in global execution plan for query with id : "
//                  << queryId);
//
//        NES_DEBUG("MlHeuristicStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
//
//        runTypeInferencePhase(queryId, queryPlan->getFaultToleranceType(), queryPlan->getLineageType());
//
//        bool enable_redundancy_elimination = true;
//
//        if(enable_redundancy_elimination) {
//            auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
//            auto context = std::make_shared<z3::context>();
//            auto signatureInferencePhase =
//                Optimizer::SignatureInferencePhase::create(context, QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
//
//            for (auto ex_node : executionNodes) {
//                auto querysubplans = ex_node->getQuerySubPlans(queryId);
//
//                SignatureEqualityUtilPtr signatureEqualityUtil = SignatureEqualityUtil::create(context);
//
//                if (querysubplans.size() >= 2) {
//                    auto a = querysubplans[0];
//                    auto b = querysubplans[1];
//
//                    runTypeInferencePhase(queryId, queryPlan->getFaultToleranceType(), queryPlan->getLineageType());
//
//                    signatureInferencePhase->execute(a);
//                    signatureInferencePhase->execute(b);
//
//                    auto targetSinkOperator = a->getSinkOperators().at(0);
//                    auto hostSinkOperator = b->getSinkOperators().at(0);;
//
//                    auto targetRootOperator = a->getSourceOperators().at(0)->getParents().at(0);
//                    auto hostRootOperator = b->getSourceOperators().at(0)->getParents().at(0);
//
//                    auto a_sign = targetSinkOperator->as<LogicalUnaryOperatorNode>()->getZ3Signature();
//                    auto b_sign = hostSinkOperator->as<LogicalUnaryOperatorNode>()->getZ3Signature();
//
//                    if (signatureEqualityUtil->checkEquality(a_sign, b_sign)) {
//                        auto targetRootChildren = targetRootOperator->getChildren();
//                        auto hostRootChildren = hostRootOperator->getChildren();
//
//                        for (auto& hostChild : hostRootChildren) {
//                            bool addedNewParent = hostChild->addParent(targetRootOperator);
//                            if (!addedNewParent) {
//                                NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Failed to add new parent");
//                            }
//                        }
//                    }
//
//                    querysubplans.pop_back();
//                }
//                ex_node->updateQuerySubPlans(queryId, querysubplans);
//            }
//            NES_DEBUG("MlHeuristicStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
//        }
//
//        return runTypeInferencePhase(queryId, queryPlan->getFaultToleranceType(), queryPlan->getLineageType());
//    } catch (Exception& ex) {
//        throw QueryPlacementException(queryId, ex.what());
//    }
//}

//void MlHeuristicStrategy::placeQueryPlanOnTopology(const QueryPlanPtr& queryPlan) {
//
//    QueryId queryId = queryPlan->getQueryId();
//    NES_DEBUG("MlHeuristicStrategy: Get the all source operators for performing the placement.");
//    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
//    for (auto& sourceOperator : sourceOperators) {
//        NES_DEBUG("MlHeuristicStrategy: Get the topology node for source operator " << sourceOperator->toString() << " placement.");
//        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sourceOperator->getId());
//        if (candidateTopologyNode->getAvailableResources() == 0) {
//            NES_ERROR("MlHeuristicStrategy: Unable to find resources on the physical node for placement of source operator");
//            throw Exception(
//
//                "MlHeuristicStrategy: Unable to find resources on the physical node for placement of source operator"
//                + std::to_string(queryId));
//        }
//        placeOperatorOnTopologyNode(queryId, sourceOperator, candidateTopologyNode);
//    }
//    NES_DEBUG("MlHeuristicStrategy: Finished placing query operators into the global execution plan");
//}

//bool MlHeuristicStrategy::placeOperatorOnTopologyNode(QueryId queryId,
//                                                   const OperatorNodePtr& operatorNode,
//                                                   TopologyNodePtr candidateTopologyNode) {
//
//    NES_DEBUG("MlHeuristicStrategy: Place " << operatorNode);
//
//    if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
//        || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
//        NES_TRACE("MlHeuristicStrategy: Received an NAry operator for placement.");
//        //Check if all children operators already placed
//        NES_TRACE("MlHeuristicStrategy: Get the topology nodes where child operators are placed.");
//        std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
//        if (childTopologyNodes.empty()) {
//            NES_WARNING("MlHeuristicStrategy: No topology node found where child operators are placed.");
//            return true;
//        }
//
//        NES_TRACE("MlHeuristicStrategy: Find a node reachable from all topology nodes where child operators are placed.");
//        if (childTopologyNodes.size() == 1) {
//            candidateTopologyNode = childTopologyNodes[0];
//        } else {
//            candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
//        }
//        if (!candidateTopologyNode) {
//            NES_ERROR(
//                "MlHeuristicStrategy: Unable to find a common ancestor topology node to place the binary operator, operatorId: "
//                << operatorNode->getId());
//            topology->print();
//            throw Exception("MlHeuristicStrategy: Unable to find a common ancestor topology node to place the binary operator");
//        }
//
//        if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
//            NES_TRACE("MlHeuristicStrategy: Received Sink operator for placement.");
//            auto pinnedSinkOperatorLocation = getTopologyNodeForPinnedOperator(operatorNode->getId());
//
//            if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
//                || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
//                candidateTopologyNode = pinnedSinkOperatorLocation;
//            } else {
//                NES_ERROR(
//                    "MlHeuristicStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be placed.");
//                throw Exception(
//
//                    "MlHeuristicStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be placed.");
//            }
//
//            if (candidateTopologyNode->getAvailableResources() == 0) {
//                NES_ERROR("MlHeuristicStrategy: Topology node where sink operator is to be placed has no capacity.");
//                throw Exception("MlHeuristicStrategy: Topology node where sink operator is to be placed has no capacity.");
//            }
//        }
//    }
//
////    candidateTopologyNode->getNodeProperty();
//    candidateTopologyNode->getAvailableResources();
//
//    bool cpu_saver_mode = true;
//    bool should_push_up = false;
//    bool can_be_placed_here = true;
//
//    bool tf_not_installed = operatorNode->instanceOf<InferModelLogicalOperatorNode>() && (!candidateTopologyNode->hasNodeProperty("tf_installed") || !std::any_cast<bool>(candidateTopologyNode->getNodeProperty("tf_installed")));
//    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0 || tf_not_installed) {
//        can_be_placed_here = false;
//    }
//
//    if(!can_be_placed_here){
//        should_push_up = true;
//        if(candidateTopologyNode->getParents().empty()) {
//            return false;
//        }
//    }
//
//    if(operatorNode->instanceOf<InferModelLogicalOperatorNode>()) {
//        if(candidateTopologyNode->getAvailableResources() < 5 && cpu_saver_mode){
//            should_push_up = true;
//        }
//
//        auto infModl = operatorNode->as<InferModelLogicalOperatorNode>();
//        float f0 = infModl->getInputSchema()->getSize();
//
//        auto ancestors = operatorNode->getAndFlattenAllAncestors();
//        auto sink = ancestors.at(ancestors.size()-1);
//        float f_new = sink->as<UnaryOperatorNode>()->getOutputSchema()->getSize();
//
//        float s = 1.0;
//
//        for (auto ancestor : ancestors) {
//            if(ancestor->instanceOf<FilterLogicalOperatorNode>()){
//                auto fltr = ancestor->as<FilterLogicalOperatorNode>();
//                s*=fltr->getSelectivity();
//            }
//        }
//        float fields_measure = f_new / f0;
//        float selectivity_measure = 1/s;
//        if (fields_measure > selectivity_measure) {
//            should_push_up = true;
//        }
//    }
//
//    if(candidateTopologyNode->getParents().empty()){
//        should_push_up = false;
//    }
//    if(should_push_up) {
//        if(placeOperatorOnTopologyNode(queryId, operatorNode, candidateTopologyNode->getParents()[0]->as<TopologyNode>())){
//            return true;
//        }
//    }
//    if(!can_be_placed_here){
//        return false;
//    }
//
//    NES_TRACE("MlHeuristicStrategy: Get the candidate execution node for the candidate topology node.");
//    ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);
//
//    NES_TRACE("MlHeuristicStrategy: Get the candidate query plan where operator is to be appended.");
//    QueryPlanPtr candidateQueryPlan = getCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);
//    candidateQueryPlan->appendOperatorAsNewRoot(operatorNode->copy());
//
//    NES_TRACE("MlHeuristicStrategy: Add the query plan to the candidate execution node.");
//    if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
//        NES_ERROR("MlHeuristicStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
//        throw Exception("MlHeuristicStrategy: failed to create a new QuerySubPlan execution node for query "
//                        + std::to_string(queryId));
//    }
//    NES_TRACE("MlHeuristicStrategy: Update the global execution plan with candidate execution node");
//    globalExecutionPlan->addExecutionNode(candidateExecutionNode);
//
//    NES_TRACE("MlHeuristicStrategy: Place the information about the candidate execution plan and operator id in the map.");
//    operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
//    NES_DEBUG("MlHeuristicStrategy: Reducing the node remaining CPU capacity by 1");
//    // Reduce the processing capacity by 1
//    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
//    candidateTopologyNode->reduceResources(1);
//    topology->reduceResources(candidateTopologyNode->getId(), 1);
//
//    NES_TRACE("MlHeuristicStrategy: Place the parent operators.");
//    for (const auto& parent : operatorNode->getParents()) {
//        placeOperatorOnTopologyNode(queryId, parent->as<OperatorNode>(), candidateTopologyNode);
//    }
//    return true;
//}

QueryPlanPtr MlHeuristicStrategy::getCandidateQueryPlan(QueryId queryId,
                                                     const OperatorNodePtr& operatorNode,
                                                     const ExecutionNodePtr& executionNode) {

    NES_DEBUG("MlHeuristicStrategy: Get candidate query plan for the operator " << operatorNode << " on execution node with id "
                                                                             << executionNode->getId());
    NES_TRACE("MlHeuristicStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("MlHeuristicStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
        return candidateQueryPlan;
    }

    std::vector<QueryPlanPtr> queryPlansWithChildren;
    NES_TRACE("MlHeuristicStrategy: Find query plans with child operators for the input logical operator.");
    std::vector<NodePtr> children = operatorNode->getChildren();
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : children) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
            return querySubPlan->hasOperatorWithId(child->as<OperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("MlHeuristicStrategy: Found query plan with child operator " << child);
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithChildren.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        if (queryPlansWithChildren.size() > 1) {
            NES_TRACE("MlHeuristicStrategy: Found more than 1 query plan with the child operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE("MlHeuristicStrategy: Prepare a new query plan and add the root of the query plans with parent operators as "
                      "the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithChildren) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
            NES_TRACE("MlHeuristicStrategy: return the updated query plan.");
            return candidateQueryPlan;
        }
        if (queryPlansWithChildren.size() == 1) {
            NES_TRACE("MlHeuristicStrategy: Found only 1 query plan with the child operator of the input logical operator. "
                      "Returning the query plan.");
            return queryPlansWithChildren[0];
        }
    }
    NES_TRACE("MlHeuristicStrategy: no query plan exists with the child operator of the input logical operator. Returning an empty "
              "query plan.");
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    return candidateQueryPlan;
}

std::vector<TopologyNodePtr> MlHeuristicStrategy::getTopologyNodesForChildrenOperators(const OperatorNodePtr& operatorNode) {

    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("MlHeuristicStrategy: Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        const auto& found = operatorToExecutionNodeMap.find(child->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("MlHeuristicStrategy: unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode = found->second->getTopologyNode();
        childTopologyNodes.push_back(childTopologyNode);
    }
    NES_DEBUG("MlHeuristicStrategy: returning list of topology nodes where children operators are placed");
    return childTopologyNodes;
}

}// namespace NES::Optimizer
