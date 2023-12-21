/*
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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

#include <utility>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy> BottomUpStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                const TopologyPtr& topology,
                                                                const TypeInferencePhasePtr& typeInferencePhase) {
    return std::make_unique<BottomUpStrategy>(BottomUpStrategy(globalExecutionPlan, topology, typeInferencePhase));
}

BottomUpStrategy::BottomUpStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                   const TopologyPtr& topology,
                                   const TypeInferencePhasePtr& typeInferencePhase)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                 const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                 const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Pin all unpinned operators
        pinOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. Place all pinned operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 5. Perform type inference on all updated query plans
        runTypeInferencePhase(queryId);

        // 6. Release the locks from the topology nodes
        return unlockTopologyNodes();
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementException(queryId, ex.what());
    }
}

void BottomUpStrategy::pinOperators(QueryId queryId,
                                    const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                    const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("Get the topology node for source operator {} placement.",
                  pinnedUpStreamOperator->toString());

        auto workerId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(workerId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->getOperatorState() == OperatorState::PLACED) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeById(workerId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                identifyPinningLocation(queryId,
                                        downStreamNode->as<LogicalOperatorNode>(),
                                        candidateTopologyNode,
                                        pinnedDownStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedUpStreamOperator->getId())) {
                NES_ERROR("Unable to find resources on the physical node for placement of source operator");
                throw Exceptions::RuntimeException(
                    "Unable to find resources on the physical node for placement of source operator");
            }
            identifyPinningLocation(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("Finished placing query operators into the global execution plan");
}

void BottomUpStrategy::identifyPinningLocation(QueryId queryId,
                                               const LogicalOperatorNodePtr& logicalOperator,
                                               TopologyNodePtr candidateTopologyNode,
                                               const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    if (logicalOperator->getOperatorState() == OperatorState::PLACED) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    NES_DEBUG("Place {}", logicalOperator->toString());
    if ((logicalOperator->hasMultipleChildrenOrParents() && !logicalOperator->instanceOf<SourceLogicalOperatorNode>())
        || logicalOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("Received an NAry operator for placement.");
        //Check if all children operators already placed
        NES_TRACE("Get the topology nodes where child operators are placed.");
        std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(logicalOperator);
        if (childTopologyNodes.empty()) {
            NES_WARNING(
                "No topology node isOperatorAPinnedDownStreamOperator where child operators are placed.");
            return;
        }

        NES_TRACE("Find a node reachable from all topology nodes where child operators are placed.");
        if (childTopologyNodes.size() == 1) {
            candidateTopologyNode = childTopologyNodes[0];
        } else {
            candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
        }

        if (!candidateTopologyNode) {
            NES_ERROR(
                "Unable to find a common ancestor topology node to place the binary operator, operatorId: {}",
                logicalOperator->getId());
            topology->print();
            throw Exceptions::RuntimeException(
                "Unable to find a common ancestor topology node to place the binary operator");
        }

        if (logicalOperator->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("Received Sink operator for placement.");
            auto workerId = std::any_cast<uint64_t>(logicalOperator->getProperty(PINNED_NODE_ID));
            auto pinnedSinkOperatorLocation = getTopologyNode(workerId);
            if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                candidateTopologyNode = pinnedSinkOperatorLocation;
            } else {
                NES_ERROR("Unexpected behavior. Could not find Topology node where sink operator is to be "
                          "placed.");
                throw Exceptions::RuntimeException(
                    "Unexpected behavior. Could not find Topology node where sink operator is to be "
                    "placed.");
            }

            if (candidateTopologyNode->getAvailableResources() == 0) {
                NES_ERROR("Topology node where sink operator is to be placed has no capacity.");
                throw Exceptions::RuntimeException(
                    "Topology node where sink operator is to be placed has no capacity.");
            }
        }
    }

    if (candidateTopologyNode->getAvailableResources() == 0) {

        NES_DEBUG("Find the next NES node in the path where operator can be placed");
        while (!candidateTopologyNode->getParents().empty()) {
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
            if (candidateTopologyNode->getAvailableResources() > 0) {
                NES_DEBUG("Found NES node for placing the operators with id : {}",
                          candidateTopologyNode->getId());
                break;
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("No node available for further placement of operators");
        throw Exceptions::RuntimeException("No node available for further placement of operators");
    }

    candidateTopologyNode->reduceResources(1);
    logicalOperator->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());

    auto isOperatorAPinnedDownStreamOperator =
        std::find_if(pinnedDownStreamOperators.begin(),
                     pinnedDownStreamOperators.end(),
                     [logicalOperator](const OperatorNodePtr& pinnedDownStreamOperator) {
                         return pinnedDownStreamOperator->getId() == logicalOperator->getId();
                     });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        NES_DEBUG("Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("Place further upstream operators.");
    for (const auto& parent : logicalOperator->getParents()) {
        identifyPinningLocation(queryId, parent->as<LogicalOperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

}// namespace NES::Optimizer
