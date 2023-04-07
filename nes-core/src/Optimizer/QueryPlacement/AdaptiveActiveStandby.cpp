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
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/AdaptiveActiveStandby.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <log4cxx/helpers/exception.h>
#include <utility>

namespace NES::Optimizer {

AdaptiveActiveStandbyPtr AdaptiveActiveStandby::create(TopologyPtr topology,
                                                       PlacementStrategy::ValueAAS placementStrategyAAS,
                                                       z3::ContextPtr z3Context) {
    return std::make_unique<AdaptiveActiveStandby>(AdaptiveActiveStandby(std::move(topology), placementStrategyAAS, std::move(z3Context)));
}

AdaptiveActiveStandby::AdaptiveActiveStandby(TopologyPtr topology,
                                             PlacementStrategy::ValueAAS placementStrategyAAS,
                                             z3::ContextPtr z3Context)
    : topology(std::move(topology)), z3Context(std::move(z3Context)) {
    this->placementStrategy = placementStrategyAAS;
}

bool AdaptiveActiveStandby::execute(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators) {
    bool success = false;
    double score = 0;
    auto start = std::chrono::steady_clock::now();

    NES_DEBUG("AdaptiveActiveStandby: Started. Current topology: " << topology->toString());

    // 1. save the source nodes
    // NOTE: Current assumption: pinned upstream ops = sources. Could very easily be extended for other cases as well.
    for (const auto& pinnedUpStreamOperator: pinnedUpStreamOperators) {
        auto sourceNode = findNodeWherePinned(pinnedUpStreamOperator);
        sourceNodes.insert(std::make_pair(sourceNode->getId(), sourceNode));
    }

    auto timeBeforeNodeDeployment = std::chrono::steady_clock::now();

    // 2. deploy new topology nodes so that every operator can be replicated and have a secondary path
    deployNewNodes(pinnedUpStreamOperators);

    auto timeAfterNodeDeployment = std::chrono::steady_clock::now();
    auto elapsedMillisecondsNodeDeployment =
        duration_cast<std::chrono::milliseconds>(timeAfterNodeDeployment - timeBeforeNodeDeployment);
    NES_DEBUG("AdaptiveActiveStandby::Node deployment: Time elapsed: " << elapsedMillisecondsNodeDeployment.count() << "ms");

    // 3. choose placement method
    if (placementStrategy == PlacementStrategy::ValueAAS::Greedy_AAS
        || placementStrategy == PlacementStrategy::ValueAAS::LocalSearch_AAS) {

        auto timeBeforeGreedy = std::chrono::steady_clock::now();

        // 3.1 find an initial placement using a greedy algorithm
        success = executeGreedyPlacement(pinnedUpStreamOperators);

        auto timeAfterGreedy = std::chrono::steady_clock::now();
        auto elapsedMillisecondsGreedy =
            duration_cast<std::chrono::milliseconds>(timeAfterGreedy - timeBeforeGreedy);
        NES_DEBUG("AdaptiveActiveStandby::Greedy placement: Time elapsed: " << elapsedMillisecondsGreedy.count() << "ms");

        if (!success) {
            NES_DEBUG("AdaptiveActiveStandby: There are no valid replica placements.");
            return false;
        }

        // 3.2 evaluate placement, add scores to operator and topology nodes
        score = evaluateEntireCandidatePlacement();

        NES_DEBUG("AdaptiveActiveStandby: The Greedy Algorithm has found the following initial replica placement: \n"
                  << candidateOperatorPlacementsToString() << "Total score with penalties: " << score);

        // 3.3 execute local search if enabled
        if (placementStrategy == PlacementStrategy::ValueAAS::LocalSearch_AAS) {
            auto bestScoreChange = 0.0;
            uint16_t unsuccessfulAttempts = 0;
            auto rep = 0;

            auto timeBeforeRLS = std::chrono::steady_clock::now();

            // 3.3.1 save the greedy algorithm's candidate as the initial maps
            const OperatorToTopologyMap initialOperatorToTopologyMap = candidateOperatorToTopologyMap;
            const TopologyToOperatorMap initialTopologyToOperatorMap = candidateTopologyToOperatorMap;

            // 3.3.2 save placement as current best
            bestOperatorToTopologyMap = candidateOperatorToTopologyMap;
            bestTopologyToOperatorMap = candidateTopologyToOperatorMap;

            // 3.3.3 repeated local search as long as there is time left
            auto currentTime = std::chrono::steady_clock::now();
            auto elapsedMilliseconds = duration_cast<std::chrono::milliseconds>(currentTime - start);

            while (elapsedMilliseconds < timeConstraint) {
                NES_DEBUG("AdaptiveActiveStandby: Starting new Local Search (rep " << ++rep << "), time left: "
                          << (timeConstraint - elapsedMilliseconds).count() << "ms.");
                auto scoreChange = executeLocalSearch(timeConstraint - elapsedMilliseconds);

                currentTime = std::chrono::steady_clock::now();
                elapsedMilliseconds = duration_cast<std::chrono::milliseconds>(currentTime - start);

                if (scoreChange < bestScoreChange) { // minimization
                    NES_DEBUG("AdaptiveActiveStandby: Local Search found a better placement \n"
                              << candidateOperatorPlacementsToString()
                              << "Score improvement : "
                              << -scoreChange);
                    // update current best placement
                    bestOperatorToTopologyMap = candidateOperatorToTopologyMap;
                    bestTopologyToOperatorMap = candidateTopologyToOperatorMap;
                    bestScoreChange = scoreChange;
                    unsuccessfulAttempts = 0;
                } else {
                    unsuccessfulAttempts++;
                    // NOTE: just an arbitrary breaking point in addition to the time constraint, could be changed to something else
                    if (unsuccessfulAttempts >= secondaryOperatorMap.size() / 3) { // experiments: 3 secondary operators per source
                        NES_DEBUG("AdaptiveActiveStandby: No improvements found for the last "
                                  << secondaryOperatorMap.size() << " Local Search reps. Terminating.");
                        break;
                    }
                }
                // restart from initial candidates
                candidateOperatorToTopologyMap = initialOperatorToTopologyMap;
                candidateTopologyToOperatorMap = initialTopologyToOperatorMap;
            }
            if (bestScoreChange < 0) {
                candidateOperatorToTopologyMap = bestOperatorToTopologyMap;
                candidateTopologyToOperatorMap = bestTopologyToOperatorMap;
                score += bestScoreChange;
                NES_DEBUG("AdaptiveActiveStandby: Local Search improved the Greedy Algorithm's placement by a score of "
                          << -bestScoreChange);
            } else {
                NES_DEBUG("AdaptiveActiveStandby: Local Search could not find a better placement than the Greedy Algorithm");
            }
            auto timeAfterRLS = std::chrono::steady_clock::now();
            auto elapsedMillisecondsRLS =
                duration_cast<std::chrono::milliseconds>(timeAfterRLS - timeBeforeRLS);
            NES_DEBUG("AdaptiveActiveStandby::Repeated local search: Time elapsed: " << elapsedMillisecondsRLS.count() << "ms"
                                                                                     << " (excluding the Greedy Algorithm!)");
        }
    } else if (placementStrategy == PlacementStrategy::ValueAAS::ILP_AAS) {
        if (z3Context == nullptr) {
            z3::config cfg;
            cfg.set("timeout", timeout);
            cfg.set("model", false);
            cfg.set("type_check", false);
            z3Context = std::make_shared<z3::context>(cfg);
            NES_INFO("AdaptiveActiveStandby: no z3Context was passed for ILP strategy, created a new one");
            return false;
        }

        auto timeBeforeILP = std::chrono::steady_clock::now();

        score = executeILPStrategy(pinnedUpStreamOperators);

        auto timeAfterILP = std::chrono::steady_clock::now();
        auto elapsedMillisecondsILP =
            duration_cast<std::chrono::milliseconds>(timeAfterILP - timeBeforeILP);
        NES_DEBUG("AdaptiveActiveStandby::ILP: Time elapsed: " << elapsedMillisecondsILP.count() << "ms");

        if (score <= 0) {
            NES_DEBUG("AdaptiveActiveStandby: There are no valid replica placements.");
            deleteReplicas();
            return false;
        }
    }

    auto currentTime = std::chrono::steady_clock::now();
    auto elapsedMilliseconds = duration_cast<std::chrono::milliseconds>(currentTime - start);

    NES_DEBUG("AdaptiveActiveStandby: Placing the best candidate \n"
              << candidateOperatorPlacementsToString()
              << "Total score with penalties: " << score << "\n"
              << "Calculation time: " << elapsedMilliseconds.count() << "ms");

    // 7. Pin the operators based on the best candidate
    pinOperators();

    return true;
}

void AdaptiveActiveStandby::deployNewNodes(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators) {
    std::vector<TopologyNodePtr> newNodes;

    // 1. check for the parents of the pinned upstream operators whether they have a path to their sink that does not include
    //    the topology nodes where the primaries are placed
    for (const auto& currentOperator : pinnedUpStreamOperators) {
        auto pinnedOperatorsTopologyNode = findNodeWherePinned(currentOperator);

        // 1.1 get the closest primary ancestors that are placed on a different topology node
        std::set<NodePtr> ancestorsOnDifferentNodes;
        getAllClosestAncestorsOnDifferentNodes(currentOperator, ancestorsOnDifferentNodes);

        // 1.2 for all ancestors on different nodes: check if there is a separate path from the node of the current pinned
        //     upstream operator to its sink, excluding the nodes of the path of the current parent
        for (const auto& currentParent : ancestorsOnDifferentNodes) {
            NES_DEBUG("AdaptiveActiveStandby: Checking if there is a separate path from topology node "
                      << pinnedOperatorsTopologyNode->toString() << " to the sink, excluding operator " <<
                      currentParent->as<OperatorNode>()->toString() << " and its ancestors");
            auto exists =
                separatePathExists(currentParent->as<OperatorNode>(), pinnedOperatorsTopologyNode);
            if (exists) {
                NES_DEBUG("AdaptiveActiveStandby: Separate path found");
            } else {
                NES_DEBUG("AdaptiveActiveStandby: No separate path. New topology nodes have to be deployed");

                // get length of shortest path between current node and sink
                auto startNodes =
                    topology->findPathBetween({pinnedOperatorsTopologyNode}, {topology->getRoot()});
                auto startNode = startNodes.front()->as<TopologyNode>();
                int shortestLength = 0;     // number of hops from start to root
                auto currentNode = startNode;
                while (currentNode->getId() != topology->getRoot()->getId()) {
                    currentNode = currentNode->getParents().front()->as<TopologyNode>();
                    shortestLength++;
                }

                // get all nodes used for the primary's path to the sink
                auto nodeIdsToExclude =
                    getNodeIdsToExcludeToTarget(currentParent->as<OperatorNode>(),topology->getRoot());

                // a new node has to be deployed in any case, it will add 2 hops (see: at the end of this branch)
                int currentLength = 2;

                // try reaching as far from start node as possible or until length reached
                TopologyNodePtr farthestParentFromStart = pinnedOperatorsTopologyNode;
                std::vector<TopologyNodePtr> nextLevel;
                // initialize next level with parents of the start node without nodes to exclude
                for (const auto& parent : pinnedOperatorsTopologyNode->getParents()) {
                    auto parentNode = parent->as<TopologyNode>();
                    if (!nodeIdsToExclude.contains(parentNode->getId()))
                        nextLevel.push_back(parentNode);
                }
                while (!nextLevel.empty() && currentLength < shortestLength) {
                    // pick parent with least amount of connections to avoid bottlenecks
                    size_t minConnections = std::numeric_limits<size_t>::max();
                    TopologyNodePtr parentWithLeastConnections;
                    for (const auto& node : nextLevel) {
                        auto currentConnections = node->getChildren().size() + node->getParents().size();
                        if (currentConnections < minConnections) {
                            minConnections = currentConnections;
                            parentWithLeastConnections = node;
                        }
                    }

                    // keep track of farthest parent
                    farthestParentFromStart = parentWithLeastConnections;
                    ++currentLength;

                    nextLevel.clear();
                    // get next level by getting all parents of the nodes, barring nodes to exclude
                    for (const auto& parent : farthestParentFromStart->getParents()) {
                        auto parentNode = parent->as<TopologyNode>();
                        if (!nodeIdsToExclude.contains(parentNode->getId()))
                            nextLevel.push_back(parentNode);
                    }
                }

                // try reaching as far from root node as possible or until length reached
                TopologyNodePtr farthestChildFromRoot = topology->getRoot();
                nextLevel.clear();
                // initialize next level with parents of the start node without nodes to exclude
                for (const auto& child : topology->getRoot()->getChildren()) {
                    auto childNode = child->as<TopologyNode>();
                    if (!nodeIdsToExclude.contains(childNode->getId()))
                        nextLevel.push_back(childNode);
                }
                while (!nextLevel.empty() && currentLength < shortestLength) {
                    // pick child with least amount of connections to avoid bottlenecks
                    size_t minConnections = std::numeric_limits<size_t>::max();
                    TopologyNodePtr childWithLeastConnections;
                    for (const auto& node : nextLevel) {
                        auto currentConnections = node->getChildren().size() + node->getParents().size();
                        if (currentConnections < minConnections) {
                            minConnections = currentConnections;
                            childWithLeastConnections = node;
                        }
                    }

                    // keep track of farthest child
                    farthestChildFromRoot = childWithLeastConnections;
                    ++currentLength;

                    nextLevel.clear();
                    // get next level by getting all children of the nodes, barring nodes to exclude
                    for (const auto& child : farthestChildFromRoot->getChildren()) {
                        auto childNode = child->as<TopologyNode>();
                        if (!nodeIdsToExclude.contains(childNode->getId()))
                            nextLevel.push_back(childNode);
                    }
                }

                // add new nodes so that the new path is at least as long as the shortest path
                NES_DEBUG("AdaptiveActiveStandby: Deploying " << shortestLength - currentLength + 1 << " node(s) between "
                          << farthestParentFromStart->toString() << " and " << farthestChildFromRoot->toString());

                // start adding new nodes from top to bottom (direction of sink to source)
                TopologyNodePtr prevNewNode, currentNewNode;

                // first node connected to farthestChildFromRoot
                currentNewNode = TopologyNode::create(newTopologyNodeIdStart + nNewTopologyNodes,
                                                      ipAddress, grpcPort, dataPort, resources);
                currentNewNode->addNodeProperty("slots", resources);

                // first new node created => 2 hops (hence the initial value of currentLength)
                // one hop here, another one after the loop
                ++nNewTopologyNodes;
                newNodes.push_back(currentNewNode);

                topology->addNewTopologyNodeAsChild(farthestChildFromRoot, currentNewNode);
                currentNewNode->addLinkProperty(farthestChildFromRoot, linkProperty);
                farthestChildFromRoot->addLinkProperty(currentNewNode, linkProperty);

                // following new nodes, if necessary
                while (currentLength < shortestLength) {
                    prevNewNode = currentNewNode;
                    currentNewNode = TopologyNode::create(newTopologyNodeIdStart + nNewTopologyNodes,
                                                          ipAddress, grpcPort, dataPort, resources);
                    currentNewNode->addNodeProperty("slots", resources);

                    ++currentLength;            // new nodes after the 1st only add single new hops
                    ++nNewTopologyNodes;
                    newNodes.push_back(currentNewNode);

                    topology->addNewTopologyNodeAsChild(prevNewNode, currentNewNode);
                    currentNewNode->addLinkProperty(prevNewNode, linkProperty);
                    prevNewNode->addLinkProperty(currentNewNode, linkProperty);
                }

                // last node connected to farthestParentFromStart
                topology->addNewTopologyNodeAsChild(currentNewNode, farthestParentFromStart);
                currentNewNode->addLinkProperty(farthestParentFromStart, linkProperty);
                farthestParentFromStart->addLinkProperty(currentNewNode, linkProperty);
            }
        }
    }

    if (newNodes.empty()) {
        NES_DEBUG("AdaptiveActiveStandby: No need for new topology nodes");
    }
    else {
        NES_DEBUG("AdaptiveActiveStandby: New topology nodes have been deployed. Resulting topology: " << topology->toString());
    }
}

bool AdaptiveActiveStandby::separatePathExists(const OperatorNodePtr& primaryOperator, const TopologyNodePtr& startTopologyNode) {
    return separatePathExists(primaryOperator, startTopologyNode, topology->getRoot());
}

bool AdaptiveActiveStandby::separatePathExists(const OperatorNodePtr& primaryOperator,
                                               const TopologyNodePtr& startTopologyNode,
                                               const TopologyNodePtr& targetTopologyNode) {

    auto nodeIdsToExclude = getNodeIdsToExcludeToTarget(primaryOperator, targetTopologyNode);

    nodeIdsToExclude.erase(targetTopologyNode->getId());

    // check if there is a path from the start node to the sink that excludes all the nodes of the primary and its ancestors
    return topology->isPathBetweenExcluding(startTopologyNode, targetTopologyNode, nodeIdsToExclude);
}

std::set<TopologyNodeId> AdaptiveActiveStandby::getNodeIdsToExcludeToTarget(const OperatorNodePtr& primaryOperator,
                                                                            const TopologyNodePtr& targetTopologyNode) {
    std::set<TopologyNodeId> nodeIds;

    if (excludeNodesConnectingPrimaries) {
        // NOTE: this does exclude intermediate topology nodes that are used for transmitting data between the primaries,
        // works under the assumption that there is either only one path or the first path is used for connecting nodes
        auto nodeOfPrimary = findNodeWherePinned(primaryOperator);
        auto startNodes = topology->findPathBetween({nodeOfPrimary}, {targetTopologyNode});

        if (startNodes.empty())
            return nodeIds;

        TopologyNodePtr node = startNodes[0];
        while (!node->getParents().empty()) {
            nodeIds.insert(node->getId());
            node = node->getParents()[0]->as<TopologyNode>();
        }
        nodeIds.insert(node->getId());
    } else {
        // NOTE: this does not exclude intermediate topology nodes that are used for transmitting data between the primaries,
        // only nodes where operators are actually placed

        // get all ancestors of the primary (including it) and the nodes where they are pinned
        auto allAncestors = primaryOperator->getAndFlattenAllAncestors();
        nodeIds = getAllTopologyNodesByIdWherePinned(allAncestors);
    }

    return nodeIds;
}

void AdaptiveActiveStandby::getAllClosestAncestorsOnDifferentNodes(const OperatorNodePtr& operatorNode, std::set<NodePtr>& results) {
    auto operatorsTopologyNode = findNodeWherePinned(operatorNode);
    auto parents = operatorNode->getParents();

    // initialize queue with parents
    std::deque<NodePtr> primaryAncestors(parents.begin(), parents.end());
    while (!primaryAncestors.empty()) {
        // get first element of queue
        auto currentAncestor= primaryAncestors.front()->as<OperatorNode>();
        primaryAncestors.pop_front();

        auto topologyNodeOfParent = findNodeWherePinned(currentAncestor);

        // check if this ancestor is placed on the same node as the pinned operator
        if (topologyNodeOfParent == operatorsTopologyNode) {
            // if yes, add its parents to the queue
            for (const auto& ancestor : currentAncestor->getParents()) {
                primaryAncestors.push_back(ancestor);
            }
        }
        else {
            // add to result vector otherwise
            results.insert(currentAncestor);
        }
    }
}

bool AdaptiveActiveStandby::sortOperatorsPredicate(const NodePtr& a, const NodePtr& b) {
    auto operatorA = a->as_if<OperatorNode>();
    auto operatorB = b->as_if<OperatorNode>();

    auto outputA = getOperatorOutput(operatorA);
    auto outputB = getOperatorOutput(operatorB);

    if (outputA < outputB)
        return true;
    if (outputA > outputB)
        return false;

    // if output is the same
    return getOperatorCost(operatorA) < getOperatorCost(operatorB);
}

//FIXME: in #1422. This needs to be defined better as at present irrespective of operator location we are returning always the default value
double AdaptiveActiveStandby::getOperatorOutput(const OperatorNodePtr& operatorNode) {
    if (operatorNode->hasProperty("output")) {
        std::any prop = operatorNode->getProperty("output");
        return std::any_cast<double>(prop);
    }

    double dmf = 1;
    double input = 10;
    if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        dmf = 0;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        dmf = .5;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        dmf = 2;
    } else if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        input = 100;
    }
    return dmf * input;
}

//FIXME: in #1422. This needs to be defined better as at present irrespective of operator location we are returning always the default value
int AdaptiveActiveStandby::getOperatorCost(const OperatorNodePtr& operatorNode) {
    if (operatorNode->hasProperty("cost")) {
        std::any prop = operatorNode->getProperty("cost");
        return std::any_cast<int>(prop);
    }

    if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        // network sinks considered to have no cost
        if (operatorNode->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>())
            return 0;
        else
            return 1;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        return 1;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>() || operatorNode->instanceOf<JoinLogicalOperatorNode>()
               || operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        return 2;
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        return 1;
    } else if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // AAS: source topology nodes can only host the source operator and nothing else
        // -> we do not keep track of its resources (set to 0)
//        if (operatorNode->as<SourceLogicalOperatorNode>()->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>())
//            return 0;
//        else
//            return 1;
        return 0;
    }
    return 2;
}

double AdaptiveActiveStandby::evaluateEntireCandidatePlacement() {
    auto totalScore = 0.0;

    NES_DEBUG("AdaptiveActiveStandby: evaluating the candidate placement");

    // iterate over all topology nodes that have to be evaluated
    for (const auto& [topologyNodeId, val] : candidateTopologyToOperatorMap) {
        auto currentNode = topology->findNodeWithId(topologyNodeId);
        auto operatorsOnCurrentNode = val.first;
        // iterate over all replicas that are placed on the current topology node
        for (auto operatorId : operatorsOnCurrentNode) {
            auto secondaryOperator = secondaryOperatorMap[operatorId];
            // evaluate current operator, update its score
            auto currentOperatorScore = evaluateSinglePlacement(secondaryOperator);

            totalScore += currentOperatorScore;
        }

        // evaluate current topology node
        auto currentTopologyNodeScore = calculateOverUtilizationPenalty(currentNode);
        // update its score
        candidateTopologyToOperatorMap[topologyNodeId].second = currentTopologyNodeScore;

        totalScore += currentTopologyNodeScore;
    }

    return totalScore;
}

std::vector<NodePtr> AdaptiveActiveStandby::getAllParents(const std::vector<OperatorNodePtr>& operators) {
    std::vector<NodePtr> allParents;
    for (const auto& currentOperator : operators) {
        std::vector<NodePtr> currentParents = currentOperator->getParents();
        allParents.insert(allParents.end(), currentParents.begin(), currentParents.end());
    }
    return allParents;
}

std::vector<NodePtr> AdaptiveActiveStandby::getAllParents(const std::vector<NodePtr>& operators) {
    std::vector<NodePtr> allParents;
    for (const auto& currentOperator : operators) {
        std::vector<NodePtr> currentParents = currentOperator->getParents();
        allParents.insert(allParents.end(), currentParents.begin(), currentParents.end());
    }
    return allParents;
}

double AdaptiveActiveStandby::getDistance(const TopologyNodePtr& start, const TopologyNodePtr& target) {
    double dist = 0.0;

    // check if it has already been calculated
    if (distances.contains(start->getId())) {
        auto firstMap = distances[start->getId()];
        if (firstMap.contains(target->getId())) {
            dist = firstMap[target->getId()];
            NES_DEBUG("AdaptiveActiveStandby: Fetched distance between " << start->toString() << " and " << target->toString()
                                                                         << ": " << dist);
            return dist;
        }
    }

    dist = calculateDistance(start, target);
    distances[start->getId()][target->getId()] = dist;

    return dist;
}

double AdaptiveActiveStandby::calculateDistance(const TopologyNodePtr& start, const TopologyNodePtr& target) {
    double dist = 0.0;

    // find the path between start and target (single start node)
    // NOTE: works if paths are truly distinct, but it would need nodesToExclude for intertwined paths
    auto node = topology->findPathBetween({start}, {target})[0];

    // traverse subgraph, add distances
    while (node->getId() != target->getId()) {
        auto parentNode = node->getParents()[0]->as<TopologyNode>();

        auto bandwidth = node->getLinkProperty(parentNode)->bandwidth;
        dist += 1.0 / bandwidth;

        node = parentNode;
    }

    NES_DEBUG("AdaptiveActiveStandby: Calculated distance between " << start->toString() << " and " << target->toString() << ": "
                                                                    << dist);

    return dist;
}

double AdaptiveActiveStandby::calculateOverUtilizationPenalty(const TopologyNodePtr& topologyNode, int reserveResources) {
    double penalty = 0.0;

    int availableResources = calculateAvailableResources(topologyNode) - reserveResources;

    if (availableResources < 0) {
        penalty = overUtilizationPenaltyWeight * (-availableResources);
        NES_DEBUG("AdaptiveActiveStandby: Over-utilizing node " << topologyNode->toString() << ". Penalty: " << penalty);
    }

    return penalty;
}

double AdaptiveActiveStandby::calculateOverUtilizationPenalty(const TopologyNodePtr& topologyNode,
                                                              const OperatorNodePtr& operatorNode) {

    int operatorCost = getOperatorCost(operatorNode);

    return calculateOverUtilizationPenalty(topologyNode, operatorCost);
}

int AdaptiveActiveStandby::calculateAvailableResources(const TopologyNodePtr& topologyNode) {
    int availableResources = topologyNode->getAvailableResources();

    auto topologyNodeId = topologyNode->getId();
    auto operatorIds = candidateTopologyToOperatorMap[topologyNodeId].first;

    for (const auto& operatorId: operatorIds) {
        auto secondaryOperator = secondaryOperatorMap[operatorId];
        availableResources -= getOperatorCost(secondaryOperator);
    }

    return availableResources;
}

void AdaptiveActiveStandby::pinSecondaryOperator(OperatorId secondaryOperatorId, TopologyNodeId topologyNodeId) {
    // update both candidate maps

    // 1. check if operator has already been pinned to another node
    if (candidateOperatorToTopologyMap.contains(secondaryOperatorId)) {
        auto prevNodeId = candidateOperatorToTopologyMap[secondaryOperatorId].first;
        if (prevNodeId != topologyNodeId) {
            // remove from topology node's operator list
            candidateTopologyToOperatorMap[prevNodeId].first.erase(secondaryOperatorId);
            NES_DEBUG("AdaptiveActiveStandby: Removing operator " << secondaryOperatorId
                                                                  << " from its current node " << prevNodeId);
            // operator's placement (candidateOperatorToTopologyMap) does not have to be removed, it will be overwritten in 2.
        } else
            NES_WARNING("AdaptiveActiveStandby: Replica has already been pinned to target node");
    }

    // 2. pin to target node
    candidateOperatorToTopologyMap[secondaryOperatorId].first = topologyNodeId;
    candidateTopologyToOperatorMap[topologyNodeId].first.insert(secondaryOperatorId);
}

std::string AdaptiveActiveStandby::candidateOperatorPlacementsToString() {
    std::stringstream placementString;
    placementString << "Primary Operator ID (secondary operator ID) -> Topology Node ID : score" << std::endl;

    for (const auto& currentPlacement : candidateOperatorToTopologyMap) {
        OperatorId secondaryOperatorId = currentPlacement.first;

        OperatorId primaryOperatorId =
            std::any_cast<uint64_t>(secondaryOperatorMap[secondaryOperatorId]->getProperty(PRIMARY_OPERATOR_ID));

        TopologyNodeId currentTopologyNodeId = currentPlacement.second.first;
        placementString << primaryOperatorId << " ("
                        << secondaryOperatorId << ") -> "
                        << currentTopologyNodeId << " : "
                        << candidateOperatorToTopologyMap[secondaryOperatorId].second
                        << std::endl;
    }

    return placementString.str();
}

OperatorNodePtr AdaptiveActiveStandby::createReplica(const OperatorNodePtr& primaryOperator) {
    NES_DEBUG("AdaptiveActiveStandby: Replicating primary operator " << primaryOperator->toString());

    OperatorNodePtr replica = primaryOperator->copy(); // shallow copy of primary
    replica->setId(replica->getId() + newOperatorNodeIdStart);

    auto primaryOperatorId = primaryOperator->getId();
    auto replicaId = replica->getId();
    // save in secondaryOperatorMap
    secondaryOperatorMap[replicaId] = replica;
    // add property: secondary
    replica->addProperty(SECONDARY, true);
    // add references to each other
    replica->addProperty(PRIMARY_OPERATOR_ID, primaryOperatorId);
    primaryOperator->addProperty(SECONDARY_OPERATOR_ID, replicaId);
    /// add property about where primary is pinned? might not be necessary

    // remove some inherited properties that are only valid for the primary
    if (replica->hasProperty(PINNED_NODE_ID))
        replica->removeProperty(PINNED_NODE_ID);

    if (replica->hasProperty(PLACED))
        replica->removeProperty(PLACED);

    replica->clear();

    // set children
    for (const auto& childNode : primaryOperator->getChildren()) {
        auto primaryChild = childNode->as<OperatorNode>();
        // children of the secondary = children of the primary that are placed on source nodes + replicas of every other child
        auto primaryChildPlacement = findNodeWherePinned(primaryChild);
        if (sourceNodes.contains(primaryChildPlacement->getId())) {      // on source nodes
            replica->addChild(primaryChild);
        } else {
            if (primaryChild->hasProperty(SECONDARY_OPERATOR_ID)) {    // replicas
                auto secondaryChild =
                    secondaryOperatorMap[std::any_cast<uint64_t>(primaryChild->getProperty(SECONDARY_OPERATOR_ID))];
                replica->addChild(secondaryChild);
            } else {
                NES_WARNING("AdaptiveActiveStandby: ERROR: a secondary child is missing");
            }
        }
    }

    // set primary parents if on the root
    for (const auto& primaryParent : primaryOperator->getParents()) {
        if (findNodeWherePinned(primaryParent)->getId() == topology->getRoot()->getId()) {
            replica->addParent(primaryParent);
        }
    }

    NES_DEBUG("AdaptiveActiveStandby: Created secondary operator " << replica->toString()
                                                                   << " for primary operator " << primaryOperator->toString());

    NES_DEBUG("AdaptiveActiveStandby: secondary's children:");
    for (const auto& child : replica->getChildren()) {
        NES_DEBUG(child->toString());
    }

    NES_DEBUG("AdaptiveActiveStandby: secondary's parents:");
    for (const auto& parent : replica->getParents()) {
        NES_DEBUG(parent->toString());
    }

    return replica;
}

void AdaptiveActiveStandby::deleteReplicas() {
    for (const auto& [secondaryId, secondaryOperator]: secondaryOperatorMap) {
        secondaryOperator->removeAllParent();
        secondaryOperator->removeChildren();
    }
}

double AdaptiveActiveStandby::evaluateSinglePlacement(const OperatorNodePtr& secondaryOperator,
                                                      const TopologyNodePtr& topologyNode) {
    // NOTE: (currently) compare to all children and parent operators
    //       - every link between replicas will be counted twice in the end
    //          - good: if we evaluate a move of a single operator, it is enough to get its saved score
    //              - it includes all relevant links (parents & children)
    //              - if we only saved scores compared to children ops:
    //                  - links between current node and its parents would have to be reevaluated
    //                  - because parents' score would also include other (all) children
    //              -> this way: extra work when updating (= evaluating all parents & children and doing moves)
    //                  - because we will have to look in both directions (up & down) for each node
    //                  - BUT when we evaluate a move, we can get score of current placement easily
    //              -> other way, by saving scores to children:
    //                  - for every single move - fully evaluate parents
    //                      - because: score of current placement regarding children is saved, but regarding parents is not
    //              => much more moves are evaluated than executed, better to count everything twice

    // make sure it is indeed a secondary operator
    if (!secondaryOperatorMap.contains(secondaryOperator->getId()))
        return 0.0;

    NES_DEBUG("AdaptiveActiveStandby: Evaluating placement of replica "
              << secondaryOperator->toString() << " if it were placed on node " << topologyNode->toString());

    double scoreToParents = 0.0;
    double scoreToChildren = 0.0;

    auto outputSecondary = getOperatorOutput(secondaryOperator);

    // 1. iterate over all parents
    for (const auto& parentNode: secondaryOperator->getParents()) {
        auto parentOperator = parentNode->as<OperatorNode>();
        auto parentOperatorsNode = findNodeWherePinned(parentOperator);
        // network cost = output * distance of nodes
        auto distance = getDistance(topologyNode, parentOperatorsNode);
        scoreToParents += outputSecondary * distance;
    }

    // 2. iterate over all children
    for (const auto& childNode: secondaryOperator->getChildren()) {
        auto childOperator = childNode->as<OperatorNode>();
        auto childOperatorsNode = findNodeWherePinned(childOperator);
        // network cost = output * distance of nodes
        auto output = getOperatorOutput(childOperator);
        auto distance = getDistance(childOperatorsNode, topologyNode);
        scoreToChildren += output * distance;
    }

    auto score = networkCostWeight * (scoreToParents + scoreToChildren);

    NES_DEBUG("AdaptiveActiveStandby: Evaluated single placement of secondary operator "
              << secondaryOperator->toString() << " to topology node "
              << topologyNode->toString() << ". Score: " << score
              << " (to parents: " << networkCostWeight * scoreToParents
              << ", to children: " << networkCostWeight * scoreToChildren << ")");

    return score;
}

double AdaptiveActiveStandby::evaluateSinglePlacement(const OperatorNodePtr& secondaryOperator) {
    // make sure it is indeed a secondary operator
    if (!secondaryOperatorMap.contains(secondaryOperator->getId()))
        return 0.0;

    auto topologyNode = findNodeWherePinned(secondaryOperator);

    NES_DEBUG("AdaptiveActiveStandby: Evaluating placement of replica "
              << secondaryOperator->toString() << " on its current node " << topologyNode->toString());

    double scoreToParents = 0.0;    // to save
    double scoreToChildren = 0.0;   // to save
    double totalScore = 0.0;    // to return

    auto outputSecondary = getOperatorOutput(secondaryOperator);

    // 1. iterate over all parents
    for (const auto& parentNode: secondaryOperator->getParents()) {
        auto parentOperator = parentNode->as<OperatorNode>();
        auto parentOperatorsNode = findNodeWherePinned(parentOperator);
        // network cost = output * distance of nodes
        auto distance = getDistance(topologyNode, parentOperatorsNode);
        scoreToParents += outputSecondary * distance;
        if (!secondaryOperatorMap.contains(parentOperator->getId()))
            totalScore += outputSecondary * distance;
        else
            totalScore += outputSecondary * distance / 2.0;     // will be counted twice for total evaluation of placement
    }

    // 2. iterate over all children
    for (const auto& childNode: secondaryOperator->getChildren()) {
        auto childOperator = childNode->as<OperatorNode>();
        auto childOperatorsNode = findNodeWherePinned(childOperator);
        // network cost = output * distance of nodes
        auto output = getOperatorOutput(childOperator);
        auto distance = getDistance(childOperatorsNode, topologyNode);
        scoreToChildren += output * distance;
        if (!secondaryOperatorMap.contains(childOperator->getId()))
            totalScore += output * distance;
        else
            totalScore += output * distance / 2.0;         // will be counted twice for total evaluation of placement
    }

    // NOTE: score of total evaluation != sum of individual saved operator scores
    candidateOperatorToTopologyMap[secondaryOperator->getId()].second = networkCostWeight * (scoreToParents + scoreToChildren);

    NES_DEBUG("AdaptiveActiveStandby: Evaluated single placement of secondary operator "
              << secondaryOperator->toString() << " to topology node "
              << topologyNode->toString() << ". Score: " << networkCostWeight * (scoreToParents + scoreToChildren)
              << " (to parents: " << networkCostWeight * scoreToParents
              << ", to children: " << networkCostWeight * scoreToChildren << ")");

    return networkCostWeight * totalScore;
}

std::set<TopologyNodeId> AdaptiveActiveStandby::getAllTopologyNodesByIdWherePinned(const std::vector<NodePtr>& operatorNodes) {
    std::set<TopologyNodeId> topologyNodeIds;

    for (const auto& currentNode : operatorNodes) {
        auto currentOperator = currentNode->as<OperatorNode>();
        auto currentOperatorId = currentOperator->getId();
        TopologyNodeId currentTopologyNodeId;
        // check if it is a secondary operator
        if (isSecondary(currentOperator))
            currentTopologyNodeId = candidateOperatorToTopologyMap[currentOperatorId].first;
        else    // primary
            currentTopologyNodeId = std::any_cast<uint64_t>(currentOperator->getProperty(PINNED_NODE_ID));
        topologyNodeIds.insert(currentTopologyNodeId);
    }

    return topologyNodeIds;
}

double AdaptiveActiveStandby::evaluateBestPath(const TopologyNodePtr& startNode, TopologyNodeId nodeIdToExclude) {
    auto bestScore = std::numeric_limits<double>::max(); // worst case

    if (startNode->getParents().empty())
        return 0.0;

    for (const auto& parentNode : startNode->getParents()) {
        auto parentTopologyNode = parentNode->as<TopologyNode>();

        if (parentTopologyNode->getId() != nodeIdToExclude) {
            auto bandwidth = startNode->getLinkProperty(parentTopologyNode)->bandwidth;
            auto currentScore = 1.0 / bandwidth + evaluateBestPath(parentTopologyNode, nodeIdToExclude);

            if (currentScore < bestScore) // minimization
                bestScore = currentScore;
        }
    }

    NES_DEBUG("AdaptiveActiveStandby: Evaluated best path in subgraph starting from topology node " << startNode->getId()
                                                                                                    << ". Score: " << bestScore );
    return bestScore;
}

bool AdaptiveActiveStandby::isSecondary(const OperatorNodePtr& operatorNode) {
    return operatorNode->hasProperty(SECONDARY) && std::any_cast<bool>(operatorNode->getProperty(SECONDARY));
}

TopologyNodePtr AdaptiveActiveStandby::findNodeWherePinned(const NodePtr& operatorNode) {
    return findNodeWherePinned(operatorNode->as<OperatorNode>());
}

TopologyNodePtr AdaptiveActiveStandby::findNodeWherePinned(const OperatorNodePtr& operatorNode) {
    // secondary
    if (isSecondary(operatorNode))
        return topology->findNodeWithId(candidateOperatorToTopologyMap[operatorNode->getId()].first);
    // primary
    else
        return topology->findNodeWithId(std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID)));
}

OperatorNodePtr AdaptiveActiveStandby::getPrimaryOperatorOfSecondary(const OperatorNodePtr& secondaryOperator) {
    return primaryOperatorMap[std::any_cast<uint64_t>(secondaryOperator->getProperty(PRIMARY_OPERATOR_ID))];
}

bool AdaptiveActiveStandby::pinOperators() {

    for (const auto& [operatorId, val]: candidateOperatorToTopologyMap) {
        auto operatorNode = secondaryOperatorMap[operatorId];
        auto topologyNodeId = val.first;
        operatorNode->addProperty(PINNED_NODE_ID, topologyNodeId);

        auto cost = AdaptiveActiveStandby::getOperatorCost(operatorNode);
        NES_DEBUG("AdaptiveActiveStandby: Reducing node " << topologyNodeId << "'s remaining CPU capacity by " << cost);
        // Reduce the processing capacity by its cost
        topology->reduceResources(topologyNodeId, cost);
    }

    return true;
}

bool AdaptiveActiveStandby::executeGreedyPlacement(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators) {
    NES_DEBUG("AdaptiveActiveStandby: starting greedy placement");
    // 1. start with first level of operators that are placed on a different node that the pinned operators
    std::set<NodePtr> closestAncestorsOnDifferentNodes;
    for (const auto& currentOperator : pinnedUpStreamOperators) {
        // 1.1 get the primary ancestors that are placed on a different topology node
        //     (only the first such ancestor is of interest on every parent path)
        getAllClosestAncestorsOnDifferentNodes(currentOperator, closestAncestorsOnDifferentNodes);
    }

    // priority queue in ascending order
    std::priority_queue<NodePtr, std::vector<NodePtr>, decltype(sortOperatorsPredicate)*> targetOperators(sortOperatorsPredicate);
    for (const auto& node: closestAncestorsOnDifferentNodes) {
        targetOperators.push(node);
    }

    auto targetOperatorsDebug = targetOperators;
    std::stringstream targetOperatorsString;
    while (!targetOperatorsDebug.empty()) {
        auto ancestor = targetOperatorsDebug.top()->as<Node>();
        targetOperatorsDebug.pop();
        targetOperatorsString << std::endl << ancestor->toString()
                              << ", output: " << getOperatorOutput(ancestor->as_if<OperatorNode>())
                              << ", cost: " << getOperatorCost(ancestor->as_if<OperatorNode>());
    }
    NES_DEBUG("AdaptiveActiveStandby: sorted closest ancestors on different nodes: " << targetOperatorsString.str());

    // 2. iterate over all target primary operators
    while (!targetOperators.empty()) {

        // 2.1 get the first (lowest output or lowest cost) element, and remove from the targets
        auto currentNode = targetOperators.top()->as<Node>();
        targetOperators.pop();
        auto currentPrimary = currentNode->as<OperatorNode>();

        // 2.2 go next if a replica has already been created
        if (currentPrimary->hasProperty(SECONDARY_OPERATOR_ID))
            continue;

        auto currentPrimaryId = currentPrimary->getId();

        // 2.3 check where current node is pinned, go next if its on the root
        auto primaryTopologyNode = findNodeWherePinned(currentPrimary);
        if (primaryTopologyNode->getId() == topology->getRoot()->getId())
            continue;

        // 2.4 add current operator to primaryOperatorMap
        if (!primaryOperatorMap.contains(currentPrimaryId))
            primaryOperatorMap[currentPrimaryId] = currentPrimary;

        // 2.5 get candidate nodes with scores for placing replica
        auto candidateTopologyNodes = getCandidatePlacementsGreedy(currentPrimary);

        // 2.6 go next if no valid placement for current primary's replica (should not happen)
        if (candidateTopologyNodes.empty()) {
            NES_WARNING("AdaptiveActiveStandby: ERROR: no valid candidate for replica of " << currentPrimary->toString());
            continue;
        }

        // 2.7 create and place current replica on the best evaluated node
        auto secondaryOperator = createReplica(currentPrimary);

        auto bestCandidatePair = *(std::min_element(candidateTopologyNodes.begin(), candidateTopologyNodes.end()));
        pinSecondaryOperator(secondaryOperator->getId(), bestCandidatePair.first);

        // 2.8 add parents as new target operators in the sorted list if all necessary children have already been replicated
        for (const auto& parentNode: currentPrimary->getParents()) {
            // skip operators that are pinned to the sink
            if (findNodeWherePinned(parentNode)->getId() == topology->getRoot()->getId())
                continue;

            // only add if all secondary children are created OR primary children are on source nodes
            bool ready = true;

            for (const auto& childNode: parentNode->getChildren()) {
                auto childOperator = childNode->as<OperatorNode>();

                if (isSecondary(childOperator))
                    continue; // not a primary -> no replica needed

                if (sourceNodes.contains(findNodeWherePinned(childOperator)->getId()))
                    continue; // primary on a source node -> no replica needed

                if (!childOperator->hasProperty(SECONDARY_OPERATOR_ID)) {
                    ready = false;
                    NES_DEBUG("AdaptiveActiveStandby: " << childOperator->toString() << " has no secondary operator property");
                    break;
                }
            }
            if (ready) {
                NES_DEBUG("AdaptiveActiveStandby: adding " << parentNode->toString() << " to the queue");
                targetOperators.push(parentNode);
            }
        }
    }
    NES_DEBUG("AdaptiveActiveStandby: Greedy Placement has finished")
    return !secondaryOperatorMap.empty();
}

std::map<TopologyNodeId, double> AdaptiveActiveStandby::getCandidatePlacementsGreedy(const OperatorNodePtr& primaryOperator) {

    NES_DEBUG("AdaptiveActiveStandby: looking for placement candidates for replica of operator " << primaryOperator->toString());

    // return value: candidate node as key with the placement score as value
    std::map<TopologyNodeId, double> evaluatedCandidates;

    // 1. get the children of the secondary operator & their placements
    std::vector<OperatorNodePtr> childrenOperators;
    std::vector<TopologyNodePtr> childrenOperatorPlacements;
    for (const auto& childNode : primaryOperator->getChildren()) {
        auto primaryChild = childNode->as<OperatorNode>();
        // children of the secondary = children of the primary that are placed on source nodes + replicas of every other child
        auto primaryChildPlacement = findNodeWherePinned(primaryChild);
        if (sourceNodes.contains(primaryChildPlacement->getId())) {      // on source nodes
            childrenOperators.push_back(primaryChild);
            childrenOperatorPlacements.push_back(primaryChildPlacement);
        } else {
            if (primaryChild->hasProperty(SECONDARY_OPERATOR_ID)) {    // replicas
                auto secondaryChild =
                    secondaryOperatorMap[std::any_cast<uint64_t>(primaryChild->getProperty(SECONDARY_OPERATOR_ID))];
                childrenOperators.push_back(secondaryChild);
                childrenOperatorPlacements.push_back(findNodeWherePinned(secondaryChild));
            } else {
                NES_WARNING("AdaptiveActiveStandby: ERROR: no valid candidate placement can be found " <<
                            "because a secondary child is missing");
                return {};
            }
        }
    }

    std::stringstream secondaryChildrenString;
    TopologyNodePtr firstChildsNode = findNodeWherePinned(childrenOperators[0]);
    bool onTheSameNode = true;

    for (const auto& child: childrenOperators) {
        auto currentChildsNode = findNodeWherePinned(child);
        if (onTheSameNode)
            onTheSameNode = currentChildsNode == firstChildsNode;
        secondaryChildrenString << std::endl << child->toString() << " on " << currentChildsNode->toString();
    }
    NES_DEBUG("AdaptiveActiveStandby: children of the secondary and their placements:" << secondaryChildrenString.str());

    // 2. get all valid candidates
    // 2.1 get first set of candidates
    std::deque<TopologyNodePtr> candidateNodes;
    // all closest common ancestors of the children placements
    for (const auto& ancestor: topology->findAllClosestCommonAncestors(childrenOperatorPlacements, false)) {
        candidateNodes.push_back(ancestor);
    }


    // 2.2 filter current candidates for validity & evaluate valid ones
    while (!candidateNodes.empty()) {
        auto candidateNode = candidateNodes.front()->as<TopologyNode>();
        candidateNodes.pop_front();

        NES_DEBUG("AdaptiveActiveStandby: examining node " << candidateNode->toString() << " as a candidate");

        // 2.2.1 no source nodes
        if (sourceNodes.contains(candidateNode->getId())) {
            NES_DEBUG("AdaptiveActiveStandby: candidate was a source node, skipping it and adding its parents to the queue");
            // add parents as candidates
            for (const auto& ancestor: candidateNode->getParents())
                candidateNodes.push_back(ancestor->as<TopologyNode>());
            continue;
        }

        // 2.2.2 secondary path to sink does not share nodes with primary path
        if (!separatePathExists(primaryOperator, candidateNode)) {// rules out primary's node as a candidate
            NES_DEBUG("AdaptiveActiveStandby: no separate path from candidate node, continuing with other candidates");
            continue;
        }

        // 2.2.3 not enough resources
        if (calculateAvailableResources(candidateNode) < getOperatorCost(primaryOperator)) {
            NES_DEBUG("AdaptiveActiveStandby: candidate does not have enough resources, also adding its parents to the queue");
            // add parents as candidates
            for (const auto& parent: candidateNode->getParents())
                candidateNodes.push_back(parent->as<TopologyNode>());
        }

        // 2.2.4 evaluate and save valid candidates
        // network cost for every child
        double score = 0.0;
        for (const auto& childOperator: childrenOperators) {
            auto childOperatorsNode = findNodeWherePinned(childOperator);
            // network cost = output * distance of nodes
            auto output = getOperatorOutput(childOperator);
            auto distance = getDistance(childOperatorsNode, candidateNode);
            score += output * distance;
        }

        // score only contains partial network cost which has not yet been weighted
        score *= networkCostWeight;

        // over-utilization penalty
        score += calculateOverUtilizationPenalty(candidateNode, primaryOperator);

        // save candidate
        evaluatedCandidates[candidateNode->getId()] = score;
    }

    if (evaluatedCandidates.empty()) {
        NES_WARNING("AdaptiveActiveStandby: No valid placement candidates for replica of " << primaryOperator->toString());
        return {};
    }

    // print candidate nodes for debugging
    std::stringstream candidateNodesString;

    for (const auto& currentCandidate : evaluatedCandidates) {
        candidateNodesString << std::endl
                             << currentCandidate.first
                             << ", score: " << currentCandidate.second;
    }
    NES_DEBUG("AdaptiveActiveStandby: Candidate node IDs for replica of operator "
              << primaryOperator->toString()
              << ": "
              << candidateNodesString.str());

    return evaluatedCandidates;
}

double AdaptiveActiveStandby::executeLocalSearch(std::chrono::milliseconds timeLeft) {
    std::set<OperatorId> operatorsWithNoImprovementSinceLastChange;
    double totalScoreImprovement = 0.0;
    auto start = std::chrono::steady_clock::now();
    std::chrono::milliseconds elapsedMilliseconds{0};

    // loop while there is time left
    while (elapsedMilliseconds < timeLeft) {
        NES_DEBUG("AdaptiveActiveStandby: Starting new iteration in Local Search, time left: "
                  << (timeLeft - elapsedMilliseconds).count() << "ms.");

        // 1. get a random secondary operator that has not been examined since the last change
        auto it = secondaryOperatorMap.begin();
        // NOTE: might be better to use C++11 random
        std::advance(it, rand() % secondaryOperatorMap.size());
        auto randomKey = it->first;
        auto currentSecondary = secondaryOperatorMap[randomKey];

        // NOTE: could be smarter to first remove operatorsWithNoImprovementSinceLastChange from the secondaryOperatorMap,
        // and then pick a random operator from that. Should not make a difference in smaller queries though
        while (operatorsWithNoImprovementSinceLastChange.contains(currentSecondary->getId())) {
            NES_DEBUG("AdaptiveActiveStandby: skipping " << currentSecondary->toString() << ", already examined");
            it = secondaryOperatorMap.begin();
            std::advance(it, rand() % secondaryOperatorMap.size());
            randomKey = it->first;
            currentSecondary = secondaryOperatorMap[randomKey];
        }

        NES_DEBUG("AdaptiveActiveStandby: attempting to improve placement of " << currentSecondary->toString());

        // 2. calculate the best valid step

        auto bestStepPair = getBestStepLocalSearch(currentSecondary);

        // 0 if no valid step
        double singleScoreImprovement = bestStepPair.second;

        if (singleScoreImprovement < 0) { // minimization
            // execute step
            LocalSearchStep step = bestStepPair.first;
            auto currentNodeId = step.first.second;
            auto currentOperatorIds = step.first.first;
            auto targetOperatorIds = step.second.first;
            auto targetNodeId = step.second.second;

            auto currentNode = topology->findNodeWithId(currentNodeId);
            auto targetNode = topology->findNodeWithId(targetNodeId);

            // 2.1 unpin & pin operators

            for (const auto& operatorId: currentOperatorIds)
                pinSecondaryOperator(operatorId, targetNodeId);

            for (const auto& operatorId: targetOperatorIds)
                pinSecondaryOperator(operatorId, currentNodeId);


            // 2.2 update scores

            // 2.2.1 topology nodes
            auto currentTopologyNodeScore =
                calculateOverUtilizationPenalty(currentNode);
            candidateTopologyToOperatorMap[currentNodeId].second = currentTopologyNodeScore;

            auto targetTopologyNodeScore =
                calculateOverUtilizationPenalty(targetNode);
            candidateTopologyToOperatorMap[targetNodeId].second = targetTopologyNodeScore;

            std::map<OperatorId, OperatorNodePtr> parentsAndChildren;
            // 2.2.2 moved operator nodes
            for (const auto& operatorId: currentOperatorIds) {
                auto operatorNode = secondaryOperatorMap[operatorId];
                // 2.2.2.1 evaluate the operator itself
                evaluateSinglePlacement(operatorNode);

                // 2.2.2.2 also collect all of the parents and children for evaluation
                for (const auto& parent: operatorNode->getParents()) {
                    auto parentOperator = parent->as<OperatorNode>();
                    parentsAndChildren[parentOperator->getId()] = parentOperator;
                }
                for (const auto& child: operatorNode->getChildren()) {
                    auto childOperator = child->as<OperatorNode>();
                    parentsAndChildren[childOperator->getId()] = childOperator;
                }
            }

            for (const auto& operatorId: targetOperatorIds) {
                auto operatorNode = secondaryOperatorMap[operatorId];
                // 2.2.2.1 evaluate the operator itself
                evaluateSinglePlacement(operatorNode);

                // 2.2.2.2 also collect all of the parents and children for evaluation
                for (const auto& parent: operatorNode->getParents()) {
                    auto parentOperator = parent->as<OperatorNode>();
                    parentsAndChildren[parentOperator->getId()] = parentOperator;
                }
                for (const auto& child: operatorNode->getChildren()) {
                    auto childOperator = child->as<OperatorNode>();
                    parentsAndChildren[childOperator->getId()] = childOperator;
                }
            }

            // 2.2.3 parents & children of moved operator nodes
            for (const auto& [operatorId, operatorNode]: parentsAndChildren) {
                evaluateSinglePlacement(operatorNode);
            }

            totalScoreImprovement += singleScoreImprovement;
            operatorsWithNoImprovementSinceLastChange.clear();
        } else {
            operatorsWithNoImprovementSinceLastChange.insert(currentSecondary->getId());
            if (operatorsWithNoImprovementSinceLastChange.size() >= secondaryOperatorMap.size()) {
                NES_DEBUG("AdaptiveActiveStandby: No more improvements found for any of the operators. " <<
                          "Terminating current local search");
                break;
            }
        }
        auto currentTime = std::chrono::steady_clock::now();
        elapsedMilliseconds = duration_cast<std::chrono::milliseconds>(currentTime - start);
    }
    return totalScoreImprovement;
}

std::pair<LocalSearchStep, double> AdaptiveActiveStandby::getBestStepLocalSearch(const OperatorNodePtr& secondaryToMove) {
    LocalSearchStep bestStep;
    double bestScoreChange = std::numeric_limits<double>::max(); // worst case
    std::vector<LocalSearchStep> steps;

    // 1. fetch some useful data
    // topology node where secondary is currently pinned
    auto currentNode = findNodeWherePinned(secondaryToMove);
    // primary operator
    auto primaryOperator = getPrimaryOperatorOfSecondary(secondaryToMove);
    // parents & children
    auto parentOperators = secondaryToMove->getParents();
    auto childrenOperators = secondaryToMove->getChildren();


    // 2. calculate valid simple steps: move or swap single replicas

    // 2.1 get all topology nodes where the upstream operators could send data
    // 2.1.1 get topology nodes where the children are pinned
    std::vector<TopologyNodePtr> childrenOperatorLocations;
    for (const auto& child: childrenOperators) {
        childrenOperatorLocations.push_back(findNodeWherePinned(child));
    }

    // 2.1.2 get all closest common ancestors of these nodes as initial candidates
    auto initialCandidateNodes = topology->findAllClosestCommonAncestors(childrenOperatorLocations, false);

    // 2.1.3 remove some invalid nodes before expanding (so that invalid branches are possibly not expanded)
    // nodes from which no separate path exists to the parents
    for (auto it = initialCandidateNodes.begin(); it != initialCandidateNodes.end(); ) {
        bool valid = true;
        auto candidateNode = *it;
        for (const auto& parentOperator: parentOperators) {
            // if no separate path even to a single parent -> invalid
            if (!separatePathExists(primaryOperator, candidateNode,
                                    findNodeWherePinned(parentOperator))) {
                it = initialCandidateNodes.erase(it);   // remove if invalid
                valid = false;
                break;
            }
        }
        if (valid)      // leave it in the set if valid
            ++it;
    }

    // 2.1.4 expand: get all ancestors of the current candidates (including themselves)
    std::set<TopologyNodePtr> candidateNodes;
    for (const auto& initialCandidateNode: initialCandidateNodes) {
        for (const auto& ancestorNode: initialCandidateNode->getAndFlattenAllAncestors()) {
            candidateNodes.insert(ancestorNode->as<TopologyNode>());
        }
    }

    // 2.1.5 remove all invalid nodes (similar to 2.1.3)
    // current node
    candidateNodes.erase(currentNode);

    // source nodes
    for (auto it = candidateNodes.begin(); it != candidateNodes.end(); ) {
        auto candidateNode = *it;
        if (sourceNodes.contains(candidateNode->getId()))
            it = candidateNodes.erase(it);
        else
            ++it;
    }

    // nodes from which no separate path exists to the parents
    for (auto it = candidateNodes.begin(); it != candidateNodes.end(); ) {
        bool valid = true;
        auto candidateNode = *it;
        for (const auto& parentOperator: parentOperators) {
            // if no separate path even to a single parent -> invalid
            if (!separatePathExists(primaryOperator, candidateNode,
                                    findNodeWherePinned(parentOperator))) {
                it = candidateNodes.erase(it);
                valid = false;
                break;
            }
        }
        if (valid)
            ++it;
    }

    // candidateNodes now contains all valid nodes to which secondaryToMove could be moved

    // print candidate nodes for debugging
    std::stringstream candidateNodesString;

    for (const auto& currentCandidate : candidateNodes) {
        candidateNodesString << std::endl
                             << currentCandidate->toString();
    }
    NES_DEBUG("AdaptiveActiveStandby: Candidate node IDs for moving replica "
              << secondaryToMove->toString()
              << ": "
              << candidateNodesString.str());


    // 2.2 evaluate all simple steps to candidate nodes
    for (const auto& candidateNode: candidateNodes) {
        // 2.2.1 check simple move
        auto simpleStepPair =
            evaluateSimpleStepLocalSearch(secondaryToMove, currentNode, candidateNode);
        if (simpleStepPair.second < bestScoreChange) {
            bestScoreChange = simpleStepPair.second;
            bestStep = simpleStepPair.first;
        }
        // 2.2.2 check for possible swaps
        auto operatorIdsOnCandidateNode = candidateTopologyToOperatorMap[candidateNode->getId()].first;
        for (const auto& operatorIdOnCandidateNode: operatorIdsOnCandidateNode) {
            // look for valid swaps between secondaryToMove and secondaries on candidateNode
            if (secondaryOperatorMap.contains(operatorIdOnCandidateNode)) {
                NES_DEBUG("AdaptiveActiveStandby: Checking if " << secondaryToMove->toString() << " could be swapped with " <<
                          secondaryOperatorMap[operatorIdOnCandidateNode]->toString());
                // valid swap: target secondary would be validly placed on currentNode
                // -> no parent / child relation
                // -> has path to all parents, separate from primaries

                // make sure that candidate operator is not an ancestor of secondaryToMove
                auto ancestorsOfSecondaryToMove = secondaryToMove->getAndFlattenAllAncestors();
                auto found1 = std::find_if(ancestorsOfSecondaryToMove.begin(), ancestorsOfSecondaryToMove.end(),
                                          [&](const NodePtr& ancestor) {
                                               return operatorIdOnCandidateNode == ancestor->as<OperatorNode>()->getId();
                                           });

                if (found1 != ancestorsOfSecondaryToMove.end()) {
                    continue;
                }

                // make sure that candidate operator is not a child of secondaryToMove
                auto allChildrenOfSecondaryToMove = secondaryToMove->getAndFlattenAllChildren(false);
                auto found2 = std::find_if(allChildrenOfSecondaryToMove.begin(), allChildrenOfSecondaryToMove.end(),
                                          [&](const NodePtr& child) {
                                               return operatorIdOnCandidateNode == child->as<OperatorNode>()->getId();
                                          });

                if (found2 != allChildrenOfSecondaryToMove.end()) {
                    continue;
                }

                // check if candidate would have separate paths to parents & children
                bool valid = true;

                auto candidateOperator = secondaryOperatorMap[operatorIdOnCandidateNode];
                auto candidatesPrimaryOperator = getPrimaryOperatorOfSecondary(candidateOperator);

                for (const auto& parentOperator: candidateOperator->getParents()) {
                    // if no separate path even to a single parent -> invalid
                    if (!separatePathExists(candidatesPrimaryOperator, currentNode,
                                            findNodeWherePinned(parentOperator))) {
                        valid = false;
                        break;
                    }
                }

                // if valid so far
                if (valid) {
                    for (const auto& childNode: candidateOperator->getChildren()) {
                        // if no separate path even from a single child -> invalid

                        auto childOperator = childNode->as<OperatorNode>();
                        auto childOperatorsLocation = findNodeWherePinned(childOperator);
                        // if the child is a secondary -> check for separate path
                        if (isSecondary(childOperator)) {
                            if (!separatePathExists(getPrimaryOperatorOfSecondary(childOperator),
                                                    childOperatorsLocation,
                                                    currentNode)) {
                                NES_DEBUG("AdaptiveActiveStandby: swap invalid");
                                valid = false;
                                break;
                            }
                        } else { // if the child is a primary -> just check for a path
                            if (childOperatorsLocation->getId() != candidateNode->getId() &&
                                !childOperatorsLocation->containAsParent(currentNode)) {
                                NES_DEBUG("AdaptiveActiveStandby: swap invalid");
                                valid = false;
                                break;
                            }
                        }
                    }
                }

                // if completely valid
                if (valid) {
                    // evaluate swap
                    simpleStepPair =
                        evaluateSimpleStepLocalSearch(secondaryToMove, currentNode,
                                                      candidateNode, candidateOperator);
                    if (simpleStepPair.second < bestScoreChange) {
                        bestScoreChange = simpleStepPair.second;
                        bestStep = simpleStepPair.first;
                    }
                }
            }
        }
    }

    // IDEA: complex steps - move or swap operator groups
    // e.g. operator group: collection of operators that are part of the same query and are placed on the same topology node
    // - it would increase search space
    //  -> allows exploring more solutions and potentially finding better ones than currently
    //  -> substantially slows the optimization process

    // 4. return best pair
    return std::make_pair(bestStep, bestScoreChange);
}

std::pair<LocalSearchStep, double> AdaptiveActiveStandby::evaluateSimpleStepLocalSearch(const OperatorNodePtr& currentSecondary,
                                                                                        const TopologyNodePtr& currentNode,
                                                                                        const TopologyNodePtr& targetNode,
                                                                                        const OperatorNodePtr& targetSecondary) {

    std::vector<OperatorId> currentSecondaryVector = {currentSecondary->getId()};
    std::vector<OperatorId> targetSecondaryVector;

    // scoreChange = new score - old score

    // 1. currentSecondary
    // 1.1 operator related score change of currentSecondary
    auto scoreChange = evaluateSinglePlacement(currentSecondary, targetNode) -
        candidateOperatorToTopologyMap[currentSecondary->getId()].second;
    auto reserveResourcesTargetNode = getOperatorCost(currentSecondary);

    // 2. targetSecondary, if there is one
    if (targetSecondary != nullptr) {
        // 2.1 operator related score change of targetSecondary
        scoreChange += evaluateSinglePlacement(targetSecondary, currentNode) -
            candidateOperatorToTopologyMap[targetSecondary->getId()].second;

        // 2.2. consider secondary's cost too
        reserveResourcesTargetNode -= getOperatorCost(targetSecondary);

        targetSecondaryVector.push_back(targetSecondary->getId());
    }

    // 3. topology node related score change of currentNode
    scoreChange += calculateOverUtilizationPenalty(currentNode, - reserveResourcesTargetNode) -
        candidateTopologyToOperatorMap[currentNode->getId()].second;

    // 4. topology node related score change of targetNode
    scoreChange += calculateOverUtilizationPenalty(targetNode, reserveResourcesTargetNode) -
        candidateTopologyToOperatorMap[targetNode->getId()].second;

    LocalSearchStep step = std::make_pair(
        std::make_pair(currentSecondaryVector,currentNode->getId()),
        std::make_pair(targetSecondaryVector, targetNode->getId())
        );

    NES_DEBUG("AdaptiveActiveStandby: Evaluated the following local search simple step: " << localSearchStepToString(step)
                                                                                          << "Score change: " << scoreChange);

    return std::make_pair(step, scoreChange);
}


std::string AdaptiveActiveStandby::localSearchStepToString(const LocalSearchStep& step) {
    std::stringstream stepString;
    stepString << std::endl << "Secondary operator ID -> Topology Node ID" << std::endl;

    for (const auto& operatorId: step.first.first) {
        stepString << operatorId << " -> " << step.second.second << std::endl;
    }

    for (const auto& operatorId: step.second.first) {
        stepString << operatorId << " -> " << step.first.second << std::endl;
    }

    return stepString.str();
}


double AdaptiveActiveStandby::executeILPStrategy(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators) {
    const double failure = -1;
    z3::optimize opt(*z3Context);
    std::map<std::string, z3::expr> placementVariables;
    std::map<OperatorId, z3::expr> operatorDistanceMap;
    std::map<TopologyNodeId, z3::expr> nodeUtilizationMap;

    NES_DEBUG("AdaptiveActiveStandby: starting ILP strategy");

    // 1. populate maps, replicate all operators that are not placed on sources or sinks

    // 1.1 collect first level of primaries that are placed on a different node that the pinned operators
    std::set<NodePtr> closestAncestorsOnDifferentNodes;
    for (const auto& currentOperator : pinnedUpStreamOperators) {
        getAllClosestAncestorsOnDifferentNodes(currentOperator, closestAncestorsOnDifferentNodes);
    }

    std::deque<NodePtr> targetOperators (closestAncestorsOnDifferentNodes.begin(), closestAncestorsOnDifferentNodes.end());

    // 1.2 iterate over all target primary operators
    while (!targetOperators.empty()) {

        // 1.2.1 get the first element, and remove from the targets
        auto currentNode = targetOperators.front()->as<Node>();
        targetOperators.pop_front();
        auto currentPrimary = currentNode->as<OperatorNode>();

        // 1.2.2 go next if a replica has already been created
        if (currentPrimary->hasProperty(SECONDARY_OPERATOR_ID))
            continue;

        auto currentPrimaryId = currentPrimary->getId();

        // 1.2.3 check where current node is pinned, go next if its on the root
        auto primaryTopologyNode = findNodeWherePinned(currentPrimary);
        if (primaryTopologyNode->getId() == topology->getRoot()->getId())
            continue;

        // 1.2.4 add current operator to primaryOperatorMap
        if (!primaryOperatorMap.contains(currentPrimaryId))
            primaryOperatorMap[currentPrimaryId] = currentPrimary;

        // 1.2.5 replicate current primary
        auto secondaryOperator = createReplica(currentPrimary);

        // 1.2.6 add parents as new target operators in the queue if all necessary children have already been replicated
        for (const auto& parentNode: currentPrimary->getParents()) {
            // skip operators that are pinned to the sink
            if (findNodeWherePinned(parentNode)->getId() == topology->getRoot()->getId())
                continue;

            // only add if all secondary children are created OR primary children are on source nodes
            bool ready = true;

            for (const auto& childNode: parentNode->getChildren()) {
                auto childOperator = childNode->as<OperatorNode>();

                if (isSecondary(childOperator))
                    continue; // not a primary -> no replica needed

                if (sourceNodes.contains(findNodeWherePinned(childOperator)->getId()))
                    continue; // primary on a source node -> no replica needed

                if (!childOperator->hasProperty(SECONDARY_OPERATOR_ID)) {
                    ready = false;
                    NES_DEBUG("AdaptiveActiveStandby: " << childOperator->toString() << " has no secondary operator property");
                    break;
                }
            }
            if (ready) {
                NES_DEBUG("AdaptiveActiveStandby: adding " << parentNode->toString() << " to the queue");
                targetOperators.push_back(parentNode);
            }
        }
    }

    // 1.3 make sure that there are operators that can be replicated
    if (secondaryOperatorMap.empty())
        return failure;


    // 2. construct the placementVariable, compute distance, utilization and mileages
    for (const auto& pinnedUpStreamOperator: pinnedUpStreamOperators) {

        // 2.1 find all secondary paths from pinned upstream operators to the sink
        std::vector<NodePtr> operatorPath;
        operatorPath.push_back(pinnedUpStreamOperator);
        while (!operatorPath.back()->getParents().empty()) {

            auto& operatorToProcess = operatorPath.back();

            // skip further processing if found an operator on the sink node
            bool isPrimary = !isSecondary(operatorToProcess->as<OperatorNode>());

            if (isPrimary && findNodeWherePinned(operatorToProcess)->getId() == topology->getRoot()->getId()) {
                NES_DEBUG("AdaptiveActiveStandby: ILP found first primary operator on the sink. " <<
                          "Skipping further downstream operators.");
                break;
            }

            // look for the next downstream operators and add them to the path.
            auto downstreamOperators = operatorToProcess->getParents();

            if (downstreamOperators.empty()) {
                NES_ERROR("AdaptiveActiveStandby: ILP unable to find pinned downstream operator.");
                return failure;
            }

            // secondary operators are the only ones that are unpinned
            // tree-like structure in primaries => tree-like structure in secondaries too
            uint16_t unpinnedDownStreamOperatorCount = 0;
            for (const auto& downstreamOperator : downstreamOperators) {

                // FIXME: (issue #2290) Assuming a tree structure, hence a node can only have a single parent.
                //  However, a query can have multiple sinks or parents.
                if (unpinnedDownStreamOperatorCount > 1) {
                    NES_ERROR("AdaptiveActiveStandby: Current ILP implementation cannot place plan with multiple " <<
                              "downstream operators.");
                    return failure;
                }

                // only include secondary operators in the path and finally a primary that is on the sink node
                if (isSecondary(downstreamOperator->as<OperatorNode>()) ||
                    findNodeWherePinned(downstreamOperator)->getId() == topology->getRoot()->getId()) {
                    operatorPath.push_back(downstreamOperator);
                    unpinnedDownStreamOperatorCount++;
                }
            }
        }

        // 2.2 find path between pinned upstream and downstream topology node
        auto upstreamTopologyNode = findNodeWherePinned(pinnedUpStreamOperator);

        auto nodeIdsToExclude = getNodeIdsToExcludeToTarget(pinnedUpStreamOperator, topology->getRoot());
        for (const auto& [srcNodeId, srcNode]: sourceNodes) {
            if (nodeIdsToExclude.contains(srcNodeId))
                nodeIdsToExclude.erase(srcNodeId);
        }
        nodeIdsToExclude.erase(topology->getRoot()->getId());

        // FIXME #2290: path with multiple parents not supported
        // workaround: just pick one path
        // if it gets supported, use topology->findAllPathsBetween
        std::vector<TopologyNodePtr> topologyPath = topology->findPathBetween({upstreamTopologyNode},
                                                                              {topology->getRoot()},
                                                                              nodeIdsToExclude);


        while (!topologyPath.back()->getParents().empty()) {
            topologyPath.emplace_back(topologyPath.back()->getParents()[0]->as<TopologyNode>());
        }

        // 2.3 Add constraints to Z3 solver and compute operator distance, node utilization, and node mileage map
        addConstraintsILP(opt,
                          operatorPath,
                          topologyPath,
                          placementVariables,
                          operatorDistanceMap,
                          nodeUtilizationMap);
    }

    // 3. calculate the network cost (= sum over all operators (output of operator * distance of nodes))
    auto cost_net = z3Context->int_val(0);  // initialize the network cost with 0

    for (const auto& [operatorId, position] : operatorDistanceMap) {
        OperatorNodePtr operatorNode = operatorMapILP[operatorId]->as<OperatorNode>();
        if (operatorNode->getParents().empty()) {
            continue;
        }

        // loop over downstream operators and compute network cost
        for (const auto& downStreamOperator : operatorNode->getParents()) {
            OperatorId downStreamOperatorId = downStreamOperator->as_if<OperatorNode>()->getId();
            // only consider relevant nodes
            if (operatorMapILP.find(downStreamOperatorId) != operatorMapILP.end()) {

                auto distance = position - operatorDistanceMap.find(downStreamOperatorId)->second;
                NES_DEBUG("distance: " << operatorId << " " << distance);

                double output = getOperatorOutput(operatorNode);

                cost_net = cost_net + z3Context->real_val(std::to_string(output).c_str()) * distance;
            }
        }
    }
    NES_DEBUG("cost_net: " << cost_net);

    // 4. calculate the node over-utilization penalties
    auto overUtilizationPenalty = z3Context->int_val(0);   // initialize the over-utilization cost with 0
    for (auto const& [topologyId, utilization] : nodeUtilizationMap) {
        std::string overUtilizationId = "S" + std::to_string(topologyId);
        // an integer expression of the slack
        auto currentOverUtilization = z3Context->int_const(overUtilizationId.c_str());

        // obtain the available slot in the current node
        auto topologyNode = topology->findNodeWithId(topologyId);
        int availableSlots = topologyNode->getAvailableResources();

        opt.add(currentOverUtilization >= 0);   // we only penalize over-utilization, hence its value should be >= 0.
        opt.add(utilization - currentOverUtilization <= availableSlots);    // formula for the over-utilization

        overUtilizationPenalty = overUtilizationPenalty + currentOverUtilization;
    }

    auto weightOverUtilization = z3Context->real_val(std::to_string(overUtilizationPenaltyWeight).c_str());
    auto weightNetwork = z3Context->real_val(std::to_string(networkCostWeight/z3ScaleDistances).c_str());

    // 5. optimize ILP problem and print solution
    opt.minimize(weightNetwork * cost_net + weightOverUtilization * overUtilizationPenalty);

    // 6. check if a solution was found
    if (z3::sat != opt.check()) {
        NES_ERROR("AdaptiveActiveStandby: ILP Solver failed.");
        return failure;
    }

    // 7. get the model to retrieve the optimization solution
    auto z3Model = opt.get_model();
    auto score = z3Model.eval(weightNetwork * cost_net + weightOverUtilization * overUtilizationPenalty).get_decimal_string(10);
    NES_DEBUG("AdaptiveActiveStandby: ILP model: \n" << z3Model);
    NES_DEBUG("AdaptiveActiveStandby: ILP Solver found solution with cost: " << score);

    // 8. fill up the candidate maps according to the model
    for (auto const& [topologyString, P] : placementVariables) {
        if (z3Model.eval(P).get_numeral_int() == 1) {// means we place the operator in the node
            int operatorId = std::stoi(topologyString.substr(0, topologyString.find(KEY_SEPARATOR)));
            int topologyNodeId = std::stoi(topologyString.substr(topologyString.find(KEY_SEPARATOR) + 1));
            OperatorNodePtr operatorNode = operatorMapILP[operatorId];

            if (isSecondary(operatorNode)) {
                pinSecondaryOperator(operatorId, topologyNodeId);
                NES_DEBUG("AdaptiveActiveStandby: ILP solution: op " << operatorId << " -> node " << topologyNodeId);
            }
        }
    }

    return std::stod(score);
}

void AdaptiveActiveStandby::addConstraintsILP(z3::optimize& opt,
                                              std::vector<NodePtr>& operatorNodePath,
                                              std::vector<TopologyNodePtr>& topologyNodePath,
                                              std::map<std::string, z3::expr>& placementVariable,
                                              std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                              std::map<TopologyNodeId, z3::expr>& nodeUtilizationMap) {

    for (uint64_t i = 0; i < operatorNodePath.size(); i++) {
        OperatorNodePtr operatorNode = operatorNodePath[i]->as<OperatorNode>();
        OperatorId operatorId = operatorNode->getId();

        if (operatorMapILP.find(operatorId) != operatorMapILP.end()) {
            // initialize the path constraint variable to 0
            auto pathConstraint = z3Context->int_val(0);
            for (const auto& node: topologyNodePath) {
                TopologyNodePtr topologyNode = node->as<TopologyNode>();
                uint64_t topologyID = topologyNode->getId();
                std::string variableID = std::to_string(operatorId) + KEY_SEPARATOR + std::to_string(topologyID);
                auto iter = placementVariable.find(variableID);
                if (iter != placementVariable.end()) {
                    pathConstraint = pathConstraint + iter->second;
                }
            }
            opt.add(pathConstraint == 1);
            break;  // all following nodes already created
        }

        operatorMapILP[operatorId] = operatorNode;
        // fill the placement variable, utilization, and distance map
        auto sum_i = z3Context->int_val(0);
        auto D_i = z3Context->int_val(0);
        for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
            TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
            uint64_t topologyID = topologyNode->getId();

            // create placement variable and constrain it to {0,1}
            std::string variableID = std::to_string(operatorId) + KEY_SEPARATOR + std::to_string(topologyID);
            auto P_IJ = z3Context->int_const(variableID.c_str());
            if ((i == 0 && j == 0) || (i == operatorNodePath.size() - 1 && j == topologyNodePath.size() - 1)) {
                opt.add(P_IJ == 1); // fix the placement of source and sink
            } else {
                // binary decision on whether to place or not, hence constrained to be either 0 or 1
                opt.add(P_IJ == 0 || P_IJ == 1);
            }
            placementVariable.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;

            // add to node utilization
            auto slots = getOperatorCost(operatorNode);

            auto iterator = nodeUtilizationMap.find(topologyID);
            if (iterator != nodeUtilizationMap.end()) {
                iterator->second = iterator->second + slots * P_IJ;
            } else {
                // utilization of a node = slots (i.e. computing cost of operator) * placement variable
                nodeUtilizationMap.insert(std::make_pair(topologyID, slots * P_IJ));
            }

            // distance matrix would not work here if there are multiple paths
            // add distance to root through this path (positive part of distance equation)
            double M = 0;
            auto currentNode = topologyNode;
            while (!currentNode->getParents().empty()) {
                auto parentNode = currentNode->getParents()[0]->as<TopologyNode>();
                M += getDistance(currentNode, parentNode);
                currentNode = parentNode;
            }
            D_i = D_i + z3Context->real_val(std::to_string(z3ScaleDistances*M).c_str()) * P_IJ;
        }
        operatorDistanceMap.insert(std::make_pair(operatorId, D_i));
        // add constraint that operator is placed exactly once on topology path
        opt.add(sum_i == 1);
    }
}

}// namespace NES::Optimizer
