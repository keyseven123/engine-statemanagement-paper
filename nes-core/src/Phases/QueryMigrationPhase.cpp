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

#include <Phases/QueryMigrationPhase.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Topology/TopologyNode.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <Topology/Topology.hpp>
#include <Operators/OperatorId.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Phases/MigrationType.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <unordered_set>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <GRPC/WorkerRPCCLient>
#include "GRPC/WorkerRPCClient.hpp"

using namespace NES;

Experimental::QueryMigrationPhase::QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                                       TopologyPtr topology,
                                           WorkerRPCClientPtr workerRpcClient,
                                           NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),  workerRPCClient(std::move(workerRpcClient)),
      queryPlacementPhase(std::move(queryPlacementPhase)) {}

Experimental::QueryMigrationPhasePtr
Experimental::QueryMigrationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                               TopologyPtr topology,
                                               WorkerRPCClientPtr workerRPCClient,
                                               NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase) {
    return std::make_shared<QueryMigrationPhase>(QueryMigrationPhase(std::move(globalExecutionPlan),std::move(topology), std::move(workerRPCClient), std::move(queryPlacementPhase)));
}
bool Experimental::QueryMigrationPhase::execute(const MigrateQueryRequestPtr& req) {
    NES_DEBUG("QueryMigrationPhase: execute migration");
    TopologyNodeId markedTopNodeId = req->getTopologyNode();
    QueryId queryId = req->getQueryId();
    //check if path exists
    auto path = findPath(queryId,markedTopNodeId);
    if(path.empty()){
        NES_ERROR("QueryMigrationPhase: no path found between child and parent nodes. No path to migrate subqueries to");
        return false;
    }

    auto markedExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(markedTopNodeId);
    //subqueries of a query that need to be migrated
    auto subqueries = markedExecutionNode->getQuerySubPlans(queryId);
    globalExecutionPlan->removeExecutionNode(markedTopNodeId);
    if(req->getMigrationType() == MigrationType::MIGRATION_WITH_BUFFERING){
        return executeMigrationWithBuffer(subqueries, markedExecutionNode );
    }
    return executeMigrationWithoutBuffer(subqueries, markedExecutionNode);

}

bool Experimental::QueryMigrationPhase::executeMigrationWithBuffer(std::vector<QueryPlanPtr>& queryPlans, const ExecutionNodePtr& markedNode) {
    NES_ERROR("With Buffer");

    auto queryId = queryPlans[0]->getQueryId();

    for (const auto& queryPlan : queryPlans) {
        auto sourceOperators = queryPlan->getSourceOperators();
        std::map<OperatorId, Experimental::QueryMigrationPhase::InformationForFindingSink>  mapOfNetworkSourceIdToInfoForFindingNetworkSink = getInfoForAllSinks(sourceOperators, queryId);
        NES_DEBUG("QueryMigrationPhase: Sending buffer requests");
        sendBufferRequests(mapOfNetworkSourceIdToInfoForFindingNetworkSink);

        NES_ERROR("QueryMigrationPhase: Beginning Migration : " << time);
        std::vector<ExecutionNodePtr> exeNodes = executePartialPlacement(queryPlan);
        if(exeNodes.size() == 0){
            NES_ERROR("QueryMigrationPhase: No execution nodes made");
            throw Exceptions::RuntimeException("QueryMigrationPhase: No execution nodes created during partial placement");
        }
        for(auto& exeNode: exeNodes){
            NES_ERROR("QueryMigrationPhase: Occupied Resources on new Nodes: " <<exeNode->getOccupiedResources(1));
        }
        NES_ERROR("PartialP Placement successul");
        bool deploySuccess = deployQuery(queryId,exeNodes);
        if(!deploySuccess){
            NES_ERROR("QueryMigrationPhase: Issues while deploying execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        NES_ERROR("Deplyoment successul");
        bool startSuccess = startQuery(queryId,exeNodes);
        if(!startSuccess){
            NES_ERROR("QueryMigrationPhase: Issues while starting execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        //sleep(30);
        NES_ERROR("Starting successul");
        sendReconfigurationRequests(mapOfNetworkSourceIdToInfoForFindingNetworkSink, queryId,exeNodes);
        bool success = markedNode->removeSingleQuerySubPlan(queryId,queryPlan->getQuerySubPlanId());
        if(!success){
            throw Exceptions::RuntimeException("QueryMigrationPhase: Error while removing a QSP from an ExecutionNode. No such QSP found");
        }
    }
    globalExecutionPlan->removeExecutionNodeFromQueryIdIndex(queryId,markedNode->getId());
    NES_ERROR("QueryMigrationPhase: complete");
    return true;
}

bool Experimental::QueryMigrationPhase::executeMigrationWithoutBuffer(const std::vector<QueryPlanPtr>& queryPlans,const ExecutionNodePtr& markedNode) {
    NES_ERROR("WithOUT Buffer");
    auto queryId = queryPlans[0]->getQueryId();

    for (const auto& queryPlan : queryPlans) {
        auto sourceOperators = queryPlan->getSourceOperators();
        std::map<OperatorId, Experimental::QueryMigrationPhase::InformationForFindingSink>  mapOfNetworkSourceIdToInfoForFindingNetworkSink = getInfoForAllSinks(sourceOperators, queryId);

        NES_ERROR("QueryMigrationPhase: Beginning Migration : " << time);
        std::vector<ExecutionNodePtr> exeNodes = executePartialPlacement(queryPlan);
        if(exeNodes.size() == 0){
            NES_ERROR("QueryMigrationPhase: No execution nodes made");
            throw Exceptions::RuntimeException("QueryMigrationPhase: No execution nodes created during partial placement");
        }
        for(auto& exeNode: exeNodes){
            NES_ERROR("QueryMigrationPhase: Occupied Resources on new Nodes: " <<exeNode->getOccupiedResources(1));
        }
        NES_ERROR("QueryMigrationPhase: partial placement successful!");
        bool deploySuccess = deployQuery(queryId,exeNodes);
        if(!deploySuccess){
            NES_DEBUG("QueryMigrationPhase: Issues while deploying execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        NES_ERROR("QueryMigrationPhase: deployed migrated queries succesfully");
        bool startSuccess = startQuery(queryId,exeNodes);

        if(!startSuccess){
            NES_DEBUG("QueryMigrationPhase: Issues while starting execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        NES_ERROR("QueryMigrationPhase: started migrated queries succesfully");
        //sleep(30);
        sendReconfigurationRequests(mapOfNetworkSourceIdToInfoForFindingNetworkSink, queryId,exeNodes);
        bool success = markedNode->removeSingleQuerySubPlan(queryId,queryPlan->getQuerySubPlanId());
        if(!success){
            throw Exceptions::RuntimeException("QueryMigrationPhase: Error while removing a QSP from an ExecutionNode. No such QSP found");
        }
    }
    globalExecutionPlan->removeExecutionNodeFromQueryIdIndex(queryId,markedNode->getId());
    NES_DEBUG("QueryMigrationPhase: complete");
    return true;
}

std::vector<TopologyNodePtr> Experimental::QueryMigrationPhase::findPath(QueryId queryId, TopologyNodeId topologyNodeId) {
    auto childNodes = findChildExecutionNodesAsTopologyNodes(queryId,topologyNodeId);
    auto parentNodes = findParentExecutionNodesAsTopologyNodes(queryId, topologyNodeId);
    return topology->findPathBetween(childNodes,parentNodes);
}

std::vector<TopologyNodePtr> Experimental::QueryMigrationPhase::findChildExecutionNodesAsTopologyNodes(QueryId queryId,TopologyNodeId topologyNodeId) {
    std::vector<TopologyNodePtr>childNodes = {};
    auto allExecutionNodesInvolvedInAQuery = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    auto allChildNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getChildren();
    for(const auto& executionNode : allExecutionNodesInvolvedInAQuery){
        for(const auto& node: allChildNodesOfTopologyNode){
            auto nodeAsTopologyNode =node->as<TopologyNode>();
            if (executionNode->getId() == nodeAsTopologyNode->getId() ){
                childNodes.push_back(nodeAsTopologyNode);
            }
        }
    }
    return childNodes;
}

std::vector<TopologyNodePtr> Experimental::QueryMigrationPhase::findParentExecutionNodesAsTopologyNodes(QueryId queryId,TopologyNodeId topologyNodeId) {
    std::vector<TopologyNodePtr>parentNodes = {};
    auto allExecutionNodesInvolvedInAQuery = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    auto allParentNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getParents();
    for(const auto& executionNode : allExecutionNodesInvolvedInAQuery){
        auto id = executionNode->getId();
        auto it = std::find_if(allParentNodesOfTopologyNode.begin(), allParentNodesOfTopologyNode.end(), [id](NodePtr& node){

            return id == (node->as<TopologyNode>()->getId());
        });
        if(it != allParentNodesOfTopologyNode.end()){
            parentNodes.push_back(it->get()->as<TopologyNode>());
        }
    }
    return parentNodes;
}
TopologyNodePtr Experimental::QueryMigrationPhase::findNewNodeLocationOfNetworkSource(QueryId queryId, OperatorId sourceOperatorId,std::vector<ExecutionNodePtr>& potentialLocations) {
    for (const auto& exeNode : potentialLocations) {
        const auto& querySubPlans = exeNode->getQuerySubPlans(queryId);
        for (const auto& plan : querySubPlans) {
            auto sourceOperators = plan->getSourceOperators();
            for(const auto& op : sourceOperators){
                auto id = op->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition().getOperatorId();
                if(id== sourceOperatorId){
                    return exeNode->getTopologyNode();
                }
            }
        }
    }
    return {};
}
std::map<OperatorId,Experimental::QueryMigrationPhase::InformationForFindingSink> Experimental::QueryMigrationPhase::getInfoForAllSinks(const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators, QueryId queryId) {
    std::map<OperatorId,QueryMigrationPhase::InformationForFindingSink> mapOfNetworkSourceIdToInfoForFindingNetworkSink;
    for(const SourceLogicalOperatorNodePtr& sourceOperator:sourceOperators){
        auto networkSourceDescriptor = sourceOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>();
        TopologyNodeId sinkTopolgoyNodeId = networkSourceDescriptor->getSinkTopologyNode();
        TopologyNodePtr sinkTopologyNode = topology->findNodeWithId(sinkTopolgoyNodeId);
        //A pair of NetworkSource and Sink "share" a NESPartition. They each hold their own individual partition but are paired together once a Channel is established.
        //This pairing, in the end, is done over the OperatorId, which is also the OperatorId of the NetworkSink.
        //We make use of this fact to find the NetworkSink that will be told to bufferData/reconfigure
        //In the future, an ID for a NetworkSink/Source pair would be useful
        uint64_t partitionIdentifier = networkSourceDescriptor->getNesPartition().getOperatorId();
        SinkLogicalOperatorNodePtr sinkOperator;
        QuerySubPlanId qspOfNetworkSink;
        auto querySubPlansOnSinkTopologyNode = globalExecutionPlan->getExecutionNodeByNodeId(sinkTopolgoyNodeId)->getQuerySubPlans(queryId);
        for (const auto& plan : querySubPlansOnSinkTopologyNode) {
            auto sinkOperators = plan->getSinkOperators();
            bool found = false;
            for (const auto& sink : sinkOperators) {
                auto id = sink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNesPartition().getOperatorId();
                if (id == partitionIdentifier) {
                    sinkOperator = sink;
                    qspOfNetworkSink = plan->getQuerySubPlanId();
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (!sinkOperator) {
            NES_DEBUG("QueryMigrationPhase: No matching NetworkSink found to NetworkSource with partitionIdentifier "
                      << partitionIdentifier);
            throw Exception("QueryMigratrionPhase: Couldnt find mathcing NetworkSink.");
        }
        OperatorId globalOperatorIdOfNetworkSink = sinkOperator->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getGlobalId();
        QueryMigrationPhase::InformationForFindingSink info{};
        info.sinkTopologyNode = sinkTopolgoyNodeId;
        info.querySubPlanOfNetworkSink = qspOfNetworkSink;
        info.globalOperatorIdOfNetworkSink = globalOperatorIdOfNetworkSink;
        mapOfNetworkSourceIdToInfoForFindingNetworkSink[partitionIdentifier] = info;
    }
    return mapOfNetworkSourceIdToInfoForFindingNetworkSink;
}
bool Experimental::QueryMigrationPhase::sendBufferRequests(std::map<OperatorId, QueryMigrationPhase::InformationForFindingSink> map) {
    for(auto entry: map){
        auto info = entry.second;
        TopologyNodePtr sinkNode = topology->findNodeWithId(info.sinkTopologyNode);
        auto ipAddress = sinkNode->getIpAddress();
        auto grpcPort = sinkNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        //mapping on worker side must be done using globalId.
        workerRPCClient->bufferData(rpcAddress,info.querySubPlanOfNetworkSink,info.globalOperatorIdOfNetworkSink);
    }
    //TODO: error handling
    return true;
}
std::vector<ExecutionNodePtr> Experimental::QueryMigrationPhase::executePartialPlacement(QueryPlanPtr queryPlan) {
    std::unordered_set<ExecutionNodePtr> executionNodesCreatedForSubquery =
        queryPlacementPhase->executePartialPlacement("BottomUp", std::move(queryPlan));
    std::vector<ExecutionNodePtr> exeNodes(executionNodesCreatedForSubquery.begin(), executionNodesCreatedForSubquery.end());
    return exeNodes;
}
bool Experimental::QueryMigrationPhase::sendReconfigurationRequests(std::map<OperatorId,QueryMigrationPhase::InformationForFindingSink>& map, uint64_t queryId, std::vector<ExecutionNodePtr>& exeNodes) {
    for(auto entry: map){
        auto partitionId = entry.first;
        auto info = entry.second;
        TopologyNodePtr sinkNode = topology->findNodeWithId(info.sinkTopologyNode);
        TopologyNodePtr newSourceTopologyNode = findNewNodeLocationOfNetworkSource(queryId, partitionId, exeNodes);
        auto ipAddress = sinkNode->getIpAddress();
        auto grpcPort = sinkNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        workerRPCClient->updateNetworkSink(rpcAddress,
                                           newSourceTopologyNode->getId(),
                                           newSourceTopologyNode->getIpAddress(),
                                           newSourceTopologyNode->getDataPort(),
                                           info.querySubPlanOfNetworkSink,
                                           info.globalOperatorIdOfNetworkSink);
    }
    //TODO: add error handling
    return true;
}

bool Experimental::QueryMigrationPhase::deployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId=" << queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const ExecutionNodePtr& executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            NES_WARNING("QueryDeploymentPhase : unable to find query sub plan with id " << queryId);
            return false;
        }

        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress);

        for (auto& querySubPlan : querySubPlans) {
            //enable this for sync calls
            //bool success = workerRPCClient->registerQuery(rpcAddress, querySubPlan);
            bool success = workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
            if (success) {
                NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << " successful");
            } else {
                NES_ERROR("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << "  failed");
                return false;
            }
        }
        completionQueues[queueForExecutionNode] = querySubPlans.size();
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Register);
    //NES_DEBUG("QueryDeploymentPhase: Finished deploying execution plan for query with Id " << queryId << " success=" << result);
    return result;
}

bool Experimental::QueryMigrationPhase::startQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId=" << queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase::startQuery at execution node with id=" << executionNode->getId()

                                                                                << " and IP=" << ipAddress);
        //enable this for sync calls
        //bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        bool success = workerRPCClient->startQueryAsyn(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }

    bool result = workerRPCClient->checkAsyncResult(completionQueues, Start);
    //NES_DEBUG("QueryDeploymentPhase: Finished starting execution plan for query with Id " << queryId << " success=" << result);
    return result;
}
