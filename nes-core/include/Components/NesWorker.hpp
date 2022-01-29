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

#ifndef NES_INCLUDE_COMPONENTS_NESWORKER_HPP_
#define NES_INCLUDE_COMPONENTS_NESWORKER_HPP_

#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <future>
#include <vector>

namespace grpc {
class Server;
class ServerCompletionQueue;
};// namespace grpc

namespace NES {

class WorkerRPCServer;
class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

class MonitoringAgent;
using MonitoringAgentPtr = std::shared_ptr<MonitoringAgent>;

class NesWorker {
  public:
    /**
     * @brief default constructor which creates a sensor node
     * @note this will create the worker actor using the default worker config
     */
    explicit NesWorker(const Configurations::WorkerConfigurationPtr& workerConfig);

    /**
     * @brief default dtor
     */
    ~NesWorker();

    /**
     * @brief start the worker using the default worker config
     * @param blocking: bool indicating if the call is blocking
     * @param withConnect: bool indicating if connect
     * @return bool indicating success
     */
    bool start(bool blocking, bool withConnect);

    /**
     * @brief configure setup with set of parent id
     * @param parentId
     * @return bool indicating sucess
     */
    bool setWithParent(uint32_t parentId);

    /**
     * @brief stop the worker
     * @return bool indicating success
     */
    bool stop(bool force);

    /**
     * @brief connect to coordinator using the default worker config
     * @return bool indicating success
     */
    bool connect();

    /**
     * @brief disconnect from coordinator
     * @return bool indicating success
     */
    bool disconnect();

    /**
    * @brief method to deregister physical stream with the coordinator
    * @param logical and physical of the stream
     * @return bool indicating success
    */
    bool unregisterPhysicalStream(std::string logicalName, std::string physicalName);

    /**
    * @brief method add new parent to this node
    * @param parentId
    * @return bool indicating success
    */
    bool addParent(uint64_t parentId);

    /**
    * @brief method to replace old with new parent
    * @param oldParentId
    * @param newParentId
    * @return bool indicating success
    */
    bool replaceParent(uint64_t oldParentId, uint64_t newParentId);

    /**
    * @brief method remove parent from this node
    * @param parentId
    * @return bool indicating success
    */
    bool removeParent(uint64_t parentId);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return vector of queryStatistics
    */
    std::vector<Runtime::QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief method to get a ptr to the node engine
     * @return pt to node engine
     */
    Runtime::NodeEnginePtr getNodeEngine();

    /**
     * @brief method to get the id of the worker
     * @return id of the worker
     */
    TopologyNodeId getTopologyNodeId() const;

    /**
     * @brief Method to check if a worker is still running
     * @return running status of the worker
     */
    [[nodiscard]] bool isWorkerRunning() const noexcept;

    /**
     * @brief Method to let the Coordinator know that a Query failed
     * @param queryId of the failed query
     * @param subQueryId of the failed query
     * @param workerId of the worker that handled the failed query
     * @param operatorId of failed query
     * @param errorMsg to describe the reason of the failure
     * @return true if Notification was successful, false otherwise
     */
    bool notifyQueryFailure(uint64_t queryId, uint64_t subQueryId, uint64_t workerId, uint64_t operatorId, std::string errorMsg);

    uint64_t getWorkerId();

  private:
    /**
     * @brief method to register physical stream with the coordinator
     * @param physicalSources: physical sources containing relevant information
     * @return bool indicating success
     */
    bool registerPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources);

    /**
     * @brief this method will start the GRPC Worker server which is responsible for reacting to calls
     */
    void buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom);

    /**
     * @brief helper method to ensure client is connected before calling rpc functions
     * @return
     */
    bool waitForConnect() const;

    void handleRpcs(WorkerRPCServer& service);

    std::unique_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    std::unique_ptr<grpc::ServerCompletionQueue> completionQueue;

    Runtime::NodeEnginePtr nodeEngine;
    MonitoringAgentPtr monitoringAgent;
    CoordinatorRPCClientPtr coordinatorRpcClient;
    const Configurations::WorkerConfigurationPtr workerConfig;
    bool connected{false};
    bool withParent{false};
    uint32_t parentId;
    std::string rpcAddress;
    std::string coordinatorIp;
    std::string localWorkerIp;
    std::string workerToCoreMapping;
    std::string queuePinList;
    uint16_t coordinatorPort;
    uint16_t localWorkerRpcPort;
    uint16_t localWorkerZmqPort;
    uint16_t numberOfSlots;
    uint16_t numWorkerThreads;
    uint32_t numberOfBuffersInGlobalBufferManager;
    uint32_t numberOfBuffersPerWorker;
    uint32_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t bufferSizeInBytes;
    // indicates the compilation strategy of the query compiler [FAST|DEBUG|OPTIMIZE].
    std::string queryCompilerCompilationStrategy;
    // indicates the pipelining strategy for the query compiler [OPERATOR_FUSION, OPERATOR_AT_A_TIME].
    std::string queryCompilerPipeliningStrategy;
    // indicates, which output buffer allocation strategy should be used.
    std::string queryCompilerOutputBufferOptimizationLevel;
    bool enableNumaAwareness{false};
    bool enableMonitoring;
    std::atomic<bool> isRunning{false};
    TopologyNodeId topologyNodeId{INVALID_TOPOLOGY_NODE_ID};
};
using NesWorkerPtr = std::shared_ptr<NesWorker>;

}// namespace NES
#endif  // NES_INCLUDE_COMPONENTS_NESWORKER_HPP_
