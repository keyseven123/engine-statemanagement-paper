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

#ifndef NES_INCLUDE_COMPONENTS_NESWORKER_HPP_
#define NES_INCLUDE_COMPONENTS_NESWORKER_HPP_

#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Services/ReplicationService.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <future>
#include <vector>
#include <memory>
#include <optional>
#include <Common/GeographicalLocation.hpp>

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

class NesWorker: public detail::virtual_enable_shared_from_this<NesWorker>, public Exceptions::ErrorListener {
    using inherited0 = detail::virtual_enable_shared_from_this<NesWorker>;
    using inherited1 = ErrorListener;
  public:
    /**
     * @brief default constructor which creates a sensor node
     * @note this will create the worker actor using the default worker config
     */
    explicit NesWorker(Configurations::WorkerConfigurationPtr&& workerConfig);

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
    * @brief method to deregister physical source with the coordinator
    * @param logical and physical of the source
     * @return bool indicating success
    */
    bool unregisterPhysicalSource(std::string logicalName, std::string physicalName);

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
     * @brief check if a location was set for this Node
     */
    bool hasLocation();


    /**
     * @brief returns an optional containing a GeographicalLocation object if the node has a fixed location or
     * containing a nullopt_t if the node does not have a location
     * @return optional containing the GeographicalLocation
     */
    std::optional<GeographicalLocation> getCurrentOrPermanentGeoLoc();

    /**
     * @brief convert a geo-coordinate string to an optional containing a GeographicalLocation or nullopt_t if the input
     * is malformated or does not contain valid coordinates
     * @param coordinates: string of the format "<latitude>, <longitude>"
     * @return std::optional containing a GeographicalLocation object or std::nullopt_t
     * if the supplied string is not a valid coordinate pair
     */
    static std::optional<GeographicalLocation> getGeoLocOptionFromString(const std::string& coordinates);


    /**
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: GeographicalLocation representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, GeographicalLocation>> getNodeIdsInRange(GeographicalLocation coord, double radius);

    /**
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, GeographicalLocation>> getNodeIdsInRange(double radius);

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

    /**
     * @brief Method to let the Coordinator know of errors and exceptions
     * @param workerId of the worker that handled the failed query
     * @param errorMsg to describe the reason of the failure
     * @return true if Notification was successful, false otherwise
     */
    bool notifyErrors(uint64_t workerId, std::string errorMsg);

    uint64_t getWorkerId();

    const Configurations::WorkerConfigurationPtr& getWorkerConfiguration() const;

    /**
      * @brief method to propagate new epoch timestamp to coordinator
      * @param timestamp: max timestamp of current epoch
      * @param queryId: identifies what query sends punctuation
      * @return bool indicating success
      */
    bool notifyEpochTermination(uint64_t timestamp, uint64_t querySubPlanId);

    /**
      * @brief method that enables the worker to send errors to the Coordinator. Calls the notifyError method
      * @param signalNumber
      * @param string with exception
      * @return bool indicating success
      */
    void onFatalError(int signalNumber, std::string string) override;

    /**
      * @brief method that enables the worker to send exceptions to the Coordinator. Calls the notifyError method
      * @param ptr exception pointer
      * @param string with exception
      * @return bool indicating success
      */
    void onFatalException(std::shared_ptr<std::exception> ptr, std::string string) override;

  private:
    /**
     * @brief method to register physical source with the coordinator
     * @param physicalSources: physical sources containing relevant information
     * @return bool indicating success
     */
    bool registerPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources);

    /**
     * @brief this method will start the GRPC Worker server which is responsible for reacting to calls
     */
    void buildAndStartGRPCServer(const std::shared_ptr<std::promise<int>>& prom);

    /**
     * @brief helper method to ensure client is connected before calling rpc functions
     * @return
     */
    bool waitForConnect() const;

    void handleRpcs(WorkerRPCServer& service);

    /**
     * @brief method to set the Nodes Location. it does not update the topology and is meant for initialization
     * @param geoLoc: The new fixed GeographicalLocation to be set
     * @return success of operation
     */
    bool setNodeLocationCoordinates(const GeographicalLocation& geoLoc);

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
    std::atomic<uint16_t> localWorkerRpcPort;
    uint16_t localWorkerZmqPort;
    uint16_t numberOfSlots;
    uint16_t numWorkerThreads;
    uint32_t numberOfBuffersInGlobalBufferManager;
    uint32_t numberOfBuffersPerWorker;
    uint32_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t bufferSizeInBytes;

    std::optional<GeographicalLocation> locationCoordinates;
    Configurations::QueryCompilerConfiguration queryCompilerConfiguration;
    bool enableNumaAwareness{false};
    bool enableMonitoring;
    bool tfInstalled{false};
    uint64_t numberOfQueues;
    uint64_t numberOfThreadsPerQueue;
    Runtime::QueryManager::QueryMangerMode queryManagerMode;
    std::atomic<bool> isRunning{false};
    TopologyNodeId topologyNodeId{INVALID_TOPOLOGY_NODE_ID};
};
using NesWorkerPtr = std::shared_ptr<NesWorker>;

}// namespace NES
#endif  // NES_INCLUDE_COMPONENTS_NESWORKER_HPP_
