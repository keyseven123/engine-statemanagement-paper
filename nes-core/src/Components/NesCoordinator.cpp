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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <REST/RestServer.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryService.hpp>
#include <Services/RequestProcessorService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <grpcpp/server_builder.h>
#include <memory>
#include <thread>

//GRPC Includes
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Services/MaintenanceService.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/SourceCatalogService.hpp>

#include "health.grpc.pb.h"
#include "health.pb.h"
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/ThreadNaming.hpp>
#include <grpcpp/ext/health_check_service_server_builder_option.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

using namespace Configurations;

NesCoordinator::NesCoordinator(CoordinatorConfigurationPtr coordinatorConfig, WorkerConfigurationPtr workerConfiguration)
    : NesCoordinator(std::move(coordinatorConfig)) {
    workerConfig = std::move(workerConfiguration);
}

NesCoordinator::NesCoordinator(CoordinatorConfigurationPtr coordinatorConfiguration)
    : coordinatorConfiguration(std::move(coordinatorConfiguration)), restIp(this->coordinatorConfiguration->restIp),
      restPort(this->coordinatorConfiguration->restPort), rpcIp(this->coordinatorConfiguration->coordinatorIp),
      rpcPort(this->coordinatorConfiguration->rpcPort), numberOfSlots(this->coordinatorConfiguration->numberOfSlots),
      numberOfWorkerThreads(this->coordinatorConfiguration->numWorkerThreads),
      numberOfBuffersInGlobalBufferManager(this->coordinatorConfiguration->numberOfBuffersInGlobalBufferManager),
      numberOfBuffersPerWorker(this->coordinatorConfiguration->numberOfBuffersPerWorker),
      numberOfBuffersInSourceLocalBufferPool(this->coordinatorConfiguration->numberOfBuffersInSourceLocalBufferPool),
      bufferSizeInBytes(this->coordinatorConfiguration->bufferSizeInBytes),
      enableMonitoring(this->coordinatorConfiguration->enableMonitoring), health_check_service_disabled_(false) {
    NES_DEBUG("NesCoordinator() restIp=" << restIp << " restPort=" << restPort << " rpcIp=" << rpcIp << " rpcPort=" << rpcPort);
    log4cxx::MDC::put("threadName", "NesCoordinator");
    topology = Topology::create();
    workerRpcClient = std::make_shared<WorkerRPCClient>();
    monitoringService = std::make_shared<MonitoringService>(workerRpcClient, topology, enableMonitoring);

    // TODO make compiler backend configurable
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto queryParsingService = QueryParsingService::create(jitCompiler);

    sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<QueryCatalog>();

    sourceCatalogService = std::make_shared<SourceCatalogService>(sourceCatalog);
    topologyManagerService = std::make_shared<TopologyManagerService>(topology);
    queryRequestQueue = std::make_shared<RequestQueue>(this->coordinatorConfiguration->optimizer.queryBatchSize);
    globalQueryPlan = GlobalQueryPlan::create();

    queryRequestProcessorService =
        std::make_shared<RequestProcessorService>(globalExecutionPlan,
                                                  topology,
                                                  queryCatalog,
                                                  globalQueryPlan,
                                                  sourceCatalog,
                                                  workerRpcClient,
                                                  queryRequestQueue,
                                                  this->coordinatorConfiguration->optimizer,
                                                  this->coordinatorConfiguration->enableQueryReconfiguration);

    queryService = std::make_shared<QueryService>(queryCatalog,
                                                  queryRequestQueue,
                                                  sourceCatalog,
                                                  queryParsingService,
                                                  this->coordinatorConfiguration->optimizer);

    udfCatalog = Catalogs::UdfCatalog::create();
    maintenanceService =
        std::make_shared<NES::Experimental::MaintenanceService>(topology, queryCatalog, queryRequestQueue, globalExecutionPlan);
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    NES_DEBUG("NesCoordinator::~NesCoordinator() ptr usage=" << workerRpcClient.use_count());

    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    sourceCatalog->reset();
    queryCatalog->clearQueries();

    topology.reset();
    sourceCatalog.reset();
    globalExecutionPlan.reset();
    queryCatalog.reset();
    workerRpcClient.reset();
    queryRequestQueue.reset();
    queryRequestProcessorService.reset();
    queryService.reset();
    monitoringService.reset();
    maintenanceService.reset();
    queryRequestProcessorThread.reset();
    worker.reset();
    sourceCatalogService.reset();
    topologyManagerService.reset();
    restThread.reset();
    restServer.reset();
    rpcThread.reset();
    coordinatorConfiguration.reset();
    workerConfig.reset();

    NES_ASSERT(topology.use_count() == 0, "NesCoordinator topology leaked");
    NES_ASSERT(sourceCatalog.use_count() == 0, "NesCoordinator sourceCatalog leaked");
    NES_ASSERT(globalExecutionPlan.use_count() == 0, "NesCoordinator globalExecutionPlan leaked");
    NES_ASSERT(queryCatalog.use_count() == 0, "NesCoordinator queryCatalog leaked");
    NES_ASSERT(workerRpcClient.use_count() == 0, "NesCoordinator workerRpcClient leaked");
    NES_ASSERT(queryRequestQueue.use_count() == 0, "NesCoordinator queryRequestQueue leaked");
    NES_ASSERT(queryRequestProcessorService.use_count() == 0, "NesCoordinator queryRequestProcessorService leaked");
    NES_ASSERT(queryService.use_count() == 0, "NesCoordinator queryService leaked");

    NES_ASSERT(rpcThread.use_count() == 0, "NesCoordinator rpcThread leaked");
    NES_ASSERT(queryRequestProcessorThread.use_count() == 0, "NesCoordinator queryRequestProcessorThread leaked");
    NES_ASSERT(worker.use_count() == 0, "NesCoordinator worker leaked");
    NES_ASSERT(restServer.use_count() == 0, "NesCoordinator restServer leaked");
    NES_ASSERT(restThread.use_count() == 0, "NesCoordinator restThread leaked");
    NES_ASSERT(sourceCatalogService.use_count() == 0, "NesCoordinator sourceCatalogService leaked");
    NES_ASSERT(topologyManagerService.use_count() == 0, "NesCoordinator topologyManagerService leaked");
    NES_ASSERT(maintenanceService.use_count() == 0, "NesCoordinator maintenanceService leaked");
}

NesWorkerPtr NesCoordinator::getNesWorker() { return worker; }

Runtime::NodeEnginePtr NesCoordinator::getNodeEngine() { return worker->getNodeEngine(); }
bool NesCoordinator::isCoordinatorRunning() { return isRunning; }

uint64_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes coordinator");
    }

    queryRequestProcessorThread = std::make_shared<std::thread>(([&]() {
        setThreadName("RqstProc");

        NES_INFO("NesCoordinator: started queryRequestProcessor");
        queryRequestProcessorService->start();
        NES_WARNING("NesCoordinator: finished queryRequestProcessor");
    }));

    NES_DEBUG("NesCoordinator: startCoordinatorRPCServer: Building GRPC Server");
    std::shared_ptr<std::promise<bool>> promRPC = std::make_shared<std::promise<bool>>();

    rpcThread = std::make_shared<std::thread>(([this, promRPC]() {
        setThreadName("nesRPC");

        NES_DEBUG("NesCoordinator: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesCoordinator: buildAndStartGRPCServer: end listening");
    }));
    promRPC->get_future().get();
    NES_DEBUG("NesCoordinator:buildAndStartGRPCServer: ready");

    NES_DEBUG("NesCoordinator: Register Logical source");
    for (auto& logicalSource : coordinatorConfiguration->logicalSources) {
        sourceCatalogService->registerLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    }
    NES_DEBUG("NesCoordinator: Finished Registering Logical source");

    //start the coordinator worker that is the sink for all queries
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    if (workerConfig) {
        NES_DEBUG("Use provided external worker config");
    } else {
        NES_DEBUG("Use provided default worker config");
        workerConfig = std::make_shared<WorkerConfiguration>();
        workerConfig->coordinatorIp = rpcIp;
        workerConfig->localWorkerIp = rpcIp;
        workerConfig->coordinatorPort = rpcPort;
        workerConfig->rpcPort = 0;
        workerConfig->dataPort = 0;
        workerConfig->numberOfSlots = numberOfSlots;
        workerConfig->numWorkerThreads = numberOfWorkerThreads;
        workerConfig->bufferSizeInBytes = bufferSizeInBytes;
        workerConfig->numberOfBuffersInSourceLocalBufferPool = numberOfBuffersInSourceLocalBufferPool;
        workerConfig->numberOfBuffersPerWorker = numberOfBuffersPerWorker;
        workerConfig->numberOfBuffersInGlobalBufferManager = numberOfBuffersInGlobalBufferManager;
        workerConfig->enableMonitoring = enableMonitoring;
    }
    auto workerConfigCopy = workerConfig;
    worker = std::make_shared<NesWorker>(std::move(workerConfigCopy));
    worker->start(/**blocking*/ false, /**withConnect*/ true);

    //Start rest that accepts queries form the outsides
    NES_DEBUG("NesCoordinator starting rest server");
    restServer = std::make_shared<RestServer>(restIp,
                                              restPort,
                                              this->inherited0::weak_from_this(),
                                              queryCatalog,
                                              sourceCatalog,
                                              topology,
                                              globalExecutionPlan,
                                              queryService,
                                              monitoringService,
                                              maintenanceService,
                                              globalQueryPlan,
                                              udfCatalog,
                                              worker->getNodeEngine()->getBufferManager());
    restThread = std::make_shared<std::thread>(([&]() {
        setThreadName("nesREST");
        restServer->start();//this call is blocking
        NES_DEBUG("NesCoordinator: startRestServer thread terminates");
    }));
    NES_DEBUG("NESWorker::startCoordinatorRESTServer: ready");

    healthThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");

        while (isRunning) {
            NES_DEBUG("NesCoordinator: start health checking");
            auto root = topologyManagerService->getRootNode();
            auto topologyIterator = NES::DepthFirstNodeIterator(root).begin();
            while (topologyIterator != NES::DepthFirstNodeIterator::end()) {
                //get node
                auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();

                //get Address
                auto nodeIp = currentTopologyNode->getIpAddress();
                auto nodeGrpcPort = currentTopologyNode->getGrpcPort();
                std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

                //check health
                NES_DEBUG("NesCoordinator::healthCheck: checking node=" << destAddress);
                auto res = workerRpcClient->checkHealth(destAddress);
                if (res) {
                    NES_DEBUG("NesCoordinator::healthCheck: node=" << destAddress << " is alive");
                } else {
                    NES_ERROR("NesCoordinator::healthCheck: node=" << destAddress << " went dead so we remove it");
                    auto ret = topologyManagerService->removePhysicalNode(currentTopologyNode);
                    if(ret)
                    {
                        NES_DEBUG("NesCoordinator::healthCheck: remove node =" << destAddress << " successfully");
                    }
                    else
                    {
                        NES_THROW_RUNTIME_ERROR("Node wen offline but could not wie be removed");
                    }
                }
                ++topologyIterator;
            }

            uint64_t waitCnt = 0;
            while(waitCnt != 1 && isRunning)//change back to 60 later
            {
                NES_TRACE("NesCoordinator: waitCnt=" << waitCnt);
                std::this_thread::sleep_for(std::chrono::seconds(1));
                waitCnt++;
            }
        }

        NES_DEBUG("NesCoordinator: stop health checking");
    }));

    if (blocking) {//blocking is for the starter to wait here for user to send query
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread->join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {//non-blocking is used for tests to continue execution
        NES_DEBUG("NesCoordinator started, return without blocking on port " << rpcPort);
        return rpcPort;
    }
    return 0UL;
}

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force=" << force);
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            throw log4cxx::helpers::Exception("Error while stopping NesCoordinator");
        }
        NES_DEBUG("NesCoordinator: rest server stopped " << successStopRest);

        if (restThread->joinable()) {
            NES_DEBUG("NesCoordinator: join restThread");
            restThread->join();
        } else {
            NES_ERROR("NesCoordinator: rest thread not joinable");
            throw log4cxx::helpers::Exception("Error while stopping thread->join");
        }

        queryRequestProcessorService->shutDown();
        if (queryRequestProcessorThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            queryRequestProcessorThread->join();
        } else {
            NES_ERROR("NesCoordinator: query processor thread not joinable");
            throw log4cxx::helpers::Exception("Error while stopping thread->join");
        }

        bool successShutdownWorker = worker->stop(force);
        if (!successShutdownWorker) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            throw log4cxx::helpers::Exception("NesCoordinator::stop error while stopping node engine");
        }
        NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");

        //stop health check:
        if (healthThread->joinable()) {
            healthThread->join();
            healthThread.reset();
        }
        else
        {
            NES_ERROR("NesCoordinator: health thread not joinable");
            throw log4cxx::helpers::Exception("Error while stopping health->join");
        }

        NES_DEBUG("NesCoordinator: stopping rpc server");
        rpcServer->Shutdown();

        rpcServer->Wait();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            rpcThread->join();
            rpcThread.reset();

        } else {
            NES_ERROR("NesCoordinator: rpc thread not joinable");
            throw log4cxx::helpers::Exception("Error while stopping thread->join");
        }
        return true;
    }
    NES_DEBUG("NesCoordinator: already stopped");
    return true;
}

void NesCoordinator::buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom) {
    grpc::ServerBuilder builder;
    NES_ASSERT(sourceCatalogService, "null sourceCatalogService");
    NES_ASSERT(topologyManagerService, "null topologyManagerService");
    this->replicationService = std::make_shared<ReplicationService>(this->inherited0::shared_from_this());
    CoordinatorRPCServer service(topologyManagerService,
                                 sourceCatalogService,
                                 monitoringService->getMonitoringManager(),
                                 this->replicationService);

    std::string address = rpcIp + ":" + std::to_string(rpcPort);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::ServerBuilderOption> option(
        new grpc::HealthCheckServiceServerBuilderOption(std::move(healthCheckServiceInterface)));
    builder.SetOption(std::move(option));
    const std::string kHealthyService("healthy_service");
    healthCheckServiceImpl.SetStatus(kHealthyService, grpc::health::v1::HealthCheckResponse_ServingStatus::HealthCheckResponse_ServingStatus_SERVING);
    builder.RegisterService(&healthCheckServiceImpl);

    rpcServer = builder.BuildAndStart();
    prom->set_value(true);
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServerServer listening on address=" << address);
    rpcServer->Wait();//blocking call
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServer end listening");
}

std::vector<Runtime::QueryStatisticsPtr> NesCoordinator::getQueryStatistics(QueryId queryId) {
    NES_INFO("NesCoordinator: Get query statistics for query Id " << queryId);
    return worker->getNodeEngine()->getQueryStatistics(queryId);
}

QueryServicePtr NesCoordinator::getQueryService() { return queryService; }

QueryCatalogPtr NesCoordinator::getQueryCatalog() { return queryCatalog; }

Catalogs::UdfCatalogPtr NesCoordinator::getUdfCatalog() { return udfCatalog; }

MonitoringServicePtr NesCoordinator::getMonitoringService() { return monitoringService; }

GlobalQueryPlanPtr NesCoordinator::getGlobalQueryPlan() { return globalQueryPlan; }

NES::Experimental::MaintenanceServicePtr NesCoordinator::getMaintenanceService() { return maintenanceService; }

void NesCoordinator::onFatalError(int, std::string) {}

void NesCoordinator::onFatalException(const std::shared_ptr<std::exception>, std::string) {}
SourceCatalogServicePtr NesCoordinator::getSourceCatalogService() const { return sourceCatalogService; }
TopologyManagerServicePtr NesCoordinator::getTopologyManagerService() const { return topologyManagerService; }

}// namespace NES
