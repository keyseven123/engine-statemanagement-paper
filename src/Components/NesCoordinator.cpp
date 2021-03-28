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

#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/RestServer.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <thread>

//GRPC Includes
#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Services/MonitoringService.hpp>
#include <Topology/Topology.hpp>
#include <Util/ThreadNaming.hpp>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

NesCoordinator::NesCoordinator(CoordinatorConfigPtr coordinatorConfig)
    : restIp(coordinatorConfig->getRestIp()->getValue()), restPort(coordinatorConfig->getRestPort()->getValue()),
      rpcIp(coordinatorConfig->getCoordinatorIp()->getValue()), rpcPort(coordinatorConfig->getRpcPort()->getValue()),
      numberOfSlots(coordinatorConfig->getNumberOfSlots()->getValue()),
      numberOfBuffersInGlobalBufferManager(coordinatorConfig->getNumberOfBuffersInGlobalBufferManager()->getValue()),
      numberOfBuffersPerPipeline(coordinatorConfig->getnumberOfBuffersPerPipeline()->getValue()),
      numberOfBuffersInSourceLocalBufferPool(coordinatorConfig->getNumberOfBuffersInSourceLocalBufferPool()->getValue()),
      bufferSizeInBytes(coordinatorConfig->getBufferSizeInBytes()->getValue()),
      numberOfWorkerThreads(coordinatorConfig->getNumWorkerThreads()->getValue()), inherited0(), inherited1(), isRunning(false) {
    NES_DEBUG("NesCoordinator() restIp=" << restIp << " restPort=" << restPort << " rpcIp=" << rpcIp << " rpcPort=" << rpcPort);
    MDC::put("threadName", "NesCoordinator");
    topology = Topology::create();
    streamCatalog = std::make_shared<StreamCatalog>();
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<QueryCatalog>();
    coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topology);
    workerRpcClient = std::make_shared<WorkerRPCClient>();
    queryRequestQueue = std::make_shared<QueryRequestQueue>(coordinatorConfig->getQueryBatchSize()->getValue());
    globalQueryPlan = GlobalQueryPlan::create();
    queryRequestProcessorService = std::make_shared<QueryRequestProcessorService>(
        globalExecutionPlan, topology, queryCatalog, globalQueryPlan, streamCatalog, workerRpcClient, queryRequestQueue,
        coordinatorConfig->getEnableQueryMerging()->getValue(), coordinatorConfig->getQueryMergerRule()->getValue());
    queryService = std::make_shared<QueryService>(queryCatalog, queryRequestQueue, streamCatalog,
                                                  coordinatorConfig->getEnableSemanticQueryValidation()->getValue());
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    NES_DEBUG("NesCoordinator::~NesCoordinator() ptr usage=" << workerRpcClient.use_count());

    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    streamCatalog->reset();
    queryCatalog->clearQueries();

    topology.reset();
    streamCatalog.reset();
    globalExecutionPlan.reset();
    queryCatalog.reset();
    workerRpcClient.reset();
    queryRequestQueue.reset();
    queryRequestProcessorService.reset();
    queryService.reset();
    monitoringService.reset();
    queryRequestProcessorThread.reset();
    worker.reset();
    coordinatorEngine.reset();
    restThread.reset();
    restServer.reset();
    rpcThread.reset();

    NES_ASSERT(topology.use_count() == 0, "NesCoordinator topology leaked");
    NES_ASSERT(streamCatalog.use_count() == 0, "NesCoordinator streamCatalog leaked");
    NES_ASSERT(globalExecutionPlan.use_count() == 0, "NesCoordinator globalExecutionPlan leaked");
    NES_ASSERT(queryCatalog.use_count() == 0, "NesCoordinator queryCatalog leaked");
    NES_ASSERT(workerRpcClient.use_count() == 0, "NesCoordinator workerRpcClient leaked");
    NES_ASSERT(queryRequestQueue.use_count() == 0, "NesCoordinator queryRequestQueue leaked");
    NES_ASSERT(queryRequestProcessorService.use_count() == 0, "NesCoordinator queryRequestProcessorService leaked");
    NES_ASSERT(queryService.use_count() == 0, "NesCoordinator queryService leaked");

    NES_ASSERT(rpcThread.use_count() == 0, "NesCoordinator rpcThread leaked");
    NES_ASSERT(queryRequestProcessorThread.use_count() == 0, "NesCoordinator queryRequestProcessorThread leaked");
    NES_ASSERT(worker.use_count() == 0, "NesCoordinator worker leaked");
    NES_ASSERT(coordinatorEngine.use_count() == 0, "NesCoordinator coordinatorEngine leaked");
    NES_ASSERT(restServer.use_count() == 0, "NesCoordinator restServer leaked");
    NES_ASSERT(restThread.use_count() == 0, "NesCoordinator restThread leaked");
    NES_ASSERT(coordinatorEngine.use_count() == 0, "NesCoordinator coordinatorEngine leaked");
    NES_ASSERT(coordinatorEngine.use_count() == 0, "NesCoordinator coordinatorEngine leaked");
}

NodeEngine::NodeEnginePtr NesCoordinator::getNodeEngine() { return worker->getNodeEngine(); }
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

    //start the coordinator worker that is the sink for all queries
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    workerConfig->resetWorkerOptions();
    workerConfig->setCoordinatorIp(rpcIp);
    workerConfig->setLocalWorkerIp(rpcIp);
    workerConfig->setCoordinatorPort(rpcPort);
    workerConfig->setRpcPort(rpcPort + 1);
    workerConfig->setDataPort(rpcPort + 2);
    workerConfig->setNumberOfSlots(numberOfSlots);
    workerConfig->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig->setBufferSizeInBytes(bufferSizeInBytes);
    workerConfig->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
    workerConfig->setnumberOfBuffersPerPipeline(numberOfBuffersPerPipeline);
    workerConfig->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
    worker = std::make_shared<NesWorker>(workerConfig, NodeType::Worker);
    worker->start(/**blocking*/ false, /**withConnect*/ true);

    //create the monitoring service, it can only be used if a NesWorker has started
    NES_DEBUG("NesCoordinator: Initializing monitoring service");
    monitoringService =
        std::make_shared<MonitoringService>(workerRpcClient, topology, worker->getNodeEngine()->getBufferManager());

    //Start rest that accepts queries form the outsides
    NES_DEBUG("NesCoordinator starting rest server");
    restServer = std::make_shared<RestServer>(restIp, restPort, this->inherited0::weak_from_this(), queryCatalog, streamCatalog,
                                              topology, globalExecutionPlan, queryService, monitoringService, globalQueryPlan);
    restThread = std::make_shared<std::thread>(([&]() {
        setThreadName("nesREST");
        restServer->start();//this call is blocking
        NES_DEBUG("NesCoordinator: startRestServer thread terminates");
    }));
    NES_DEBUG("NESWorker::startCoordinatorRESTServer: ready");

    if (blocking) {//blocking is for the starter to wait here for user to send query
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread->join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {//non-blocking is used for tests to continue execution
        NES_DEBUG("NesCoordinator started, return without blocking on port " << rpcPort);
        return rpcPort;
    }
    return -1;
}

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force=" << force);
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            throw Exception("Error while stopping NesCoordinator");
        }
        NES_DEBUG("NesCoordinator: rest server stopped " << successStopRest);

        if (restThread->joinable()) {
            NES_DEBUG("NesCoordinator: join restThread");
            restThread->join();
        } else {
            NES_ERROR("NesCoordinator: rest thread not joinable");
            throw Exception("Error while stopping thread->join");
        }

        queryRequestProcessorService->shutDown();
        if (queryRequestProcessorThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            queryRequestProcessorThread->join();
        } else {
            NES_ERROR("NesCoordinator: query processor thread not joinable");
            throw Exception("Error while stopping thread->join");
        }

        bool successShutdownWorker = worker->stop(force);
        if (!successShutdownWorker) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            throw Exception("NesCoordinator::stop error while stopping node engine");
        } else {
            NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");
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
            throw Exception("Error while stopping thread->join");
        }
        return true;
    } else {
        NES_DEBUG("NesCoordinator: already stopped");
        return true;
    }
}

void NesCoordinator::buildAndStartGRPCServer(std::shared_ptr<std::promise<bool>> prom) {
    grpc::ServerBuilder builder;
    NES_ASSERT(coordinatorEngine, "null coordinator engine");
    CoordinatorRPCServer service(coordinatorEngine);

    std::string address = rpcIp + ":" + std::to_string(rpcPort);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    rpcServer = std::move(builder.BuildAndStart());
    prom->set_value(true);
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServerServer listening on address=" << address);
    rpcServer->Wait();//blocking call
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServer end listening");
}

std::vector<NodeEngine::QueryStatisticsPtr> NesCoordinator::getQueryStatistics(QueryId queryId) {
    NES_INFO("NesCoordinator: Get query statistics for query Id " << queryId);
    return worker->getNodeEngine()->getQueryStatistics(queryId);
}

QueryServicePtr NesCoordinator::getQueryService() { return queryService; }

QueryCatalogPtr NesCoordinator::getQueryCatalog() { return queryCatalog; }

MonitoringServicePtr NesCoordinator::getMonitoringService() { return monitoringService; }

GlobalQueryPlanPtr NesCoordinator::getGlobalQueryPlan() { return globalQueryPlan; }

void NesCoordinator::onFatalError(int, std::string) {}

void NesCoordinator::onFatalException(const std::shared_ptr<std::exception>, std::string) {}

}// namespace NES
