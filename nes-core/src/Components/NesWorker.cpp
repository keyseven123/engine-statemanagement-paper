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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/GeographicalLocation.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <CoordinatorRPCService.pb.h>
#include <GRPC/CallData.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Network/NetworkManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <csignal>
#include <future>
#include <log4cxx/helpers/exception.h>
#include <utility>

using namespace std;
volatile sig_atomic_t flag = 0;

void termFunc(int) {
    cout << "termfunc" << endl;
    flag = 1;
}

namespace NES {

NesWorker::NesWorker(Configurations::WorkerConfigurationPtr&& workerConfig)
    : workerConfig(workerConfig), coordinatorIp(workerConfig->coordinatorIp.getValue()),
      localWorkerIp(workerConfig->localWorkerIp.getValue()), workerToCoreMapping(workerConfig->workerPinList.getValue()),
      queuePinList(workerConfig->queuePinList.getValue()), coordinatorPort(workerConfig->coordinatorPort.getValue()),
      localWorkerRpcPort(workerConfig->rpcPort.getValue()), localWorkerZmqPort(workerConfig->dataPort.getValue()),
      numberOfSlots(workerConfig->numberOfSlots.getValue()), numWorkerThreads(workerConfig->numWorkerThreads.getValue()),
      numberOfBuffersInGlobalBufferManager(workerConfig->numberOfBuffersInGlobalBufferManager.getValue()),
      numberOfBuffersPerWorker(workerConfig->numberOfBuffersPerWorker.getValue()),
      numberOfBuffersInSourceLocalBufferPool(workerConfig->numberOfBuffersInSourceLocalBufferPool.getValue()),
      bufferSizeInBytes(workerConfig->bufferSizeInBytes.getValue()),
      locationCoordinates(getGeoLocOptionFromString(workerConfig->locationCoordinates.getValue())),
      queryCompilerConfiguration(workerConfig->queryCompiler), enableNumaAwareness(workerConfig->numaAwareness.getValue()),
      enableMonitoring(workerConfig->enableMonitoring.getValue()), tfInstalled(workerConfig->tfInstalled.getValue()), numberOfQueues(workerConfig->numberOfQueues.getValue()),
      numberOfThreadsPerQueue(workerConfig->numberOfThreadsPerQueue.getValue()),
      queryManagerMode(workerConfig->queryManagerMode.getValue()) {
    setThreadName("NesWorker");
    NES_DEBUG("NesWorker: constructed");
    NES_ASSERT2_FMT(coordinatorPort > 0, "Cannot use 0 as coordinator port");
}

NesWorker::~NesWorker() { stop(true); }

bool NesWorker::setWithParent(uint32_t parentId) {
    withParent = true;
    this->parentId = std::move(parentId);
    return true;
}

void NesWorker::handleRpcs(WorkerRPCServer& service) {
    //TODO: somehow we need this although it is not called at all
    // Spawn a new CallData instance to serve new clients.

    CallData call(service, completionQueue.get());
    call.proceed();
    void* tag = nullptr;// uniquely identifies a request.
    bool ok = 0;        //
    while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or completionQueue is shutting down.
        bool ret = completionQueue->Next(&tag, &ok);
        NES_DEBUG("handleRpcs got item from queue with ret=" << ret);
        if (!ret) {
            //we are going to shut down
            return;
        }
        NES_ASSERT(ok, "handleRpcs got invalid message");
        static_cast<CallData*>(tag)->proceed();
    }
}

void NesWorker::buildAndStartGRPCServer(const std::shared_ptr<std::promise<int>>& portPromise) {
    WorkerRPCServer service(nodeEngine, monitoringAgent);
    ServerBuilder builder;
    int actualRpcPort;
    builder.AddListeningPort(rpcAddress, grpc::InsecureServerCredentials(), &actualRpcPort);
    builder.RegisterService(&service);
    completionQueue = builder.AddCompletionQueue();
    rpcServer = builder.BuildAndStart();
    portPromise->set_value(actualRpcPort);
    NES_DEBUG("NesWorker: buildAndStartGRPCServer Server listening on address " << rpcAddress << ":" << actualRpcPort);
    //this call is already blocking
    handleRpcs(service);
    rpcServer->Wait();
    NES_DEBUG("NesWorker: buildAndStartGRPCServer end listening");
}

uint64_t NesWorker::getWorkerId() { return coordinatorRpcClient->getId(); }

bool NesWorker::start(bool blocking, bool withConnect) {
    NES_DEBUG("NesWorker: start with blocking "
              << blocking << " coordinatorIp=" << coordinatorIp << " coordinatorPort=" << coordinatorPort << " localWorkerIp="
              << localWorkerIp << " localWorkerRpcPort=" << localWorkerRpcPort << " localWorkerZmqPort=" << localWorkerZmqPort);
    NES_DEBUG("NesWorker::start: start Runtime");
    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes worker");
    }

    std::vector<PhysicalSourcePtr> physicalSources;
    for (auto physicalSource : workerConfig->physicalSources.getValues()) {
        physicalSources.push_back(physicalSource);
    }

    try {
        nodeEngine = Runtime::NodeEngineFactory::createNodeEngine(localWorkerIp,
                                                                  localWorkerZmqPort,
                                                                  physicalSources,
                                                                  numWorkerThreads,
                                                                  bufferSizeInBytes,
                                                                  numberOfBuffersInGlobalBufferManager,
                                                                  numberOfBuffersInSourceLocalBufferPool,
                                                                  numberOfBuffersPerWorker,
                                                                  queryCompilerConfiguration,
                                                                  this->inherited0::shared_from_this(),
                                                                  enableNumaAwareness ? Runtime::NumaAwarenessFlag::ENABLED
                                                                                      : Runtime::NumaAwarenessFlag::DISABLED,
                                                                  workerToCoreMapping,
                                                                  numberOfQueues,
                                                                  numberOfThreadsPerQueue,
                                                                  queryManagerMode);
        NES_DEBUG("NesWorker: Node engine started successfully");
        monitoringAgent = MonitoringAgent::create(enableMonitoring);
        NES_DEBUG("NesWorker: MonitoringAgent configured with monitoring=" << enableMonitoring);
    } catch (std::exception& err) {
        NES_ERROR("NesWorker: node engine could not be started");
        throw log4cxx::helpers::Exception("NesWorker error while starting node engine");
    }

    rpcAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort);
    NES_DEBUG("NesWorker: request startWorkerRPCServer for accepting messages for address=" << rpcAddress << ":"
                                                                                            << localWorkerRpcPort.load());
    std::shared_ptr<std::promise<int>> promRPC = std::make_shared<std::promise<int>>();

    rpcThread = std::make_shared<std::thread>(([this, promRPC]() {
        NES_DEBUG("NesWorker: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesWorker: buildAndStartGRPCServer: end listening");
    }));
    localWorkerRpcPort.store(promRPC->get_future().get());
    rpcAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort.load());
    NES_DEBUG("NesWorker: startWorkerRPCServer ready for accepting messages for address=" << rpcAddress << ":"
                                                                                          << localWorkerRpcPort.load());

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        bool con = connect();
        NES_DEBUG("connected= " << con);
        NES_ASSERT(con, "cannot connect");
    }
    if (!workerConfig->physicalSources.getValues().empty()) {
        NES_DEBUG("NesWorker: start with register source");
        bool success = registerPhysicalSources(physicalSources);
        NES_DEBUG("registered= " << success);
        NES_ASSERT(success, "cannot register");
    }
    if (withParent) {
        NES_DEBUG("NesWorker: add parent id=" << parentId);
        bool success = addParent(parentId);
        NES_DEBUG("parent add= " << success);
        NES_ASSERT(success, "cannot addParent");
    }

    if (blocking) {
        NES_DEBUG("NesWorker: started, join now and waiting for work");
        signal(SIGINT, termFunc);
        while (true) {
            if (flag) {
                NES_DEBUG("NesWorker: caught signal terminating worker");
                flag = 0;
                break;
            }
            cout << "NesWorker wait" << endl;
            sleep(5);
        }
        NES_DEBUG("NesWorker: joined, return");
        return true;
    }
    NES_DEBUG("NesWorker: started, return without blocking");
    return true;
}

Runtime::NodeEnginePtr NesWorker::getNodeEngine() { return nodeEngine; }

bool NesWorker::isWorkerRunning() const noexcept { return isRunning; }

bool NesWorker::stop(bool) {
    NES_DEBUG("NesWorker: stop");
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        bool successShutdownNodeEngine = nodeEngine->stop();
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            throw log4cxx::helpers::Exception("NesWorker::stop  error while stopping node engine");
        }
        NES_DEBUG("NesWorker::stop : Node engine stopped successfully");

        nodeEngine.reset();
        NES_DEBUG("NesWorker: stopping rpc server");
        rpcServer->Shutdown();
        //shut down the async queue
        completionQueue->Shutdown();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesWorker: join rpcThread");
            rpcThread->join();
        }
        rpcServer.reset();
        rpcThread.reset();

        return successShutdownNodeEngine;
    }
    NES_WARNING("NesWorker::stop: already stopped");
    return true;
}

bool NesWorker::connect() {
    std::string address = coordinatorIp + ":" + std::to_string(coordinatorPort);

    coordinatorRpcClient = std::make_shared<CoordinatorRPCClient>(address);
    std::string localAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort);
    auto registrationMetrics = monitoringAgent->getRegistrationMetrics();

    NES_DEBUG("NesWorker::connect() with server address= " << address << " localaddress=" << localAddress);

    bool successPRCRegister = coordinatorRpcClient->registerNode(localWorkerIp,
                                                                 localWorkerRpcPort.load(),
                                                                 nodeEngine->getNetworkManager()->getServerDataPort(),
                                                                 numberOfSlots,
                                                                 registrationMetrics,
                                                                 locationCoordinates,
                                                                 tfInstalled);
    NES_DEBUG("NesWorker::connect() got id=" << coordinatorRpcClient->getId());
    topologyNodeId = coordinatorRpcClient->getId();
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc register success");
        connected = true;
        return true;
    }
    NES_DEBUG("NesWorker::registerNode rpc register failed");
    connected = false;
    return false;
}

bool NesWorker::disconnect() {
    NES_DEBUG("NesWorker::disconnect()");
    bool successPRCRegister = coordinatorRpcClient->unregisterNode();
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc unregister success");
        return true;
    }
    NES_DEBUG("NesWorker::registerNode rpc unregister failed");
    return false;
}

bool NesWorker::unregisterPhysicalSource(std::string logicalName, std::string physicalName) {
    bool success = coordinatorRpcClient->unregisterPhysicalSource(std::move(logicalName), std::move(physicalName));
    NES_DEBUG("NesWorker::unregisterPhysicalSource success=" << success);
    return success;
}

const Configurations::WorkerConfigurationPtr& NesWorker::getWorkerConfiguration() const { return workerConfig; }

bool NesWorker::registerPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources) {
    NES_ASSERT(!physicalSources.empty(), "invalid physical sources");
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "cannot connect");
    bool success = coordinatorRpcClient->registerPhysicalSources(physicalSources);
    NES_ASSERT(success, "failed to register source");
    NES_DEBUG("NesWorker::registerPhysicalSources success=" << success);
    return success;
}

bool NesWorker::addParent(uint64_t parentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->addParent(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success);
    return success;
}

bool NesWorker::replaceParent(uint64_t oldParentId, uint64_t newParentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->replaceParent(oldParentId, newParentId);
    if (!success) {
        NES_WARNING("NesWorker::replaceParent() failed to replace oldParent=" << oldParentId
                                                                              << " with newParentId=" << newParentId);
    }
    NES_DEBUG("NesWorker::replaceParent() success=" << success);
    return success;
}

bool NesWorker::removeParent(uint64_t parentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->removeParent(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success);
    return success;
}

std::vector<Runtime::QueryStatisticsPtr> NesWorker::getQueryStatistics(QueryId queryId) {
    return nodeEngine->getQueryStatistics(queryId);
}

bool NesWorker::waitForConnect() const {
    NES_DEBUG("NesWorker::waitForConnect()");
    auto timeoutInSec = std::chrono::seconds(3);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("waitForConnect: check connect");
        if (!connected) {
            NES_DEBUG("waitForConnect: not connected, sleep");
            sleep(1);
        } else {
            NES_DEBUG("waitForConnect: connected");
            return true;
        }
    }
    NES_DEBUG("waitForConnect: not connected after timeout");
    return false;
}

bool NesWorker::notifyQueryFailure(uint64_t queryId,
                                   uint64_t subQueryId,
                                   uint64_t workerId,
                                   uint64_t operatorId,
                                   std::string errorMsg) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->notifyQueryFailure(queryId, subQueryId, workerId, operatorId, errorMsg);
    NES_DEBUG("NesWorker::notifyQueryFailure success=" << success);
    return success;
}

bool NesWorker::notifyEpochTermination(uint64_t timestamp, uint64_t queryId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->notifyEpochTermination(timestamp, queryId);
    NES_DEBUG("NesWorker::propagatePunctuation success=" << success);
    return success;
}

bool NesWorker::notifyErrors(uint64_t workerId, std::string errorMsg) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->sendErrors(workerId, errorMsg);
    NES_DEBUG("NesWorker::sendErrors success=" << success);
    return success;
}

void NesWorker::onFatalError(int signalNumber, std::string callstack){
    NES_ERROR("onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack);
    std::string errorMsg;
    std::cerr << "QNesWorker failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
    // save errors in errorMsg
    errorMsg = "onFatalError: signal [" + std::to_string(signalNumber) + "] error [" + strerror(errno) + "] callstack " + callstack;
    //send it to Coordinator
    this->notifyErrors(this->getWorkerId(), errorMsg);
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

void NesWorker::onFatalException(std::shared_ptr<std::exception> ptr, std::string callstack) {
    NES_ERROR("onFatalException: exception=[" << ptr->what() << "] callstack=\n" << callstack);
    std::string errorMsg;
    std::cerr << "NesWorker failed fatally" << std::endl;
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Exception: " << ptr->what() << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
    // save errors in errorMsg
    errorMsg = "onFatalException: exception=[" + std::string(ptr->what()) + "] callstack=\n" + callstack;
    //send it to Coordinator
    this->notifyErrors(this->getWorkerId(), errorMsg);
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

TopologyNodeId NesWorker::getTopologyNodeId() const { return topologyNodeId; }

bool NesWorker::hasLocation() { return locationCoordinates.has_value(); }

bool NesWorker::setNodeLocationCoordinates(const GeographicalLocation& geoLoc) {
    locationCoordinates = geoLoc;
    return true;
}

//TODO #2475 check first if the node is mobile and if it is, then return a value from the gps/csv interface once the interface is implemented
//TODO #2475 also add another function that will only return a position if the node is a mobile node
std::optional<GeographicalLocation> NesWorker::getCurrentOrPermanentGeoLoc() { return locationCoordinates; }

//std::optional<std::tuple<double, double>> NesWorker::getGeoLocOptionFromString(const std::string& coordinates) {
std::optional<GeographicalLocation> NesWorker::getGeoLocOptionFromString(const std::string& coordinates) {
    try {
        return GeographicalLocation::fromString(coordinates);
    } catch (std::exception& e) {
        NES_WARNING("could not retrieve location from string: " << e.what());
        NES_WARNING("empty optional will be returned as location");
        return {};
    }
}

std::vector<std::pair<uint64_t, GeographicalLocation>> NesWorker::getNodeIdsInRange(GeographicalLocation coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, GeographicalLocation>> NesWorker::getNodeIdsInRange(double radius) {
    auto coord = getCurrentOrPermanentGeoLoc();
    if (coord.has_value()) {
        return getNodeIdsInRange(coord.value(), radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}

}// namespace NES