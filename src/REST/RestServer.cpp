#include <REST/RestServer.hpp>

#include <Catalogs/StreamCatalog.hpp>
#include <REST/RestEngine.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Util/Logger.hpp>
#include <iostream>

#include <Components/NesCoordinator.hpp>

namespace NES {

RestServer::RestServer(std::string host, u_int16_t port, NesCoordinatorWeakPtr coordinator, QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
                       TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryServicePtr queryService, MonitoringServicePtr monitoringService, GlobalQueryPlanPtr globalQueryPlan)
    : host(host), port(port), restEngine(std::make_shared<RestEngine>(streamCatalog, coordinator, queryCatalog, topology, globalExecutionPlan, queryService, monitoringService, globalQueryPlan)) {
    NES_DEBUG("RestServer: Initializing");
}

RestServer::~RestServer() {
    NES_DEBUG("~RestServer()");
}

bool RestServer::start() {

    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));
    InterruptHandler::hookUserInterruptHandler();
    restEngine->setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");
    try {
        // wait for server initialization...
        restEngine->accept().wait();
        NES_DEBUG("RestServer: Server started");
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << restEngine->endpoint());
        InterruptHandler::waitForUserInterrupt();
        restEngine->shutdown().wait();
        NES_DEBUG("RestServer: after waitForUserInterrupt");
    } catch (const std::exception& e) {
        NES_ERROR("RestServer: Unable to start REST server << " << e.what());
        return false;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        return false;
    }
    return true;
}

bool RestServer::stop() {
    NES_DEBUG("RestServer::stop");
    InterruptHandler::handleUserInterrupt(SIGTERM);
    return true;
}

}// namespace NES
