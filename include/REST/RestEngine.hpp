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

#pragma once

#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include "REST/Controller/BaseController.hpp"

#include <cpprest/details/http_server.h>
#include <cpprest/http_listener.h>
#include <pplx/pplxtasks.h>
#include <string>

namespace NES {
class MonitoringService;
typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

class MaintenanceService;
typedef std::shared_ptr<MaintenanceService> MaintenanceServicePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class QueryController;
typedef std::shared_ptr<QueryController> QueryControllerPtr;

class QueryCatalogController;
typedef std::shared_ptr<QueryCatalogController> QueryCatalogControllerPtr;

class StreamCatalogController;
typedef std::shared_ptr<StreamCatalogController> StreamCatalogControllerPtr;

class ConnectivityController;
typedef std::shared_ptr<ConnectivityController> ConnectivityControllerPtr;

class MonitoringController;
typedef std::shared_ptr<MonitoringController> MonitoringControllerPtr;

class MaintenanceController;
typedef std::shared_ptr<MaintenanceController> MaintenanceControllerPtr;

class TopologyController;
typedef std::shared_ptr<TopologyController> TopologyControllerPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class NesCoordinator;
typedef std::weak_ptr<NesCoordinator> NesCoordinatorWeakPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class RestEngine : public BaseController {

  public:
    RestEngine(StreamCatalogPtr streamCatalog, NesCoordinatorWeakPtr coordinator, QueryCatalogPtr queryCatalog,
               TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryServicePtr queryService,
               MonitoringServicePtr monitoringService, MaintenanceServicePtr maintenanceService, GlobalQueryPlanPtr globalQueryPlan);

    ~RestEngine();

    void handleGet(http_request message);
    void handlePost(http_request message);
    void handleDelete(http_request message);
    void handlePut(http_request message);
    void handlePatch(http_request message);
    void handleHead(http_request message);
    void handleTrace(http_request message);
    void handleMerge(http_request message);
    void initRestOpHandlers();
    void setEndpoint(const std::string& value);
    std::string endpoint() const;
    pplx::task<void> accept();
    pplx::task<void> shutdown();
    std::vector<utility::string_t> splitPath(utility::string_t path);

  protected:
    web::http::experimental::listener::http_listener _listener;// main micro service network endpoint

  private:
    QueryControllerPtr queryController;
    QueryCatalogControllerPtr queryCatalogController;
    StreamCatalogControllerPtr streamCatalogController;
    ConnectivityControllerPtr connectivityController;
    MonitoringControllerPtr monitoringController;
    TopologyControllerPtr topologyController;
    MaintenanceControllerPtr maintenanceController;
};

typedef std::shared_ptr<RestEngine> RestEnginePtr;
}// namespace NES