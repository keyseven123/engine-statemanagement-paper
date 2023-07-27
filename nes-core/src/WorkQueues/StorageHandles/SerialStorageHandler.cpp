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
#include <Topology/Topology.hpp>
#include <WorkQueues/StorageHandles/SerialStorageHandler.hpp>
#include <memory>
#include <utility>

namespace NES {

SerialStorageHandler::SerialStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                                           TopologyPtr topology,
                                           QueryCatalogServicePtr queryCatalogService,
                                           GlobalQueryPlanPtr globalQueryPlan,
                                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                           Catalogs::UDF::UDFCatalogPtr udfCatalog)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),
      queryCatalogService(std::move(queryCatalogService)), globalQueryPlan(std::move(globalQueryPlan)),
      sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)) {}

std::shared_ptr<SerialStorageHandler> SerialStorageHandler::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                   TopologyPtr topology,
                                                                   QueryCatalogServicePtr queryCatalogService,
                                                                   GlobalQueryPlanPtr globalQueryPlan,
                                                                   Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                                   Catalogs::UDF::UDFCatalogPtr udfCatalog) {
    return std::make_shared<SerialStorageHandler>(std::move(globalExecutionPlan),
                                                  std::move(topology),
                                                  std::move(queryCatalogService),
                                                  std::move(globalQueryPlan),
                                                  std::move(sourceCatalog),
                                                  std::move(udfCatalog));
}

GlobalExecutionPlanHandle SerialStorageHandler::getGlobalExecutionPlanHandle(const RequestId) {
    return {&*globalExecutionPlan, UnlockDeleter()};
}

TopologyHandle SerialStorageHandler::getTopologyHandle(const RequestId) { return {&*topology, UnlockDeleter()}; }

QueryCatalogServiceHandle SerialStorageHandler::getQueryCatalogServiceHandle(const RequestId) {
    return {&*queryCatalogService, UnlockDeleter()};
}

GlobalQueryPlanHandle SerialStorageHandler::getGlobalQueryPlanHandle(const RequestId) {
    return {&*globalQueryPlan, UnlockDeleter()};
}

SourceCatalogHandle SerialStorageHandler::getSourceCatalogHandle(const RequestId) { return {&*sourceCatalog, UnlockDeleter()}; }

UDFCatalogHandle SerialStorageHandler::getUDFCatalogHandle(const RequestId) { return {&*udfCatalog, UnlockDeleter()}; }
}// namespace NES
