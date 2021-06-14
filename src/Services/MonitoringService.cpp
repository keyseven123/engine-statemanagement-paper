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

#include <Services/MonitoringService.hpp>

#include <API/Schema.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MetricValues/GroupedValues.hpp>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_client.h>

namespace NES {

MonitoringService::MonitoringService(TopologyPtr topology,
                                     NodeEngine::BufferManagerPtr bufferManager,
                                     MonitoringManagerPtr monitoringManager)
    : topology(topology), bufferManager(bufferManager), monitoringManager(monitoringManager) {
    NES_DEBUG("MonitoringService: Initializing");
}

MonitoringService::~MonitoringService() {
    NES_DEBUG("MonitoringService: Shutting down");
    topology.reset();
}

web::json::value MonitoringService::registerMonitoringPlanToAllNodes(MonitoringPlanPtr monitoringPlan) {
    web::json::value metricsJson{};
    auto root = topology->getRoot();

    std::vector<uint64_t> nodeIds;
    auto nodes = root->getAndFlattenAllChildren(false);
    for (const auto& node : root->getAndFlattenAllChildren(false)) {
        std::shared_ptr<TopologyNode> tNode = node->as<TopologyNode>();
        nodeIds.emplace_back(tNode->getId());
    }
    auto success = monitoringManager->registerRemoteMonitoringPlans(nodeIds, monitoringPlan);
    metricsJson["success"] = success;
    return metricsJson;
}

web::json::value MonitoringService::requestMonitoringDataAsJson(uint64_t nodeId) {
    NES_DEBUG("MonitoringService: Requesting monitoring data from worker id=" + std::to_string(nodeId));
    TopologyNodePtr node = topology->findNodeWithId(nodeId);

    if (node) {
        auto tupleBuffer = bufferManager->getBufferBlocking();
        auto success = monitoringManager->requestMonitoringData(nodeId, tupleBuffer);

        if (success) {
            NES_DEBUG("MonitoringService: Successfully received monitoring data from GRPC.");
            auto plan = monitoringManager->getMonitoringPlan(nodeId);
            auto schema = plan->createSchema();
            GroupedValues parsedValues = plan->fromBuffer(schema, tupleBuffer);
            auto jsonValues = parsedValues.asJson();
            tupleBuffer.release();
            return jsonValues;
        } else {
            std::string errorMsg = "MonitoringService: Error retrieving monitoring data from node " + nodeId;
            NES_ERROR(errorMsg);
            tupleBuffer.release();
            throw std::runtime_error(errorMsg);
        }
    } else {
        throw std::runtime_error("MonitoringService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
}

web::json::value MonitoringService::requestMonitoringDataFromAllNodesAsJson() {
    web::json::value metricsJson{};
    auto root = topology->getRoot();
    NES_INFO("MonitoringService: Requesting metrics for node " + std::to_string(root->getId()));
    metricsJson[std::to_string(root->getId())] = requestMonitoringDataAsJson(root->getId());

    NES_INFO("MonitoringService: Metrics from coordinator received \n" + metricsJson.serialize());

    for (const auto& node : root->getAndFlattenAllChildren(false)) {
        std::shared_ptr<TopologyNode> tNode = node->as<TopologyNode>();
        NES_INFO("MonitoringService: Requesting metrics for node " + std::to_string(tNode->getId()));
        metricsJson[std::to_string(tNode->getId())] = requestMonitoringDataAsJson(tNode->getId());
    }
    NES_INFO("MonitoringService: Metrics from coordinator received \n" + metricsJson.serialize());

    return metricsJson;
}

utf8string MonitoringService::requestMonitoringDataViaPrometheusAsString(int64_t nodeId, int16_t port) {
    NES_DEBUG("MonitoringService: Requesting monitoring data from worker id= " + std::to_string(nodeId));
    TopologyNodePtr node = topology->findNodeWithId(nodeId);

    if (node) {
        auto nodeIp = node->getIpAddress();
        auto address = "http://" + nodeIp + ":" + std::to_string(port) + "/metrics";

        utf8string metricsReturn;
        web::http::client::http_client clientQ1(address);
        NES_INFO("MonitoringService: Executing metric request to prometheus node exporter on " + address);
        clientQ1.request(web::http::methods::GET)
            .then([](const web::http::http_response& response) {
                NES_INFO("MonitoringService: Extract metrics from prometheus node exporter response.");
                return response.extract_utf8string();
            })
            .then([&metricsReturn](const pplx::task<utf8string>& task) {
                NES_INFO("MonitoringService: Set metrics return from node exporter responses.");
                metricsReturn = task.get();
            })
            .wait();
        return metricsReturn;
    } else {
        throw std::runtime_error("MonitoringService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
}

web::json::value MonitoringService::requestMonitoringDataFromAllNodesViaPrometheusAsJson() {
    web::json::value metricsJson{};
    auto root = topology->getRoot();
    metricsJson[std::to_string(root->getId())] =
        web::json::value::string(requestMonitoringDataViaPrometheusAsString(root->getId(), 9100));

    for (const auto& node : root->getAndFlattenAllChildren(false)) {
        std::shared_ptr<TopologyNode> tNode = node->as<TopologyNode>();
        metricsJson[std::to_string(tNode->getId())] =
            web::json::value::string(requestMonitoringDataViaPrometheusAsString(root->getId(), 9100));
    }
    return metricsJson;
}

}// namespace NES