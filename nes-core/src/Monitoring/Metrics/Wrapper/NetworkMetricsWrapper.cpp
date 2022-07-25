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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cpprest/json.h>
#include <cstring>

namespace NES {
NetworkMetricsWrapper::NetworkMetricsWrapper() : schema(NetworkMetrics::getDefaultSchema("")) {}

NetworkMetricsWrapper::NetworkMetricsWrapper(uint64_t nodeId) : nodeId(nodeId), schema(NetworkMetrics::getDefaultSchema("")) {}

NetworkMetricsWrapper::NetworkMetricsWrapper(uint64_t nodeId, SchemaPtr schema) : nodeId(nodeId), schema(schema) {}

NetworkMetricsWrapper::NetworkMetricsWrapper(std::vector<NetworkMetrics>&& arr) {
    if (!arr.empty()) {
        networkMetrics = std::move(arr);
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetricsWrapper: Object cannot be allocated with less than 0 cores.");
    }
    NES_TRACE("NetworkMetricsWrapper: Allocating memory for " + std::to_string(arr.size()) + " metrics.");
}

NetworkMetricsWrapper::NetworkMetricsWrapper(std::vector<NetworkMetrics>&& arr, SchemaPtr schemaNew) {
    if (!arr.empty()) {
        networkMetrics = std::move(arr);
        schema = std::move(schemaNew);
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetricsWrapper: Object cannot be allocated with less than 0 cores.");
    }
    NES_TRACE("NetworkMetricsWrapper: Allocating memory for " + std::to_string(arr.size()) + " metrics.");
}

NetworkMetricsWrapper::NetworkMetricsWrapper(SchemaPtr schema) : schema(std::move(schema)) {}

void NetworkMetricsWrapper::setSchema(SchemaPtr newSchema) { this->schema = std::move(newSchema); }
SchemaPtr NetworkMetricsWrapper::getSchema() const { return this->schema; }

void NetworkMetricsWrapper::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto schema = this->schema;
    auto totalSize = schema->getSchemaSizeInBytes() * size();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "NetworkMetricsWrapper: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    for (unsigned int i = 0; i < size(); i++) {
        NetworkMetrics metrics = getNetworkValue(i);
        metrics.nodeId = nodeId;
        metrics.setSchema(schema);
        metrics.writeToBuffer(buf, tupleIndex + i);
    }
}

void NetworkMetricsWrapper::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto schema = this->schema;
    auto interfaceList = std::vector<NetworkMetrics>();
    NES_TRACE("NetworkMetricsWrapper: Parsing buffer with number of tuples " << buf.getNumberOfTuples());

    for (unsigned int n = 0; n < buf.getNumberOfTuples(); n++) {
        //for each core parse the according CpuMetrics
        NetworkMetrics metrics{};
        metrics.setSchema(schema);
        NES::readFromBuffer(metrics, buf, tupleIndex + n);
        interfaceList.emplace_back(metrics);
    }
    networkMetrics = std::move(interfaceList);
    nodeId = networkMetrics[0].nodeId;
}

NetworkMetrics NetworkMetricsWrapper::getNetworkValue(uint64_t interfaceNo) const {
    if (interfaceNo >= size()) {
        NES_THROW_RUNTIME_ERROR("NetworkMetricsWrapper: ArrayType index out of bound " + std::to_string(interfaceNo)
                                + ">=" + std::to_string(size()));
    }
    NetworkMetrics networkMetric = networkMetrics.at(interfaceNo);
    networkMetric.setSchema(this->schema);
    return networkMetric;
}

void NetworkMetricsWrapper::addNetworkMetrics(NetworkMetrics&& nwValue) {
    nwValue.nodeId = this->nodeId;
    networkMetrics.emplace_back(nwValue);
}

uint64_t NetworkMetricsWrapper::size() const { return networkMetrics.size(); }

std::vector<std::string> NetworkMetricsWrapper::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkMetrics.size());

    for (const auto& netVal : networkMetrics) {
        keys.push_back(std::to_string(netVal.interfaceName));
    }

    return keys;
}

web::json::value NetworkMetricsWrapper::toJson() const {
    web::json::value metricsJsonWrapper{};
    metricsJsonWrapper["NODE_ID"] = web::json::value::number(nodeId);

    web::json::value metricsJson{};
    for (auto networkVal : networkMetrics) {
        metricsJson[networkVal.interfaceName] = networkVal.toJson();
    }
    metricsJsonWrapper["values"] = metricsJson;
    return metricsJsonWrapper;
}

bool NetworkMetricsWrapper::operator==(const NetworkMetricsWrapper& rhs) const {
    if (networkMetrics.size() != rhs.networkMetrics.size()) {
        return false;
    }

    for (auto i = static_cast<decltype(networkMetrics)::size_type>(0); i < networkMetrics.size(); ++i) {
        if (networkMetrics[i] != rhs.networkMetrics[i]) {
            return false;
        }
    }

    if (nodeId != rhs.nodeId) {
        return false;
    }

    return true;
}

bool NetworkMetricsWrapper::operator!=(const NetworkMetricsWrapper& rhs) const { return !(rhs == *this); }

uint64_t NetworkMetricsWrapper::getNodeId() const { return nodeId; }

void NetworkMetricsWrapper::setNodeId(uint64_t nodeId) {
    this->nodeId = nodeId;
    if (!networkMetrics.empty()) {
        for (auto& nMetric : networkMetrics) {
            nMetric.nodeId = this->nodeId;
        }
    }
}

void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

web::json::value asJson(const NetworkMetricsWrapper& metrics) { return metrics.toJson(); }

}// namespace NES