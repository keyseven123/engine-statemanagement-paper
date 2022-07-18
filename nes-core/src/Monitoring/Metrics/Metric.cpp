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

#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

void writeToBuffer(const uint64_t& metrics, Runtime::TupleBuffer&, uint64_t) {
    NES_THROW_RUNTIME_ERROR("Metric: Serialization for uint64_t not possible for metric " << metrics);
}

void writeToBuffer(const std::string& metrics, Runtime::TupleBuffer&, uint64_t) {
    NES_THROW_RUNTIME_ERROR("Metric: Serialization for std::string not possible for metric " << metrics);
}

void writeToBuffer(const std::shared_ptr<Metric> metric, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    writeToBuffer(*metric, buf, tupleIndex);
}

void readFromBuffer(uint64_t&, Runtime::TupleBuffer&, uint64_t) {
    NES_THROW_RUNTIME_ERROR("Metric: Deserialization for uint64_t not possible");
}

//void readFromBufferNEW(uint64_t&, Runtime::TupleBuffer&, uint64_t) {
//    NES_THROW_RUNTIME_ERROR("Metric: Deserialization for uint64_t not possible");
//}

void readFromBuffer(std::string&, Runtime::TupleBuffer&, uint64_t) {
    NES_THROW_RUNTIME_ERROR("Metric: Deserialization for uint64_t not possible");
}

//void readFromBufferNEW(std::string&, Runtime::TupleBuffer&, uint64_t) {
//    NES_THROW_RUNTIME_ERROR("Metric: Deserialization for uint64_t not possible");
//}

void readFromBuffer(std::shared_ptr<Metric> metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    readFromBuffer(*metrics, buf, tupleIndex);
}       //nummer 1

//void readFromBufferNEW(std::shared_ptr<Metric> metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex, SchemaPtr schema) {
//    readFromBufferNEW(*metrics, buf, tupleIndex, schema);
//}

web::json::value asJson(uint64_t intMetric) {
    web::json::value metricsJson{};
    metricsJson["intMetric"] = intMetric;
    return metricsJson;
}

web::json::value asJson(std::string stringMetric) {
    web::json::value metricsJson{};
    metricsJson["stringMetric"] = web::json::value::string(stringMetric);
    return metricsJson;
}

web::json::value asJson(std::shared_ptr<Metric> ptrMetric) { return asJson(*ptrMetric); }

}// namespace NES