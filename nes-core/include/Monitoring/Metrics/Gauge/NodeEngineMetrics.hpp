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

#ifndef NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_NODEENGINEMETRICS_HPP_
#define NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_NODEENGINEMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Monitoring {

/**
 * @brief NodeEngineMetrics class, that is responsible for collecting and managing the statistics metrics.
 */
class NodeEngineMetrics {
  public:
    NodeEngineMetrics();

    /**
    * @brief Returns the schema of the class with a given prefix.
    * @param prefix
    * @return the schema
    */
    static SchemaPtr getSchema(const std::string& prefix = "");

    /**
    * @brief Writes a metrics objects to the given TupleBuffer and index.
    * @param buf the tuple buffer
    * @param tupleIndex the index indication its location in the buffer
    */
    void writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const;

    /**
    * @brief Parses a metrics objects from a TupleBuffer..
    * @param buf the tuple buffer
    * @param the tuple index indicating the location of the tuple
    */
    void readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex);

    /**
    * @brief Returns the metrics as json
    * @return Json containing the metrics
    */
    [[nodiscard]] nlohmann::json toJson() const;

    //equality operators
    bool operator==(const NodeEngineMetrics& rhs) const;
    bool operator!=(const NodeEngineMetrics& rhs) const;

    uint64_t nodeId;
    uint64_t processedTasks;
    uint64_t processedTuple;
    uint64_t processedBuffers;
    uint64_t processedWatermarks;
    uint64_t latencySum;
    uint64_t queueSizeSum;
    uint64_t availableGlobalBufferSum;
    uint64_t availableFixedBufferSum;
    uint64_t timestampQueryStart;
    uint64_t timestampFirstProcessedTask;
    uint64_t timestampLastProcessedTask;
    uint64_t queryId;
    uint64_t subQueryId;
    std::map<uint64_t, std::vector<uint64_t>> tsToLatencyMap;
} __attribute__((packed));
using NodeEngineMetricsPtr = std::shared_ptr<NodeEngineMetrics>;

/**
 * @brief Writes metrics objects to a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
 */
void writeToBuffer(const NodeEngineMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses metrics objects from a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
 */
void readFromBuffer(NodeEngineMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const NodeEngineMetrics& metrics);

}// namespace NES::Monitoring

#endif// NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_NODEENGINEMETRICS_HPP_
