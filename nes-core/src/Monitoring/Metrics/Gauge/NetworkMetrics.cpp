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

#include "Monitoring/Metrics/Gauge/NetworkMetrics.hpp"
#include "API/AttributeField.hpp"
#include "API/Schema.hpp"
#include "Common/DataTypes/FixedChar.hpp"
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include "Runtime/TupleBuffer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <cpprest/json.h>

namespace NES::Monitoring {

NetworkMetrics::NetworkMetrics()
    : nodeId(0), interfaceName(0), rBytes(0), rPackets(0), rErrs(0), rDrop(0), rFifo(0), rFrame(0), rCompressed(0), rMulticast(0),
      tBytes(0), tPackets(0), tErrs(0), tDrop(0), tFifo(0), tColls(0), tCarrier(0), tCompressed(0) {}

NES::SchemaPtr NetworkMetrics::getSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    NES::SchemaPtr schema = NES::Schema::create(NES::Schema::ROW_LAYOUT)
                                ->addField(prefix + "node_id", BasicType::UINT64)

                                ->addField(prefix + "name", BasicType::UINT64)
                                ->addField(prefix + "rBytes", BasicType::UINT64)
                                ->addField(prefix + "rPackets", BasicType::UINT64)
                                ->addField(prefix + "rErrs", BasicType::UINT64)
                                ->addField(prefix + "rDrop", BasicType::UINT64)
                                ->addField(prefix + "rFifo", BasicType::UINT64)
                                ->addField(prefix + "rFrame", BasicType::UINT64)
                                ->addField(prefix + "rCompressed", BasicType::UINT64)
                                ->addField(prefix + "rMulticast", BasicType::UINT64)

                                ->addField(prefix + "tBytes", BasicType::UINT64)
                                ->addField(prefix + "tPackets", BasicType::UINT64)
                                ->addField(prefix + "tErrs", BasicType::UINT64)
                                ->addField(prefix + "tDrop", BasicType::UINT64)
                                ->addField(prefix + "tFifo", BasicType::UINT64)
                                ->addField(prefix + "tColls", BasicType::UINT64)
                                ->addField(prefix + "tCarrier", BasicType::UINT64)
                                ->addField(prefix + "tCompressed", BasicType::UINT64);

    return schema;
}

void NetworkMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto totalSize = NetworkMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "NetworkMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    auto layout = Runtime::MemoryLayouts::RowLayout::create(NetworkMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(interfaceName);
    buffer[tupleIndex][cnt++].write<uint64_t>(rBytes);
    buffer[tupleIndex][cnt++].write<uint64_t>(rPackets);
    buffer[tupleIndex][cnt++].write<uint64_t>(rErrs);
    buffer[tupleIndex][cnt++].write<uint64_t>(rDrop);
    buffer[tupleIndex][cnt++].write<uint64_t>(rFifo);
    buffer[tupleIndex][cnt++].write<uint64_t>(rFrame);
    buffer[tupleIndex][cnt++].write<uint64_t>(rCompressed);
    buffer[tupleIndex][cnt++].write<uint64_t>(rMulticast);

    buffer[tupleIndex][cnt++].write<uint64_t>(tBytes);
    buffer[tupleIndex][cnt++].write<uint64_t>(tPackets);
    buffer[tupleIndex][cnt++].write<uint64_t>(tErrs);
    buffer[tupleIndex][cnt++].write<uint64_t>(tDrop);
    buffer[tupleIndex][cnt++].write<uint64_t>(tFifo);
    buffer[tupleIndex][cnt++].write<uint64_t>(tColls);
    buffer[tupleIndex][cnt++].write<uint64_t>(tCarrier);
    buffer[tupleIndex][cnt++].write<uint64_t>(tCompressed);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void NetworkMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(NetworkMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    interfaceName = buffer[tupleIndex][cnt++].read<uint64_t>();
    rBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    rPackets = buffer[tupleIndex][cnt++].read<uint64_t>();
    rErrs = buffer[tupleIndex][cnt++].read<uint64_t>();
    rDrop = buffer[tupleIndex][cnt++].read<uint64_t>();
    rFifo = buffer[tupleIndex][cnt++].read<uint64_t>();
    rFrame = buffer[tupleIndex][cnt++].read<uint64_t>();
    rCompressed = buffer[tupleIndex][cnt++].read<uint64_t>();
    rMulticast = buffer[tupleIndex][cnt++].read<uint64_t>();

    tBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    tPackets = buffer[tupleIndex][cnt++].read<uint64_t>();
    tErrs = buffer[tupleIndex][cnt++].read<uint64_t>();
    tDrop = buffer[tupleIndex][cnt++].read<uint64_t>();
    tFifo = buffer[tupleIndex][cnt++].read<uint64_t>();
    tColls = buffer[tupleIndex][cnt++].read<uint64_t>();
    tCarrier = buffer[tupleIndex][cnt++].read<uint64_t>();
    tCompressed = buffer[tupleIndex][cnt++].read<uint64_t>();
}

web::json::value NetworkMetrics::toJson() const {
    web::json::value metricsJson{};

    metricsJson["NODE_ID"] = web::json::value::number(nodeId);
    metricsJson["R_BYTES"] = web::json::value::number(rBytes);
    metricsJson["R_PACKETS"] = web::json::value::number(rPackets);
    metricsJson["R_ERRS"] = web::json::value::number(rErrs);
    metricsJson["R_DROP"] = web::json::value::number(rDrop);
    metricsJson["R_FIFO"] = web::json::value::number(rFifo);
    metricsJson["R_FRAME"] = web::json::value::number(rFrame);
    metricsJson["R_COMPRESSED"] = web::json::value::number(rCompressed);
    metricsJson["R_MULTICAST"] = web::json::value::number(rMulticast);

    metricsJson["T_BYTES"] = web::json::value::number(tBytes);
    metricsJson["T_PACKETS"] = web::json::value::number(tPackets);
    metricsJson["T_ERRS"] = web::json::value::number(tErrs);
    metricsJson["T_DROP"] = web::json::value::number(tDrop);
    metricsJson["T_FIFO"] = web::json::value::number(tFifo);
    metricsJson["T_COLLS"] = web::json::value::number(tColls);
    metricsJson["T_CARRIER"] = web::json::value::number(tCarrier);
    metricsJson["T_COMPRESSED"] = web::json::value::number(tCompressed);

    return metricsJson;
}

bool NetworkMetrics::operator==(const NetworkMetrics& rhs) const {
    return nodeId == rhs.nodeId && interfaceName == rhs.interfaceName && rBytes == rhs.rBytes && rPackets == rhs.rPackets
        && rErrs == rhs.rErrs && rDrop == rhs.rDrop && rFifo == rhs.rFifo && rFrame == rhs.rFrame
        && rCompressed == rhs.rCompressed && rMulticast == rhs.rMulticast && tBytes == rhs.tBytes && tPackets == rhs.tPackets
        && tErrs == rhs.tErrs && tDrop == rhs.tDrop && tFifo == rhs.tFifo && tColls == rhs.tColls && tCarrier == rhs.tCarrier
        && tCompressed == rhs.tCompressed;
}
bool NetworkMetrics::operator!=(const NetworkMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

web::json::value asJson(const NetworkMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring
