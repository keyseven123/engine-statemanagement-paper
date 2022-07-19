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
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

CpuMetrics::CpuMetrics()
    : nodeId(0), coreNum(0), user(0), nice(0), system(0), idle(0), iowait(0), irq(0), softirq(0), steal(0), guest(0),
      guestnice(0) {}

SchemaPtr CpuMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "coreNum", BasicType::UINT64)
                           ->addField(prefix + "user", BasicType::UINT64)
                           ->addField(prefix + "nice", BasicType::UINT64)
                           ->addField(prefix + "system", BasicType::UINT64)
                           ->addField(prefix + "idle", BasicType::UINT64)
                           ->addField(prefix + "iowait", BasicType::UINT64)
                           ->addField(prefix + "irq", BasicType::UINT64)
                           ->addField(prefix + "softirq", BasicType::UINT64)
                           ->addField(prefix + "steal", BasicType::UINT64)
                           ->addField(prefix + "guest", BasicType::UINT64)
                           ->addField(prefix + "guestnice", BasicType::UINT64);
    return schema;
}

SchemaPtr CpuMetrics::createSchema(const std::string& prefix, std::list<std::string> configuredMetrics) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64);

    for (const auto& metric : configuredMetrics) {
        if (metric == "coreNum") {
            schema->addField(prefix + "coreNum", BasicType::UINT64);
        } else if (metric == "user") {
            schema->addField(prefix + "user", BasicType::UINT64);
        } else if (metric == "nice") {
            schema->addField(prefix + "nice", BasicType::UINT64);
        } else if (metric == "system") {
            schema->addField(prefix + "system", BasicType::UINT64);
        } else if (metric == "idle") {
            schema->addField(prefix + "idle", BasicType::UINT64);
        } else if (metric == "iowait") {
            schema->addField(prefix + "iowait", BasicType::UINT64);
        } else if (metric == "irq") {
            schema->addField(prefix + "irq", BasicType::UINT64);
        } else if (metric == "softirq") {
            schema->addField(prefix + "softirq", BasicType::UINT64);
        } else if (metric == "steal") {
            schema->addField(prefix + "steal", BasicType::UINT64);
        } else if (metric == "guest") {
            schema->addField(prefix + "guest", BasicType::UINT64);
        } else if (metric == "guestnice") {
            schema->addField(prefix + "guestnice", BasicType::UINT64);
        } else {
            NES_INFO("DiskMetrics: Metric unknown: " << metric);
        }
    }
    return schema;
}

void CpuMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(CpuMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = CpuMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "CpuMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(coreNum);
    buffer[tupleIndex][cnt++].write<uint64_t>(user);
    buffer[tupleIndex][cnt++].write<uint64_t>(nice);
    buffer[tupleIndex][cnt++].write<uint64_t>(system);
    buffer[tupleIndex][cnt++].write<uint64_t>(idle);
    buffer[tupleIndex][cnt++].write<uint64_t>(iowait);
    buffer[tupleIndex][cnt++].write<uint64_t>(irq);
    buffer[tupleIndex][cnt++].write<uint64_t>(softirq);
    buffer[tupleIndex][cnt++].write<uint64_t>(steal);
    buffer[tupleIndex][cnt++].write<uint64_t>(guest);
    buffer[tupleIndex][cnt++].write<uint64_t>(guestnice);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void CpuMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(CpuMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    int cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    coreNum = buffer[tupleIndex][cnt++].read<uint64_t>();
    user = buffer[tupleIndex][cnt++].read<uint64_t>();
    nice = buffer[tupleIndex][cnt++].read<uint64_t>();
    system = buffer[tupleIndex][cnt++].read<uint64_t>();
    idle = buffer[tupleIndex][cnt++].read<uint64_t>();
    iowait = buffer[tupleIndex][cnt++].read<uint64_t>();
    irq = buffer[tupleIndex][cnt++].read<uint64_t>();
    softirq = buffer[tupleIndex][cnt++].read<uint64_t>();
    steal = buffer[tupleIndex][cnt++].read<uint64_t>();
    guest = buffer[tupleIndex][cnt++].read<uint64_t>();
    guestnice = buffer[tupleIndex][cnt++].read<uint64_t>();
}

std::ostream& operator<<(std::ostream& os, const CpuMetrics& values) {
    os << "nodeId: " << values.nodeId << "coreNum: " << values.coreNum << "user: " << values.user << " nice: " << values.nice
       << " system: " << values.system << " idle: " << values.idle << " iowait: " << values.iowait << " irq: " << values.irq
       << " softirq: " << values.softirq << " steal: " << values.steal << " guest: " << values.guest
       << " guestnice: " << values.guestnice;
    return os;
}

web::json::value CpuMetrics::toJson() const {
    web::json::value metricsJson{};
    metricsJson["NODE_ID"] = web::json::value::number(nodeId);
    metricsJson["CORE_NUM"] = web::json::value::number(coreNum);
    metricsJson["USER"] = web::json::value::number(user);
    metricsJson["NICE"] = web::json::value::number(nice);
    metricsJson["SYSTEM"] = web::json::value::number(system);
    metricsJson["IDLE"] = web::json::value::number(idle);
    metricsJson["IOWAIT"] = web::json::value::number(iowait);
    metricsJson["IRQ"] = web::json::value::number(irq);
    metricsJson["SOFTIRQ"] = web::json::value::number(softirq);
    metricsJson["STEAL"] = web::json::value::number(steal);
    metricsJson["GUEST"] = web::json::value::number(guest);
    metricsJson["GUESTNICE"] = web::json::value::number(guestnice);

    return metricsJson;
}

bool CpuMetrics::operator==(const CpuMetrics& rhs) const {
    return nodeId == rhs.nodeId && coreNum == rhs.coreNum && user == rhs.user && nice == rhs.nice && system == rhs.system
        && idle == rhs.idle && iowait == rhs.iowait && irq == rhs.irq && softirq == rhs.softirq && steal == rhs.steal
        && guest == rhs.guest && guestnice == rhs.guestnice;
}

bool CpuMetrics::operator!=(const CpuMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

web::json::value asJson(const CpuMetrics& metrics) { return metrics.toJson(); }

}// namespace NES