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

#include <Monitoring/MetricValues/CpuValues.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cstring>

namespace NES {

SchemaPtr CpuValues::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
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

CpuValues CpuValues::fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    CpuValues output{};
    //get index where the schema for CpuValues is starting
    auto i = schema->getIndex(prefix + "user");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->getName(), "user")
        && UtilityFunctions::endsWith(schema->fields[i + 9]->getName(), "guestnice")) {
        NES_DEBUG("CpuValues: Index found for " + prefix + "user" + " at " + std::to_string(i));
        auto layout = NodeEngine::createRowLayout(schema);
        //set the values to the output object
        output.user = layout->getValueField<uint64_t>(0, i)->read(buf);
        output.nice = layout->getValueField<uint64_t>(0, i + 1)->read(buf);
        output.system = layout->getValueField<uint64_t>(0, i + 2)->read(buf);
        output.idle = layout->getValueField<uint64_t>(0, i + 3)->read(buf);
        output.iowait = layout->getValueField<uint64_t>(0, i + 4)->read(buf);
        output.irq = layout->getValueField<uint64_t>(0, i + 5)->read(buf);
        output.softirq = layout->getValueField<uint64_t>(0, i + 6)->read(buf);
        output.steal = layout->getValueField<uint64_t>(0, i + 7)->read(buf);
        output.guest = layout->getValueField<uint64_t>(0, i + 8)->read(buf);
        output.guestnice = layout->getValueField<uint64_t>(0, i + 9)->read(buf);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuValues: Metrics could not be parsed from schema with prefix " + prefix + ":\n"
                                + schema->toString());
    }
    return output;
}

std::ostream& operator<<(std::ostream& os, const CpuValues& values) {
    os << "user: " << values.user << " nice: " << values.nice << " system: " << values.system << " idle: " << values.idle
       << " iowait: " << values.iowait << " irq: " << values.irq << " softirq: " << values.softirq << " steal: " << values.steal
       << " guest: " << values.guest << " guestnice: " << values.guestnice;
    return os;
}

void writeToBuffer(const CpuValues& metrics, NodeEngine::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBufferAs<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(CpuValues) < buf.getBufferSize(), "CpuValues: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metrics, sizeof(CpuValues));
    buf.setNumberOfTuples(1);
}

}// namespace NES
