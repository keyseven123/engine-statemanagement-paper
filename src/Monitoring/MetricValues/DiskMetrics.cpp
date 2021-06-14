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

#include <API/Schema.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr DiskMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64)
                           ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

void writeToBuffer(const DiskMetrics& metric, NodeEngine::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(DiskMetrics) < buf.getBufferSize(), "DiskMetrics: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metric, sizeof(DiskMetrics));
    buf.setNumberOfTuples(1);
}

DiskMetrics DiskMetrics::fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    DiskMetrics output{};
    auto i = schema->getIndex(prefix);

    if (i >= schema->getSize()) {
        NES_THROW_RUNTIME_ERROR("DiskMetrics: Prefix " + prefix + " could not be found in schema:\n" + schema->toString());
    }
    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("DiskMetrics: Tuple size should be 1, but is " + std::to_string(buf.getNumberOfTuples()));
    }
    if (!(UtilityFunctions::endsWith(schema->fields[i]->getName(), "F_BSIZE")
          && UtilityFunctions::endsWith(schema->fields[i + 4]->getName(), "F_BAVAIL"))) {
        NES_THROW_RUNTIME_ERROR("DiskMetrics: Missing fields in schema.");
    }

    auto layout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
    auto bindedRowLayout = layout->bind(buf);

    output.fBsize = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.fFrsize = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.fBlocks = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.fBfree = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.fBavail = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];

    return output;
}

web::json::value DiskMetrics::toJson() {
    web::json::value metricsJson{};
    metricsJson["F_BSIZE"] = web::json::value::number(fBsize);
    metricsJson["F_FRSIZE"] = web::json::value::number(fFrsize);
    metricsJson["F_BLOCKS"] = web::json::value::number(fBlocks);
    metricsJson["F_BFREE"] = web::json::value::number(fBfree);
    metricsJson["F_BAVAIL"] = web::json::value::number(fBavail);
    return metricsJson;
}

SchemaPtr getSchema(const DiskMetrics&, const std::string& prefix) { return DiskMetrics::getSchema(prefix); }
}// namespace NES