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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <utility>
namespace NES {

DefaultSource::DefaultSource(SchemaPtr schema,
                             NodeEngine::BufferManagerPtr bufferManager,
                             NodeEngine::QueryManagerPtr queryManager,
                             const uint64_t numbersOfBufferToProduce,
                             uint64_t frequency,
                             OperatorId operatorId,
                             OperatorId logicalSourceOperatorId,
                             size_t numSourceLocalBuffers,
                             std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numbersOfBufferToProduce,
                      operatorId,
                      logicalSourceOperatorId,
                      numSourceLocalBuffers,
                      DataSource::GatheringMode::FREQUENCY_MODE,
                      successors) {
    NES_DEBUG("DefaultSource:" << this << " creating");
    this->gatheringInterval = std::chrono::milliseconds(frequency);
}

std::optional<NodeEngine::TupleBuffer> DefaultSource::receiveData() {
    // 10 tuples of size one
    NES_DEBUG("Source:" << this << " requesting buffer");

    auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("Source:" << this << " got buffer");
    uint64_t tupleCnt = 10;

    auto layout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(std::make_shared<Schema>(schema), true);
    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutBufferPtr bindedRowLayout = layout->bind(buf);

    auto value = 1;
    auto fields = schema->fields;
    for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
        for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            auto dataType = fields[fieldIndex]->getDataType();
            auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                if (basicPhysicalType->nativeType == BasicPhysicalType::CHAR) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<char, true>::create(fieldIndex,
                                                                                               bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_8) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint8_t, true>::create(fieldIndex,
                                                                                                  bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_16) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint16_t, true>::create(fieldIndex,
                                                                                                   bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_32) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint32_t, true>::create(fieldIndex,
                                                                                                   bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_64) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(fieldIndex,
                                                                                                   bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_8) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int8_t, true>::create(fieldIndex,
                                                                                                 bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_16) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int16_t, true>::create(fieldIndex,
                                                                                                  bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_32) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int32_t, true>::create(fieldIndex,
                                                                                                  bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_64) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(fieldIndex,
                                                                                                  bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::FLOAT) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<float, true>::create(fieldIndex,
                                                                                                bindedRowLayout)[recordIndex] =
                        value;
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::DOUBLE) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<double, true>::create(fieldIndex,
                                                                                                 bindedRowLayout)[recordIndex] =
                        value;
                } else {
                    NES_DEBUG("This data source only generates data for numeric fields");
                }
            } else {
                NES_DEBUG("This data source only generates data for numeric fields");
            }
        }
    }
    buf.setNumberOfTuples(tupleCnt);
    // TODO move this to trace
    NES_DEBUG("Source: id=" << operatorId << " Generated buffer with " << buf.getNumberOfTuples() << "/"
                            << schema->getSchemaSizeInBytes() << "\n"
                            << UtilityFunctions::prettyPrintTupleBuffer(buf, schema));
    return buf;
}

SourceType DefaultSource::getType() const { return DEFAULT_SOURCE; }

}// namespace NES
