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

#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {
uint64_t DynamicColumnLayoutBuffer::calcOffset(uint64_t recordIndex, uint64_t fieldIndex, const bool boundaryChecks) {
    auto fieldSizes = dynamicColLayout->getFieldSizes();

    if (boundaryChecks && fieldIndex >= fieldSizes.size()) {
        NES_THROW_RUNTIME_ERROR("jthField" << fieldIndex << " is larger than fieldSize.size() " << fieldSizes.size());
    }
    if (boundaryChecks && fieldIndex >= columnOffsets.size()) {
        NES_THROW_RUNTIME_ERROR("columnOffsets" << fieldIndex << " is larger than columnOffsets.size() " << columnOffsets.size());
    }

    auto offSet = (recordIndex * fieldSizes[fieldIndex]) + columnOffsets[fieldIndex];
    NES_DEBUG("DynamicColumnLayoutBuffer.calcOffset: offSet = " << offSet);
    return offSet;
}
DynamicColumnLayoutBuffer::DynamicColumnLayoutBuffer(TupleBuffer tupleBuffer,
                                                     uint64_t capacity,
                                                     std::shared_ptr<DynamicColumnLayout> dynamicColLayout,
                                                     std::vector<COL_OFFSET_SIZE> columnOffsets)
    : DynamicLayoutBuffer(tupleBuffer, capacity), columnOffsets(std::move(columnOffsets)), dynamicColLayout(dynamicColLayout) {
    this->basePointer = tupleBuffer.getBuffer<uint8_t>();
}

}// namespace NES::NodeEngine::DynamicMemoryLayout