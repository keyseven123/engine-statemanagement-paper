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

#ifndef NES_DYNAMICLAYOUTBUFFER_HPP
#define NES_DYNAMICLAYOUTBUFFER_HPP

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <string.h>

namespace NES::NodeEngine::DynamicMemoryLayout {

typedef uint64_t FIELD_SIZE;

/**
 * @brief This abstract class is the base class for DynamicRowLayoutBuffer and DynamicColumnLayoutBuffer.
 * As the base class, it has multiple methods or members that are useful for both derived classes.
 */
class DynamicLayoutBuffer {

  public:
    DynamicLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity)
        : tupleBuffer(tupleBuffer), capacity(capacity), numberOfRecords(0) {}
    /**
     * @brief calculates the address/offset of ithRecord and jthField
     * @param ithRecord
     * @param jthField
     * @return
     */
    virtual uint64_t calcOffset(uint64_t recordIndex, uint64_t fieldIndex, const bool boundaryChecks) = 0;

    /**
     * @brief This method returns the maximum number of records, so the capacity.
     * @return
     */
    uint64_t getCapacity() { return capacity; }

    /**
     * @brief This method returns the current number of records that are in the associated buffer
     * @return
     */
    uint64_t getNumberOfRecords() { return numberOfRecords; }

    /**
     * @brief This methods returns a reference to the associated buffer
     * @return
     */
    TupleBuffer& getTupleBuffer() { return tupleBuffer; }

  protected:
    TupleBuffer& tupleBuffer;
    uint64_t capacity;
    uint64_t numberOfRecords = 0;
};
}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICLAYOUTBUFFER_HPP
