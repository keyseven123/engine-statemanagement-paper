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

#ifndef NES_DYNAMICROWLAYOUTBUFFER_HPP
#define NES_DYNAMICROWLAYOUTBUFFER_HPP

#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <stdint.h>

namespace NES::NodeEngine::DynamicMemoryLayout {

/**
 * @brief This class is dervied from DynamicLayoutBuffer. As such, it implements the abstract methods and also implements pushRecord() and readRecord() as templated methods.
 * This class is non-thread safe
 */
class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    /**
     * @return retrieves the record size
     */
    FIELD_SIZE getRecordSize() { return dynamicRowLayout->getRecordSize(); }

    /**
     * @return retrieves the field offsets of the column layout
     */
    const std::vector<FIELD_SIZE>& getFieldOffSets() { return dynamicRowLayout->getFieldOffSets(); }

    /**
     * @param fieldName
     * @return field index from the fieldName
     */
    std::optional<uint64_t> getFieldIndexFromName(std::string fieldName) const {
        return dynamicRowLayout->getFieldIndexFromName(fieldName);
    };
    /**
     * @brief This function calculates the offset in the associated buffer for ithRecord and jthField in bytes
     * @param ithRecord
     * @param jthField
     * @param boundaryChecks
     * @return
     */
    uint64_t calcOffset(uint64_t recordIndex, uint64_t fieldIndex, const bool boundaryChecks) override;

    /**
     * Calling this function will result in reading record at recordIndex in the tupleBuffer associated with this layoutBuffer.
     * @tparam Types
     * @param record
     */
    template<bool boundaryChecks, typename... Types>
    std::tuple<Types...> readRecord(uint64_t recordIndex);

    /**
     * Calling this function will result in adding record in the tupleBuffer associated with this layoutBuffer
     * @tparam Types
     * @param record
     */
    template<bool boundaryChecks, typename... Types>
    bool pushRecord(std::tuple<Types...> record);

    /**
     * @brief Constructor for DynamicRowLayoutBuffer
     * @param tupleBuffer
     * @param capacity
     * @param dynamicRowLayout
     */
    DynamicRowLayoutBuffer(TupleBuffer tupleBuffer, uint64_t capacity, std::shared_ptr<DynamicRowLayout> dynamicRowLayout);

  private:
    /**
     * Copies fields of tuple sequentially to address
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<I == sizeof...(Ts), void>::type copyTupleFieldsToBuffer(std::tuple<Ts...> tup, uint8_t* address);
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type copyTupleFieldsToBuffer(std::tuple<Ts...> tup, uint8_t* address);

    /**
     * Copies fields of tuple sequentially from address
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<I == sizeof...(Ts), void>::type copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint8_t* address);
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint8_t* address);

    const DynamicRowLayoutPtr dynamicRowLayout;
    const uint8_t* basePointer;
};

template<size_t I, typename... Ts>
typename std::enable_if<I == sizeof...(Ts), void>::type DynamicRowLayoutBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                                                                        uint8_t* address) {
    // Iterated through tuple, so simply return
    ((void) address);
    ((void) tup);
    return;
}

template<size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type DynamicRowLayoutBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                                                                         uint8_t* address) {
    // Get current type of tuple and cast address to this type pointer
    *((typename std::tuple_element<I, std::tuple<Ts...>>::type*) (address)) = std::get<I>(tup);

    // Go to the next field of tuple
    copyTupleFieldsToBuffer<I + 1>(tup, address + sizeof(typename std::tuple_element<I, std::tuple<Ts...>>::type));
}

template<size_t I, typename... Ts>
typename std::enable_if<I == sizeof...(Ts), void>::type DynamicRowLayoutBuffer::copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup,
                                                                                                          uint8_t* address) {
    // Iterated through tuple, so simply return
    ((void) address);
    ((void) tup);
    return;
}

template<size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type DynamicRowLayoutBuffer::copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup,
                                                                                                           uint8_t* address) {
    // Get current type of tuple and cast address to this type pointer
    std::get<I>(tup) = *((typename std::tuple_element<I, std::tuple<Ts...>>::type*) (address));

    // Go to the next field of tuple
    copyTupleFieldsFromBuffer<I + 1>(tup, address + sizeof(typename std::tuple_element<I, std::tuple<Ts...>>::type));
}

template<bool boundaryChecks, typename... Types>
bool DynamicRowLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    if (boundaryChecks && numberOfRecords >= capacity) {
        NES_WARNING("TupleBuffer is full and thus no tuple can be added!");
        return false;
    }

    uint64_t offSet = (numberOfRecords * this->getRecordSize());
    uint8_t* address = const_cast<uint8_t*>(basePointer + offSet);
    ++numberOfRecords;

    copyTupleFieldsToBuffer(record, address);

    tupleBuffer.setNumberOfTuples(numberOfRecords);
    NES_DEBUG("DynamicRowLayoutBuffer: numberOfRecords = " << numberOfRecords);

    return true;
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicRowLayoutBuffer::readRecord(uint64_t recordIndex) {
    if (boundaryChecks && recordIndex >= capacity) {
        NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutBuffer: Trying to access a record above capacity");
    }

    std::tuple<Types...> retTuple;

    uint64_t offSet = (recordIndex * this->getRecordSize());
    uint8_t* address = const_cast<uint8_t*>(basePointer + offSet);

    copyTupleFieldsFromBuffer(retTuple, address);

    return retTuple;
}

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
