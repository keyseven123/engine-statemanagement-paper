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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MEMORYSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MEMORYSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {
/**
 * @brief Descriptor defining properties used for creating physical memory source
 */
class MemorySourceDescriptor : public SourceDescriptor {
  public:
    /**
     * @brief Ctor of a MemorySourceDescriptor
     * @param schema the schame of the source
     * @param memoryArea a non-null pointer to the area of memory to use in the source
     * @param memoryAreaSize the size of the area of memory
     */
    explicit MemorySourceDescriptor(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize,
                                    uint64_t numBuffersToProcess, std::chrono::milliseconds frequency);

    /**
     * @brief Factory method to create a MemorySourceDescriptor object
     * @param schema the schame of the source
     * @param memoryArea a non-null pointer to the area of memory to use in the source
     * @param memoryAreaSize the size of the area of memory
     * @return a correctly initialized shared ptr to MemorySourceDescriptor
     */
    static std::shared_ptr<MemorySourceDescriptor> create(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea,
                                                          size_t memoryAreaSize, uint64_t numBuffersToProcess,
                                                          std::chrono::milliseconds frequency);

    /**
     * @brief Provides the string representation of the memory source
     * @return the string representation of the memory source
     */
    std::string toString() override;

    /**
     * @brief Equality method to compare two source descriptors stored as shared_ptr
     * @param other the source descriptor to compare against
     * @return true if type, schema, and memory area are equal
     */
    bool equal(SourceDescriptorPtr other) override;

    /**
     * @brief returns the shared ptr to the memory area
     * @return the shared ptr to the memory area
     */
    std::shared_ptr<uint8_t> getMemoryArea();

    /**
     * @brief returns the size of the stored memory area
     * @return the size of the stored memory area
     */
    size_t getMemoryAreaSize() const;

    uint64_t getNumBuffersToProcess() const;
    std::chrono::milliseconds getFrequency() const;

  private:
    std::shared_ptr<uint8_t> memoryArea;
    size_t memoryAreaSize;
    uint64_t numBuffersToProcess;
    std::chrono::milliseconds frequency;
};
}// namespace NES

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MEMORYSOURCEDESCRIPTOR_HPP_
