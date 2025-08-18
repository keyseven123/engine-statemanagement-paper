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

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES::Memory::MemoryLayouts
{

class MemoryLayoutTupleBuffer;

/// @brief Returns a new tuple buffer of at least the newBufferSize. It tries to first get a fixed pool buffer. If this does not work, it
/// fall-backs to an unpooled buffer
/// @return TupleBuffer
TupleBuffer getNewBuffer(AbstractBufferProvider* tupleBufferProvider, uint32_t newBufferSize);


/// @brief Reads the variable sized data from the child buffer at the provided index and returns the pointer to the var sized data
/// @return Pointer to variable sized data
const int8_t* loadAssociatedVarSizedValue(const TupleBuffer* tupleBuffer, uint64_t combinedIdxOffset);

/// @brief Reads the variable sized data from the child buffer at the provided index. Similar as loadAssociatedVarSizedValue, but returns a string
/// @return Variable sized data as a string
std::string readVarSizedDataAsString(const TupleBuffer& tupleBuffer, uint64_t combinedIdxOffset);

/// @brief Writes the varSizedValue to the tupleBuffer. Similar to writeVarSizedData, but this method expects the varSizedValue containing
/// the length of varSizedValue as its first 32-bits
/// @return Combined child index and offset
uint64_t storeAssociatedVarSizedValue(
    const TupleBuffer* tupleBuffer, AbstractBufferProvider* bufferProvider, const int8_t* varSizedValue, uint32_t varSizedValueLength);

/// @brief Writes the variable sized data to the buffer
/// @param buffer
/// @param value
/// @param bufferProvider
/// @return Index of the child buffer
uint64_t writeVarSizedData(const TupleBuffer& buffer, std::string_view value, AbstractBufferProvider& bufferProvider);

/// @brief A MemoryLayout defines a strategy in which a specific schema / a individual tuple is mapped to a tuple buffer.
/// To this end, it requires the definition of an schema and a specific buffer size.
/// Currently. we support a RowLayout and a ColumnLayout.
class MemoryLayout
{
public:
    /// @brief Constructor for MemoryLayout.
    /// @param bufferSize A memory layout is always created for a specific buffer size.
    /// @param schema A memory layout is always created for a specific schema.
    MemoryLayout(uint64_t bufferSize, Schema schema);
    MemoryLayout(const MemoryLayout&) = default;

    virtual ~MemoryLayout() = default;

    /// Gets the field index for a specific field name. If the field name not exists, we return an empty optional.
    /// @return either field index for fieldName or empty optional
    [[nodiscard]] std::optional<uint64_t> getFieldIndexFromName(const std::string& fieldName) const;

    /// Calculates the offset in the tuple buffer of a particular field for a specific tuple.
    /// Depending on the concrete MemoryLayout, e.g., Columnar or Row - Layout, this may result in different calculations.
    [[nodiscard]] virtual uint64_t getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const = 0;

    /// @brief Gets the number of tuples a tuple buffer with this memory layout could occupy.
    /// Depending on the concrete memory layout this value may change, e.g., some layouts may add some padding or alignment.
    /// @return number of tuples a tuple buffer can occupy.
    [[nodiscard]] uint64_t getCapacity() const;

    [[nodiscard]] uint64_t getTupleSize() const;
    [[nodiscard]] uint64_t getBufferSize() const;
    void setBufferSize(uint64_t bufferSize);
    [[nodiscard]] const Schema& getSchema() const;
    [[nodiscard]] DataType getPhysicalType(uint64_t fieldIndex) const;
    [[nodiscard]] uint64_t getFieldSize(uint64_t fieldIndex) const;
    [[nodiscard]] std::vector<std::string> getKeyFieldNames() const;
    void setKeyFieldNames(const std::vector<std::string>& keyFields);
    bool operator==(const MemoryLayout& rhs) const = default;
    bool operator!=(const MemoryLayout& rhs) const = default;

protected:
    uint64_t bufferSize;
    Schema schema;
    uint64_t recordSize;
    uint64_t capacity;
    std::vector<uint64_t> physicalFieldSizes;
    std::vector<DataType> physicalTypes;
    std::unordered_map<std::string, uint64_t> nameFieldIndexMap;
    std::vector<std::string> keyFieldNames;
};


}
