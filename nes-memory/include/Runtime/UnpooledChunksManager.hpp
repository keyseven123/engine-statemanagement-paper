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

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <memory_resource>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/RollingAverage.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>

namespace NES
{

namespace Memory::detail
{
class MemorySegment;
}

/// Helper struct that stores necessary information for accessing unpooled chunks
/// Instead of allocating the exact needed space, we allocate a chunk of a space calculated by a rolling average of the last n sizes.
/// Thus, we (pre-)allocate potentially multiple buffers. At least, there is a high chance that one chunk contains multiple tuple buffers
struct UnpooledChunk
{
    static constexpr size_t NUM_PRE_ALLOCATED_CHUNKS = 10;
    static constexpr size_t ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE = 100;
    struct ChunkControlBlock
    {
        size_t totalSize = 0;
        size_t usedSize = 0;
        uint8_t* startOfChunk = nullptr;
        std::vector<std::unique_ptr<Memory::detail::MemorySegment>> unpooledMemorySegments;
        uint64_t activeMemorySegments = 0;
        uint64_t alignment = 0;

        friend std::ostream& operator<<(std::ostream& os, const ChunkControlBlock& chunkControlBlock)
        {
            return os << fmt::format(
                       "CCB {} ({}/{}B) with {} activeMemorySegments",
                       fmt::ptr(chunkControlBlock.startOfChunk),
                       chunkControlBlock.usedSize,
                       chunkControlBlock.totalSize,
                       chunkControlBlock.activeMemorySegments);
        }
    };

    void emplaceChunkControlBlock(uint8_t* chunkKey, std::unique_ptr<Memory::detail::MemorySegment> newMemorySegment);
    std::unordered_map<uint8_t*, ChunkControlBlock> chunks;
    uint8_t* lastAllocateChunkKey = nullptr;
    RollingAverage<size_t> rollingAverage{ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE};
};

/// Stores and tracks all memory chunks for unpooled / variable sized buffers
class UnpooledChunksManager : public Memory::BufferRecycler, public std::enable_shared_from_this<UnpooledChunksManager>
{

    /// Needed for allocating and deallocating memory
    std::shared_ptr<std::pmr::memory_resource> memoryResource;

    /// UnpooledBufferData is a shared_ptr, as we pass a shared_ptr to anyone that requires access to an unpooled buffer chunk
    folly::Synchronized<std::unordered_map<std::thread::id, std::shared_ptr<folly::Synchronized<UnpooledChunk>>>> allLocalUnpooledBuffers;

    /// Returns two pointers wrapped in a pair
    /// std::get<0>: the key that is being used in the unordered_map of a ChunkControlBlock
    /// std::get<1>: pointer to the memory address that is large enough for neededSize
    std::pair<uint8_t*, uint8_t*> allocateSpace(std::thread::id threadId, size_t neededSize, size_t alignment);

    std::shared_ptr<folly::Synchronized<UnpooledChunk>> getChunk(std::thread::id threadId);
    std::shared_ptr<folly::Synchronized<UnpooledChunk>> getLocalChunk(std::thread::id threadId);

    void recyclePooledBuffer(Memory::detail::MemorySegment* buffer) override;
    void recycleUnpooledBuffer(
        Memory::detail::MemorySegment* memorySegment, const Memory::ThreadIdCopyLastChunkPtr& threadCopyLastChunkPtr) override;

public:
    explicit UnpooledChunksManager(std::shared_ptr<std::pmr::memory_resource> memoryResource);
    UnpooledChunksManager() = delete;
    virtual ~UnpooledChunksManager() = default;
    size_t getNumberOfUnpooledBuffers() const;
    Memory::TupleBuffer getUnpooledBuffer(size_t neededSize, size_t alignment);
};




}

FMT_OSTREAM(NES::UnpooledChunk::ChunkControlBlock);
