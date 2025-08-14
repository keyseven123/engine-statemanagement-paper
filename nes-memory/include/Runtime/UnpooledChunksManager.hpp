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
#include <map>
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


/// Helper struct that stores necessary information for accessing unpooled chunks
/// Instead of allocating the exact needed space, we allocate a chunk of a space calculated by a rolling average of the last n sizes.
/// Thus, we (pre-)allocate potentially multiple buffers. At least, there is a high chance that one chunk contains multiple tuple buffers
struct ThreadLocalChunks
{
    struct MemoryChunk
    {
        uint8_t* startOfChunk = nullptr;
        size_t alignment = 0;
        size_t totalSize = 0;
    };

    struct ChunkControlBlock
    {
        MemoryChunk memoryChunk;
        size_t usedSize = 0;
        std::vector<std::unique_ptr<Memory::detail::MemorySegment>> unpooledMemorySegments;
        uint64_t activeMemorySegments = 0;

        ChunkControlBlock() = default;
        ChunkControlBlock(const ChunkControlBlock& other) = delete;
        ChunkControlBlock(ChunkControlBlock&& other) noexcept
            : memoryChunk(std::move(other.memoryChunk))
            , usedSize(std::move(other.usedSize))
            , unpooledMemorySegments(std::move(other.unpooledMemorySegments))
            , activeMemorySegments(std::move(other.activeMemorySegments))
        {
        }
        ChunkControlBlock& operator=(const ChunkControlBlock& other) = delete;
        ChunkControlBlock& operator=(ChunkControlBlock&& other) noexcept
        {
            memoryChunk = std::move(other.memoryChunk);
            usedSize = std::move(other.usedSize);
            unpooledMemorySegments = std::move(other.unpooledMemorySegments);
            activeMemorySegments = std::move(other.activeMemorySegments);
            return *this;
        }

        friend std::ostream& operator<<(std::ostream& os, const ChunkControlBlock& chunkControlBlock)
        {
            return os << fmt::format(
                       "CCB {} ({}/{}B) with {} activeMemorySegments",
                       fmt::ptr(chunkControlBlock.memoryChunk.startOfChunk),
                       chunkControlBlock.usedSize,
                       chunkControlBlock.memoryChunk.totalSize,
                       chunkControlBlock.activeMemorySegments);
        }
    };

    /// Stores the last chunks so that we reduce the amount of allocate()/deallocate() calls
    class ChunkCache
    {
        /// Needed for deallocating memory, if we need to free-up space in the cache
        std::shared_ptr<std::pmr::memory_resource> memoryResource;
        std::multimap<size_t, MemoryChunk> chunksCache;
        uint64_t chunkCacheSpace = 10;

    public:
        explicit ChunkCache(std::shared_ptr<std::pmr::memory_resource> memoryResource) : memoryResource(std::move(memoryResource)) { }
        void insertIntoCache(ChunkControlBlock& chunkControlBlock);
        std::optional<MemoryChunk> tryGetChunk(size_t neededSize);
    };

    explicit ThreadLocalChunks(uint64_t windowSize, std::shared_ptr<std::pmr::memory_resource> memoryResource);
    void emplaceMemorySegment(uint8_t* chunkKey, std::unique_ptr<Memory::detail::MemorySegment> newMemorySegment);
    void insertIntoCache(ChunkControlBlock& chunkControlBlock);
    std::optional<MemoryChunk> tryGetChunk(size_t neededSize);


    std::unordered_map<uint8_t*, ChunkControlBlock> chunks;
    uint8_t* lastAllocateChunkKey;
    RollingAverage<size_t> rollingAverage;

private:
    /// We have this chunk cache private, as we want to control access to it. The access should happen via the methods of ThreadLocalChunks
    ChunkCache chunkCache;
};

/// Stores and tracks all memory chunks for unpooled / variable sized buffers
class UnpooledChunksManager final : public std::enable_shared_from_this<UnpooledChunksManager>, public Memory::BufferRecycler
{
    static constexpr auto NUM_PRE_ALLOCATED_CHUNKS = 10;
    static constexpr auto ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE = 100;

    /// Needed for allocating and deallocating memory
    std::shared_ptr<std::pmr::memory_resource> memoryResource;

    /// UnpooledBufferData is a shared_ptr, as we pass a shared_ptr to anyone that requires access to an unpooled buffer chunk
    folly::Synchronized<std::unordered_map<std::thread::id, std::shared_ptr<folly::Synchronized<ThreadLocalChunks>>>> allThreadLocalChunks;

    /// Returns two pointers wrapped in a pair
    /// std::get<0>: the key that is being used in the unordered_map of a ChunkControlBlock
    /// std::get<1>: pointer to the memory address that is large enough for neededSize
    std::pair<uint8_t*, uint8_t*> allocateSpace(size_t neededSize, size_t alignment);

    /// Gets the thread local chunk for the current threadId
    std::shared_ptr<folly::Synchronized<ThreadLocalChunks>> getThreadLocalChunkForCurrentThread();
    std::shared_ptr<folly::Synchronized<ThreadLocalChunks>> getThreadLocalChunkFromOtherThread(std::thread::id threadId);

    void recyclePooledBuffer(Memory::detail::MemorySegment* buffer) override;
    void
    recycleUnpooledBuffer(Memory::detail::MemorySegment* buffer, const Memory::ThreadIdCopyLastChunkPtr& threadCopyLastChunkPtr) override;

public:
    explicit UnpooledChunksManager(std::shared_ptr<std::pmr::memory_resource> memoryResource);
    ~UnpooledChunksManager() override = default;
    size_t getNumberOfUnpooledBuffers() const;
    Memory::TupleBuffer getUnpooledBuffer(size_t neededSize, size_t alignment);
};

}

FMT_OSTREAM(NES::ThreadLocalChunks::ChunkControlBlock);
