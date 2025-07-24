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


#include <Runtime/UnpooledChunksManager.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <ranges>
#include <thread>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{
UnpooledChunksManager::UnpooledChunksManager(std::shared_ptr<std::pmr::memory_resource> memoryResource)
    : memoryResource(std::move(memoryResource))
{
}

ThreadLocalChunks::ThreadLocalChunks(const uint64_t windowSize, std::shared_ptr<std::pmr::memory_resource> memoryResource)
    : lastAllocateChunkKey(nullptr), rollingAverage(windowSize), chunkCache(std::move(memoryResource))
{
}

void ThreadLocalChunks::ChunkCache::insertIntoCache(ChunkControlBlock& chunkControlBlock)
{
    /// Checking if we need to move a chunk control block to the chunk cache
    /// If this is the case, we opt for the chunk control block that has the smallest memoryChunk
    if (ccbCache.size() >= chunkCacheSpace)
    {
        auto chunkToBeMoved = std::move(ccbCache.begin()->second);
        ccbCache.erase(ccbCache.begin());

        /// Checking if we need to remove and deallocate a chunk control block
        /// If this is the case, we opt for the smallest chunk as the larger a chunk is, the higher the chance we can reuse it
        if (chunksCache.size() >= chunkCacheSpace)
        {
            const auto [startOfChunk, alignment, totalSize] = chunksCache.begin()->second;
            chunksCache.erase(chunksCache.begin());
            memoryResource->deallocate(startOfChunk, totalSize, alignment);
        }

        /// Inserting the chunk control block from the CCB-cache as we have enough space for it now
        chunksCache.insert({chunkToBeMoved.memoryChunk.totalSize, chunkToBeMoved.memoryChunk});
    }

    /// Inserting the chunk control block as we have enough space for it
    chunkControlBlock.usedSize = 0;
    chunkControlBlock.activeMemorySegments = 0;
    ccbCache.insert({chunkControlBlock.memoryChunk.totalSize, std::move(chunkControlBlock)});
}

std::optional<ThreadLocalChunks::MemoryChunk> ThreadLocalChunks::ChunkCache::tryGetChunk(const size_t neededSize)
{
    /// Searching for a chunk that has a larger size than neededSize
    if (const auto it = chunksCache.lower_bound(neededSize); it != chunksCache.end())
    {
        auto recycledChunk = std::move(it->second);
        chunksCache.erase(it);
        return recycledChunk;
    }
    /// We could not find a chunk of a suitable size
    return {};
}

std::pair<ThreadLocalChunks::ChunkControlBlock, Memory::detail::MemorySegment*>
ThreadLocalChunks::ChunkCache::tryGetMemorySegment(const size_t neededSize)
{
    /// Searching for a memory that has a larger size than neededSize
    for (auto it = ccbCache.begin(); it != ccbCache.end(); ++it)
    {
        auto& chunkControlBlock = it->second;
        for (auto memSegmentIt = chunkControlBlock.unpooledMemorySegments.begin();
             memSegmentIt != chunkControlBlock.unpooledMemorySegments.end();
             ++memSegmentIt)
        {
            if ((*memSegmentIt)->size >= neededSize)
            {
                auto recycledChunk = std::move(it->second);
                auto memSegment = std::move(*memSegmentIt);
                recycledChunk.unpooledMemorySegments.clear();
                recycledChunk.unpooledMemorySegments.emplace_back(std::move(memSegment));
                ccbCache.erase(it);
                return {std::move(recycledChunk), memSegmentIt->get()};
            }
        }
    }

    /// We could not find a chunk of a suitable size
    return {{}, nullptr};
}

void ThreadLocalChunks::emplaceMemorySegment(uint8_t* chunkKey, std::unique_ptr<Memory::detail::MemorySegment> newMemorySegment)
{
    auto& curUnpooledChunk = chunks.at(chunkKey);
    curUnpooledChunk.unpooledMemorySegments.emplace_back(std::move(newMemorySegment));
}

void ThreadLocalChunks::insertIntoCache(ChunkControlBlock& chunkControlBlock)
{
    chunkCache.insertIntoCache(chunkControlBlock);
}

std::optional<ThreadLocalChunks::MemoryChunk> ThreadLocalChunks::tryGetChunk(const size_t neededSize)
{
    return chunkCache.tryGetChunk(neededSize);
}

Memory::detail::MemorySegment* ThreadLocalChunks::tryGetMemorySegment(const size_t neededSize)
{
    auto [chunkControlBlock, memSegment] = chunkCache.tryGetMemorySegment(neededSize);
    if (memSegment)
    {
        lastAllocateChunkKey = chunkControlBlock.memoryChunk.startOfChunk;
        chunkControlBlock.usedSize = neededSize;
        chunkControlBlock.activeMemorySegments = 1;
        chunks[chunkControlBlock.memoryChunk.startOfChunk] = std::move(chunkControlBlock);
    }
    return memSegment;
}


std::shared_ptr<folly::Synchronized<ThreadLocalChunks>> UnpooledChunksManager::getThreadLocalChunkForCurrentThread()
{
    thread_local std::shared_ptr<folly::Synchronized<ThreadLocalChunks>> localChunk
        = getThreadLocalChunkFromOtherThread(std::this_thread::get_id());
    return localChunk;
}

std::shared_ptr<folly::Synchronized<ThreadLocalChunks>> UnpooledChunksManager::getThreadLocalChunkFromOtherThread(std::thread::id threadId)
{
    thread_local std::unordered_map<std::thread::id, std::shared_ptr<folly::Synchronized<ThreadLocalChunks>>> localThreadLocalChunks
        = allThreadLocalChunks.copy();
    if (const auto existingChunk = localThreadLocalChunks.find(threadId); existingChunk != localThreadLocalChunks.end())
    {
        return existingChunk->second;
    }
    if (threadId == std::this_thread::get_id())
    {
        /// We have seen a new thread id and need to create a new UnpooledBufferChunkData for it.
        /// We only do this if the current thread does not have itself in the local hash map.
        /// We can always assume that every thread will call this method before anyother thread calls it
        auto newChunk
            = std::make_shared<folly::Synchronized<ThreadLocalChunks>>(ThreadLocalChunks(ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE, memoryResource));
        allThreadLocalChunks.wlock()->insert({threadId, newChunk});
    }

    localThreadLocalChunks = allThreadLocalChunks.copy();
    return localThreadLocalChunks.at(threadId);
}

size_t UnpooledChunksManager::getNumberOfUnpooledBuffers() const
{
    const auto lockedAllBufferChunkData = allThreadLocalChunks.rlock();
    size_t numOfUnpooledBuffers = 0;
    for (const auto& chunkData : *lockedAllBufferChunkData | std::views::values)
    {
        const auto rLockedChunkData = chunkData->rlock();
        numOfUnpooledBuffers += std::accumulate(
            rLockedChunkData->chunks.begin(),
            rLockedChunkData->chunks.end(),
            0,
            [](const auto& sum, const auto& item) { return sum + item.second.activeMemorySegments; });
    }
    return numOfUnpooledBuffers;
}

std::pair<uint8_t*, uint8_t*> UnpooledChunksManager::allocateSpace(const size_t neededSize, const size_t alignment)
{
    /// There exist two possibilities that can happen
    /// 1. We have enough space in an already allocated chunk or 2. we need to allocate a new chunk of memory

    const auto lockedLocalUnpooledBufferData = getThreadLocalChunkForCurrentThread()->wlock();
    const auto newRollingAverage = static_cast<size_t>(lockedLocalUnpooledBufferData->rollingAverage.add(neededSize));
    auto& localLastAllocatedChunkKey = lockedLocalUnpooledBufferData->lastAllocateChunkKey;
    auto& localUnpooledBufferChunkStorage = lockedLocalUnpooledBufferData->chunks;
    if (localUnpooledBufferChunkStorage.contains(localLastAllocatedChunkKey))
    {
        if (auto& currentAllocatedChunk = localUnpooledBufferChunkStorage.at(localLastAllocatedChunkKey);
            currentAllocatedChunk.usedSize + neededSize < currentAllocatedChunk.memoryChunk.totalSize)
        {
            /// There is enough space in the last allocated chunk. Thus, we can create a tuple buffer from the available space
            const auto localMemoryForNewTupleBuffer = currentAllocatedChunk.memoryChunk.startOfChunk + currentAllocatedChunk.usedSize;
            const auto localKeyForUnpooledBufferChunk = localLastAllocatedChunkKey;
            currentAllocatedChunk.activeMemorySegments += 1;
            currentAllocatedChunk.usedSize += neededSize;
            // NES_TRACE(
            //     "Added tuple buffer {} of {}B to: {}",
            //     fmt::ptr(localMemoryForNewTupleBuffer),
            //     neededSize,
            //     fmt::format("{}", currentAllocatedChunk));
            return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
        }
    }

    /// The last allocated chunk is not enough. Thus, we need to allocate a new chunk and insert it into the unpooled buffer storage
    /// The memory to allocate must be larger than bufferSize, while also taking the rolling average into account.
    /// We check if the chunk cache has a suitable chunk available
    if (const auto recycledChunkControlBlock = lockedLocalUnpooledBufferData->tryGetChunk(neededSize);
        recycledChunkControlBlock.has_value())
    {
        // Updating the local last allocate chunk key and adding the new chunk to the local chunk storage
        localLastAllocatedChunkKey = recycledChunkControlBlock.value().startOfChunk;
        const auto localKeyForUnpooledBufferChunk = recycledChunkControlBlock.value().startOfChunk;
        const auto localMemoryForNewTupleBuffer = recycledChunkControlBlock.value().startOfChunk;
        auto& currentAllocatedChunk = localUnpooledBufferChunkStorage[localKeyForUnpooledBufferChunk];
        currentAllocatedChunk.memoryChunk.startOfChunk = recycledChunkControlBlock.value().startOfChunk;
        currentAllocatedChunk.memoryChunk.totalSize = recycledChunkControlBlock.value().totalSize;
        currentAllocatedChunk.memoryChunk.alignment = alignment;
        currentAllocatedChunk.usedSize = neededSize;
        currentAllocatedChunk.activeMemorySegments = 1;
        return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
    }


    /// We could not find a suitable chunk in the chunk cache. Therefore, we need to allocate new memory.
    /// For now, we allocate multiple localLastAllocateChunkKeyrolling averages. If this is too small for the current bufferSize, we allocate at least the bufferSize
    const auto newAllocationSizeExact = std::max(neededSize, newRollingAverage * NUM_PRE_ALLOCATED_CHUNKS);
    const auto newAllocationSize = (newAllocationSizeExact + 4095U) & ~4095U; /// Round to the nearest multiple of 4KB (page size)
    auto* const newlyAllocatedMemory = static_cast<uint8_t*>(memoryResource->allocate(newAllocationSize, alignment));
    if (newlyAllocatedMemory == nullptr)
    {
        NES_WARNING("Could not allocate {} bytes for unpooled chunk!", newAllocationSize);
        return {};
    }

    /// Updating the local last allocate chunk key and adding the new chunk to the local chunk storage
    localLastAllocatedChunkKey = newlyAllocatedMemory;
    const auto localKeyForUnpooledBufferChunk = newlyAllocatedMemory;
    const auto localMemoryForNewTupleBuffer = newlyAllocatedMemory;
    auto& currentAllocatedChunk = localUnpooledBufferChunkStorage[localKeyForUnpooledBufferChunk];
    currentAllocatedChunk.memoryChunk.startOfChunk = newlyAllocatedMemory;
    currentAllocatedChunk.memoryChunk.totalSize = newAllocationSize;
    currentAllocatedChunk.memoryChunk.alignment = alignment;
    currentAllocatedChunk.usedSize = neededSize;
    currentAllocatedChunk.activeMemorySegments = 1;
    // NES_TRACE("Created new chunk {} for tuple buffer {} of {}B", currentAllocatedChunk, fmt::ptr(localMemoryForNewTupleBuffer), neededSize);
    return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
}

void UnpooledChunksManager::recyclePooledBuffer(Memory::detail::MemorySegment*)
{
    INVARIANT(false, "Should not be called!");
}

void UnpooledChunksManager::recycleUnpooledBuffer(
    Memory::detail::MemorySegment*, const Memory::ThreadIdCopyLastChunkPtr& threadCopyLastChunkPtr)
{
    const auto chunk = getThreadLocalChunkFromOtherThread(threadCopyLastChunkPtr.threadId);
    auto lockedLocalUnpooledBufferData = chunk->wlock();
    auto& curUnpooledChunk = lockedLocalUnpooledBufferData->chunks[threadCopyLastChunkPtr.lastChunkPtr];
    INVARIANT(
        curUnpooledChunk.activeMemorySegments > 0,
        "curUnpooledChunk.activeMemorySegments must be larger than 0 but is {}",
        curUnpooledChunk.activeMemorySegments);
    curUnpooledChunk.activeMemorySegments -= 1;
    if (curUnpooledChunk.activeMemorySegments == 0)
    {
        /// All memory segments have been removed, therefore, we move the ccb into the cache
        const auto extractedChunk = lockedLocalUnpooledBufferData->chunks.extract(threadCopyLastChunkPtr.lastChunkPtr);
        lockedLocalUnpooledBufferData->lastAllocateChunkKey = nullptr;
        lockedLocalUnpooledBufferData->insertIntoCache(extractedChunk.mapped());
    }
}

Memory::TupleBuffer UnpooledChunksManager::getUnpooledBuffer(const size_t neededSize, const size_t alignment)
{
    /// We first check if there are some recycled chunks/memory segments that we can use
    /// If this is not the case, we ask the chunk manager to allocate new space
    const auto chunk = this->getThreadLocalChunkForCurrentThread();
    Memory::detail::MemorySegment* leakedMemSegment = nullptr;

    /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
    const auto alignedBufferSizePlusControlBlock = Memory::alignBufferSize(neededSize, alignment);
    if (leakedMemSegment = chunk->wlock()->tryGetMemorySegment(alignedBufferSizePlusControlBlock); leakedMemSegment == nullptr)
    {
        /// Getting space from the unpooled chunks manager
        const auto& [localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer]
            = this->allocateSpace(alignedBufferSizePlusControlBlock, alignment);
        /// Creating a new memory segment, and adding it to the unpooledMemorySegments
        const auto alignedBufferSize = Memory::alignBufferSize(neededSize, alignment);
        auto memSegment = std::make_unique<Memory::detail::MemorySegment>(
            localMemoryForNewTupleBuffer,
            alignedBufferSize,
            [copyOfLastChunkPtr = localKeyForUnpooledBufferChunk,
             copyOfThreadId = std::this_thread::get_id()](Memory::detail::MemorySegment* memorySegment, Memory::BufferRecycler* recycler)
            {
                /// We need to store the last chunk ptr and the thread id to find the memory segment in the ThreadLocalChunks.
                /// This is necessary, as another thread than the allocation thread, might return the allocated memorysegment
                const Memory::ThreadIdCopyLastChunkPtr threadIdCopyLastChunkPtr{copyOfThreadId, copyOfLastChunkPtr};
                recycler->recycleUnpooledBuffer(memorySegment, threadIdCopyLastChunkPtr);
            });

        leakedMemSegment = memSegment.get();
        {
            /// Inserting the memory segment into the unpooled buffer storage
            const auto lockedLocalUnpooledBufferData = chunk->wlock();
            lockedLocalUnpooledBufferData->emplaceMemorySegment(localKeyForUnpooledBufferChunk, std::move(memSegment));
        }
    }
    INVARIANT(leakedMemSegment != nullptr, "Memory segment is null!");
    if (leakedMemSegment->controlBlock->prepare(shared_from_this()))
    {
        return Memory::TupleBuffer(leakedMemSegment->controlBlock.get(), leakedMemSegment->ptr, neededSize);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

}
