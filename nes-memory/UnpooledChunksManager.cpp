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
#include <unordered_map>
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


void UnpooledChunk::emplaceChunkControlBlock(uint8_t* chunkKey, std::unique_ptr<Memory::detail::MemorySegment> newMemorySegment)
{
    auto& curUnpooledChunk = chunks.at(chunkKey);
    curUnpooledChunk.unpooledMemorySegments.emplace_back(std::move(newMemorySegment));
}


thread_local std::shared_ptr<folly::Synchronized<UnpooledChunk>> localUnpooledBuffer = nullptr;
std::shared_ptr<folly::Synchronized<UnpooledChunk>> UnpooledChunksManager::getLocalChunk(const std::thread::id threadId)
{
    if (not localUnpooledBuffer) [[unlikely]]
    {
        /// We have not initialized the thread_local variant
        localUnpooledBuffer = getChunk(threadId);
    }

    return localUnpooledBuffer;
}

std::shared_ptr<folly::Synchronized<UnpooledChunk>> UnpooledChunksManager::getChunk(std::thread::id threadId)
{
    {
        const auto lockedUnpooledBuffers = allLocalUnpooledBuffers.rlock();
        if (const auto existingChunk = lockedUnpooledBuffers->find(threadId); existingChunk != lockedUnpooledBuffers->cend())
        {
            return existingChunk->second;
        }
    }

    /// We have seen a new thread id and need to create a new UnpooledBufferChunkData for it
    auto newUnpooledChunk = std::make_shared<folly::Synchronized<UnpooledChunk>>();
    allLocalUnpooledBuffers.wlock()->insert({threadId, newUnpooledChunk});
    return newUnpooledChunk;
}

size_t UnpooledChunksManager::getNumberOfUnpooledBuffers() const
{
    const auto lockedAllBufferChunkData = allLocalUnpooledBuffers.rlock();
    size_t numOfUnpooledBuffers = 0;
    uint64_t sizeOfAllocatedBytes = 0;
    for (const auto& chunkData : *lockedAllBufferChunkData | std::views::values)
    {
        const auto rLockedChunkData = chunkData->rlock();
        numOfUnpooledBuffers += std::accumulate(
            rLockedChunkData->chunks.begin(),
            rLockedChunkData->chunks.end(),
            0,
            [](const auto& sum, const auto& item) { return sum + item.second.activeMemorySegments; });
        sizeOfAllocatedBytes += std::accumulate(
            rLockedChunkData->chunks.begin(),
            rLockedChunkData->chunks.end(),
            static_cast<size_t>(0),
            [](const auto& sum, const auto& item) { return sum + item.second.totalSize; });
    }
    return numOfUnpooledBuffers;
}

std::pair<uint8_t*, uint8_t*>
UnpooledChunksManager::allocateSpace(const std::thread::id threadId, const size_t neededSize, const size_t alignment)
{
    /// There exist two possibilities that can happen
    /// 1. We have enough space in an already allocated chunk or 2. we need to allocate a new chunk of memory
    size_t newAllocationSizeExact = 0;
    {
        const auto lockedLocalUnpooledBufferData = getLocalChunk(threadId)->wlock();
        const auto newRollingAverage = static_cast<size_t>(lockedLocalUnpooledBufferData->rollingAverage.add(neededSize));
        newAllocationSizeExact = std::max(neededSize, newRollingAverage * UnpooledChunk::NUM_PRE_ALLOCATED_CHUNKS);
        auto& localLastAllocatedChunkKey = lockedLocalUnpooledBufferData->lastAllocateChunkKey;
        auto& localUnpooledBufferChunkStorage = lockedLocalUnpooledBufferData->chunks;
        if (localUnpooledBufferChunkStorage.contains(localLastAllocatedChunkKey))
        {
            if (auto& currentAllocatedChunk = localUnpooledBufferChunkStorage.at(localLastAllocatedChunkKey);
                currentAllocatedChunk.usedSize + neededSize <= currentAllocatedChunk.totalSize)
            {
                /// There is enough space in the last allocated chunk. Thus, we can create a tuple buffer from the available space
                const auto localMemoryForNewTupleBuffer = currentAllocatedChunk.startOfChunk + currentAllocatedChunk.usedSize;
                const auto localKeyForUnpooledBufferChunk = localLastAllocatedChunkKey;
                currentAllocatedChunk.activeMemorySegments += 1;
                currentAllocatedChunk.usedSize += neededSize;
                return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
            }
        }
    }

    /// The last allocated chunk is not enough. Thus, we need to allocate a new chunk and insert it into the unpooled buffer storage
    /// The memory to allocate must be larger than bufferSize, while also taking the rolling average into account.
    /// For now, we allocate multiple localLastAllocateChunkKeyrolling averages. If this is too small for the current bufferSize, we allocate at least the bufferSize
    const auto newAllocationSize = (newAllocationSizeExact + 4095UL) & ~4095UL; /// Round to the nearest multiple of 4KB (page size)
    auto* const newlyAllocatedMemory = static_cast<uint8_t*>(memoryResource->allocate(newAllocationSize, alignment));
    if (newlyAllocatedMemory == nullptr)
    {
        NES_WARNING("Could not allocate {} bytes for unpooled chunk!", newAllocationSize);
        return {};
    }

    /// Updating the local last allocate chunk key and adding the new chunk to the local chunk storage
    auto lockedLocalUnpooledBufferData = getLocalChunk(threadId)->wlock();
    auto& localLastAllocatedChunkKey = lockedLocalUnpooledBufferData->lastAllocateChunkKey;
    localLastAllocatedChunkKey = newlyAllocatedMemory;
    const auto localKeyForUnpooledBufferChunk = newlyAllocatedMemory;
    const auto localMemoryForNewTupleBuffer = newlyAllocatedMemory;
    auto& currentAllocatedChunk = lockedLocalUnpooledBufferData->chunks[localKeyForUnpooledBufferChunk];
    lockedLocalUnpooledBufferData.unlock();

    currentAllocatedChunk.startOfChunk = newlyAllocatedMemory;
    currentAllocatedChunk.totalSize = newAllocationSize;
    currentAllocatedChunk.usedSize += neededSize;
    currentAllocatedChunk.activeMemorySegments += 1;
    currentAllocatedChunk.alignment = alignment;
    return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
}

void UnpooledChunksManager::recyclePooledBuffer(Memory::detail::MemorySegment*)
{
    INVARIANT(false, "This method should not be called!");
}

void UnpooledChunksManager::recycleUnpooledBuffer(
    Memory::detail::MemorySegment* memorySegment, const Memory::ThreadIdCopyLastChunkPtr& threadCopyLastChunkPtr)
{
    const auto chunk = this->getChunk(threadCopyLastChunkPtr.threadId);
    bool shallDeallocate = false;
    {
        const auto lockedLocalUnpooledBufferData = chunk->wlock();
        auto& curUnpooledChunk = lockedLocalUnpooledBufferData->chunks.at(threadCopyLastChunkPtr.lastChunkPtr);
        INVARIANT(
            curUnpooledChunk.activeMemorySegments > 0,
            "curUnpooledChunk.activeMemorySegments must be larger than 0 but is {}",
            curUnpooledChunk.activeMemorySegments);
        curUnpooledChunk.activeMemorySegments -= 1;
        memorySegment->size = 0;
        shallDeallocate = curUnpooledChunk.activeMemorySegments == 0;
    }
    if (shallDeallocate)
    {
        /// All memory segments have been removed, therefore, we can deallocate the unpooled chunk
        auto extractedLockedUnpooledBufferData = [&]()
        {
            const auto lockedLocalUnpooledBufferData = chunk->wlock();
            if (lockedLocalUnpooledBufferData->lastAllocateChunkKey == threadCopyLastChunkPtr.lastChunkPtr)
            {
                /// As we are deallocating the last allocated chunk, we need to reset the key to nullptr.
                /// Thus, we know that in the next getUnpooledBuffer(), we have to create/allocate a new memory
                lockedLocalUnpooledBufferData->lastAllocateChunkKey = nullptr;
            }
            return lockedLocalUnpooledBufferData->chunks.extract(threadCopyLastChunkPtr.lastChunkPtr);
        }();
        const auto& unpooledChunk = extractedLockedUnpooledBufferData.mapped();
        const auto startOfChunk = unpooledChunk.startOfChunk;
        const auto totalSize = unpooledChunk.totalSize;
        const auto alignment = unpooledChunk.alignment;
        memoryResource->deallocate(startOfChunk, totalSize, alignment);
    }
}

Memory::TupleBuffer UnpooledChunksManager::getUnpooledBuffer(const size_t neededSize, const size_t alignment)
{
    const auto threadId = std::this_thread::get_id();

    /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
    const auto alignedBufferSizePlusControlBlock
        = Memory::alignBufferSize(neededSize + sizeof(Memory::detail::BufferControlBlock), alignment);

    /// Getting space from the unpooled chunks manager
    const auto& [localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer]
        = this->allocateSpace(threadId, alignedBufferSizePlusControlBlock, alignment);

    /// Creating a new memory segment, and adding it to the unpooledMemorySegments
    const auto alignedBufferSize = Memory::alignBufferSize(neededSize, alignment);
    const auto controlBlockSize = Memory::alignBufferSize(sizeof(Memory::detail::BufferControlBlock), alignment);
    auto memSegment = std::make_unique<Memory::detail::MemorySegment>(
        localMemoryForNewTupleBuffer + controlBlockSize,
        alignedBufferSize,
        [threadId, localKeyForUnpooledBufferChunk](Memory::detail::MemorySegment* segment, BufferRecycler* recycler)
        {
            const Memory::ThreadIdCopyLastChunkPtr threadCopyLastChunkPtr{threadId, localKeyForUnpooledBufferChunk};
            recycler->recycleUnpooledBuffer(segment, threadCopyLastChunkPtr);
        });

    auto* leakedMemSegment = memSegment.get();
    {
        /// Inserting the memory segment into the unpooled buffer storage
        const auto chunk = this->getLocalChunk(threadId);
        const auto lockedLocalUnpooledBufferData = chunk->wlock();
        lockedLocalUnpooledBufferData->emplaceChunkControlBlock(localKeyForUnpooledBufferChunk, std::move(memSegment));
    }

    if (leakedMemSegment->controlBlock->prepare(shared_from_this()))
    {
        return Memory::TupleBuffer(leakedMemSegment->controlBlock.get(), leakedMemSegment->ptr, neededSize);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

}
