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
#include <thread>
#include "../../TupleBufferImpl.hpp"

namespace NES::Memory
{

/// Stores necessary information for the recycle unpooled buffer callback
struct ThreadIdCopyLastChunkPtr
{
    ThreadIdCopyLastChunkPtr(std::thread::id threadId, uint8_t* lastChunkPtr)
        : threadId(std::move(threadId)), lastChunkPtr(lastChunkPtr) { }

    ThreadIdCopyLastChunkPtr(ThreadIdCopyLastChunkPtr&& other) = default;
    ThreadIdCopyLastChunkPtr(const ThreadIdCopyLastChunkPtr& other) = default;

    ThreadIdCopyLastChunkPtr& operator=(const ThreadIdCopyLastChunkPtr& other)
    {
        threadId = other.threadId;
        lastChunkPtr = other.lastChunkPtr;
        return *this;
    }

    bool operator==(const ThreadIdCopyLastChunkPtr& other) const
    {
        return threadId == other.threadId && lastChunkPtr == other.lastChunkPtr;
    }

    std::thread::id threadId;
    uint8_t* lastChunkPtr;
};

///@brief Interface for buffer recycling mechanism
class BufferRecycler
{
public:
    virtual ~BufferRecycler() = default;
    /// @brief Interface method for pooled buffer recycling
    /// @param buffer the buffer to recycle
    virtual void recyclePooledBuffer(detail::MemorySegment* buffer) = 0;

    /// @brief Interface method for unpooled buffer recycling
    /// @param buffer the buffer to recycle
    /// @param threadIdCopyLastChunkPtr stores the thread id and last chunk ptr
    virtual void recycleUnpooledBuffer(detail::MemorySegment* buffer, const ThreadIdCopyLastChunkPtr& threadIdCopyLastChunkPtr) = 0;
};

}
