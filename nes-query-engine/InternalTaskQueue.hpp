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
#include <folly/MPMCQueue.h>
#include <Task.hpp>

namespace NES
{

namespace detail
{
using Queue = folly::MPMCQueue<Task>;
}

class InternalTaskQueue
{
    std::vector<detail::Queue> internalTaskQueues;
    std::atomic<uint64_t> nextQueueForWriting = 0;

    /// Queue reference always outlives the actual queue
    static thread_local std::optional<std::reference_wrapper<folly::MPMCQueue<Task>>> ownQueue;

    detail::Queue& getOwnQueue(const WorkerThreadId& threadId)
    {
        if (not ownQueue.has_value())
        {
            ownQueue = internalTaskQueues[threadId % internalTaskQueues.size()];
        }
        return ownQueue.value();
    }

public:
    InternalTaskQueue(const size_t numberOfQueues, const size_t internalTaskQueueSize)
    {
        internalTaskQueues.reserve(numberOfQueues);
        for (size_t i = 0; i < numberOfQueues; ++i)
        {
            internalTaskQueues.emplace_back(detail::Queue{internalTaskQueueSize});
        }
    }

    ssize_t size(const WorkerThreadId& threadId) { return getOwnQueue(threadId).size(); }

    detail::Queue& accessQueueForReading(const WorkerThreadId& threadId) { return getOwnQueue(threadId); }

    detail::Queue& accessQueueForWriting()
    {
        const auto queuePos = nextQueueForWriting % internalTaskQueues.size();
        ++nextQueueForWriting;
        return internalTaskQueues[queuePos];
    }
};

}
