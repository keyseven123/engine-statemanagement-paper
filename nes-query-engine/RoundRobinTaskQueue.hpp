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
#include <TaskQueue.hpp>

namespace NES
{

class RoundRobinTaskQueue final : public TaskQueue
{
    std::vector<std::shared_ptr<detail::Queue>> taskQueues;
    std::atomic<uint64_t> nextQueueForWriting = 0;


    std::shared_ptr<detail::Queue> getOwnQueue(const WorkerThreadId& threadId) { return taskQueues[threadId % taskQueues.size()]; }

public:
    RoundRobinTaskQueue(const size_t numberOfQueues, const size_t taskQueueSize)
    {
        taskQueues.reserve(numberOfQueues);
        for (size_t i = 0; i < numberOfQueues; ++i)
        {
            taskQueues.emplace_back(std::make_shared<detail::Queue>(taskQueueSize));
        }
    }

    ssize_t size(const QueryId&, const WorkerThreadId& threadId) override { return getOwnQueue(threadId)->size(); }
    std::shared_ptr<detail::Queue> accessQueueForReading(const QueryId&, const WorkerThreadId& threadId) override { return getOwnQueue(threadId); }
    std::shared_ptr<detail::Queue> accessQueueForWriting(const QueryId&, const WorkerThreadId&) override
    {
        const auto queuePos = nextQueueForWriting % taskQueues.size();
        ++nextQueueForWriting;
        return taskQueues[queuePos];
    }
    void addedQuery(const QueryId&, const WorkerThreadId&) override {
        /// We do not care if a new query gets added, as we map the queries to the worker threads
    };
    ~RoundRobinTaskQueue() override = default;
};

}
