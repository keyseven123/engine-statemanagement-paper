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
#include <TaskQueue.hpp>

namespace NES
{

class SingleTaskQueue final : public TaskQueue
{
    detail::Queue taskQueue;

public:
    explicit SingleTaskQueue(const size_t taskQueueSize) : taskQueue(detail::Queue{taskQueueSize}) { }

    ~SingleTaskQueue() override = default;

    ssize_t size(const QueryId&, const WorkerThreadId&) override { return taskQueue.size(); }

    detail::Queue& accessQueueForReading(const QueryId&, const WorkerThreadId&) override { return taskQueue; }

    detail::Queue& accessQueueForWriting(const QueryId&, const WorkerThreadId&) override { return taskQueue; }

    void addedQuery(const QueryId&, const WorkerThreadId&) override
    {
        /// We do not care if a new query gets added, as we have one queue for every query and worker thread
    }
};

}
