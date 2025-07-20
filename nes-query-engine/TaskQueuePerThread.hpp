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

/// Each query has its own internal task queue
class TaskQueuePerQuery final : public TaskQueue
{
    size_t taskQueueSize;
    std::map<QueryId, std::shared_ptr<detail::Queue>> taskQueues;


    std::shared_ptr<detail::Queue> getOwnQueue(const QueryId& queryId)
    {
        return taskQueues[queryId];
    }

public:
    explicit TaskQueuePerQuery(const size_t taskQueueSize) : taskQueueSize(taskQueueSize) {}
    ssize_t size(const QueryId& queryId, const WorkerThreadId&) override { return getOwnQueue(queryId)->size(); }
    std::shared_ptr<detail::Queue> accessQueueForReading(const QueryId& queryId, const WorkerThreadId&) override { return getOwnQueue(queryId); }
    std::shared_ptr<detail::Queue> accessQueueForWriting(const QueryId& queryId, const WorkerThreadId&) override { return getOwnQueue(queryId); }
    void addedQuery(const QueryId& queryId, const WorkerThreadId&) override
    {
        PRECONDITION(not taskQueues.contains(queryId), "QueryId {} should not exist already ", queryId);

        /// We need to add a new queue for the new query
        taskQueues[queryId] = std::make_shared<detail::Queue>(taskQueueSize);
    };
    ~TaskQueuePerQuery() override = default;
};

}
