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

class TaskQueue
{
public:
    virtual ~TaskQueue() = default;
    virtual ssize_t size(const QueryId& queryId, const WorkerThreadId& threadId) = 0;
    virtual std::shared_ptr<detail::Queue> accessQueueForReading(const QueryId& queryId, const WorkerThreadId& threadId) = 0;
    virtual std::shared_ptr<detail::Queue> accessQueueForWriting(const QueryId& queryId, const WorkerThreadId&) = 0;
    virtual void addedQuery(const QueryId& queryId, const WorkerThreadId& newWorkerThreadId) = 0;
};

}
