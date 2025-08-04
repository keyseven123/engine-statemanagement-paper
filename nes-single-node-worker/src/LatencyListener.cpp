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

#include <iostream>
#include <queue>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/ThreadNaming.hpp>
#include <folly/Synchronized.h>
#include <LatencyListener.hpp>

namespace NES
{
namespace
{

struct TaskIntermediateStore
{
    TaskIntermediateStore(
        QueryId queryId, const uint64_t bytes, const uint64_t numberOfTuples, ChronoClock::time_point startTimePoint)
        : queryId(std::move(queryId))
        , bytes(bytes)
        , numberOfTuples(numberOfTuples)
        , startTimePoint(std::move(startTimePoint))
    {
    }
    explicit TaskIntermediateStore() : queryId(INVALID_QUERY_ID), bytes(0), numberOfTuples(0) { }
    QueryId queryId;
    uint64_t bytes;
    uint64_t numberOfTuples;
    ChronoClock::time_point startTimePoint;
};

void threadRoutine(const std::stop_token& token, folly::Synchronized<std::queue<Event>>& events)
{
    setThreadName("LatencyCalculator");

    std::unordered_map<TaskId, TaskIntermediateStore> intermediateStore;

    while (!token.stop_requested())
    {
        if (events.rlock()->empty())
        {
            continue;
        }

        /// Using lambda invocation to gain the possibility of a return
        /// As we only have one thread removing items from the queue, we are sure that we will retrieve a new item here
        auto event = [&events]()
        {
            const auto lockedEvents = events.wlock();
            const auto newEvent = lockedEvents->front();
            lockedEvents->pop();
            return newEvent;
        }();

        std::visit(
        Overloaded{
            [&](const TaskEmit& taskEmit)
            {
                intermediateStore.insert({taskEmit.taskId, TaskIntermediateStore{taskEmit.queryId, taskEmit.bytesInTupleBuffer, taskEmit.numberOfTuples, taskEmit.timestamp}});
            },
            [&](const TaskExecutionComplete&)
            {

            },
            [](auto) {}},
        event);
    }
}

}


void LatencyListener::onEvent(Event event)
{
    std::visit(
        Overloaded{
            [&](const TaskEmit& taskEmit) { events.wlock()->emplace(taskEmit); },
            [&](const TaskExecutionComplete& taskExecutionCompleted) { events.wlock()->emplace(taskExecutionCompleted); },
            [](auto) {}},
        event);
}

void LatencyListener::onNodeShutdown()
{
    /// We wait until the queue is empty or for 30 seconds
    const std::chrono::seconds timeout{30};
    const auto endTime = std::chrono::high_resolution_clock::now() + timeout;
    while (true)
    {
        /// Check if the queue is empty
        if (events.rlock()->empty())
        {
            return;
        }

        /// Check if the timeout has been reached
        if (std::chrono::high_resolution_clock::now() >= endTime)
        {
            std::cout << fmt::format(
                "Queue in ThroughputListener still contains {} elements but could not finish in {}.", events.rlock()->size(), timeout)
                      << std::endl;
            NES_WARNING(
                "Queue in ThroughputListener still contains {} elements but could not finish in {}.", events.rlock()->size(), timeout);
        }
    }
}


LatencyListener::LatencyListener() : calculateThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken, events); })
{
}

}
