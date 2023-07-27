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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

NonKeyedSlidingWindowSinkOperatorHandler::NonKeyedSlidingWindowSinkOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::shared_ptr<GlobalSliceStore<NonKeyedSlice>>& globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {}

void NonKeyedSlidingWindowSinkOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t entrySize) {
    this->entrySize = entrySize;
}

void NonKeyedSlidingWindowSinkOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                     Runtime::StateManagerPtr,
                                                     uint32_t) {
    NES_DEBUG("start NonKeyedSlidingWindowSinkOperatorHandler");
}

void NonKeyedSlidingWindowSinkOperatorHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                                    Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop NonKeyedSlidingWindowSinkOperatorHandler: {}", queryTerminationType);
}

NonKeyedSlicePtr NonKeyedSlidingWindowSinkOperatorHandler::createGlobalSlice(WindowTriggerTask* windowTriggerTask) {
    return std::make_unique<NonKeyedSlice>(entrySize, windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}

std::vector<NonKeyedSliceSharedPtr>
NonKeyedSlidingWindowSinkOperatorHandler::getSlicesForWindow(WindowTriggerTask* windowTriggerTask) {
    NES_DEBUG("getSlicesForWindow {} - {}", windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
    return globalSliceStore->getSlicesForWindow(windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}

Windowing::LogicalWindowDefinitionPtr NonKeyedSlidingWindowSinkOperatorHandler::getWindowDefinition() { return windowDefinition; }

GlobalSliceStore<NonKeyedSlice>& NonKeyedSlidingWindowSinkOperatorHandler::getGlobalSliceStore() { return *globalSliceStore; }

NonKeyedSlidingWindowSinkOperatorHandler::~NonKeyedSlidingWindowSinkOperatorHandler() {
    NES_DEBUG("Destruct NonKeyedSlidingWindowSinkOperatorHandler");
}

void NonKeyedSlidingWindowSinkOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    globalSliceStore.reset();
}

}// namespace NES::Windowing::Experimental