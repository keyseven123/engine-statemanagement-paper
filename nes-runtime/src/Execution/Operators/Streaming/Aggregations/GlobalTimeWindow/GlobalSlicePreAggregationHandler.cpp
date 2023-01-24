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

#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

GlobalSlicePreAggregationHandler::GlobalSlicePreAggregationHandler(uint64_t windowSize,
                                                                   uint64_t windowSlide,
                                                                   const std::vector<OriginId>& origins,
                                                                   std::weak_ptr<GlobalSliceStaging> weakSliceStagingPtr)
    : windowSize(windowSize), windowSlide(windowSlide), weakSliceStaging(weakSliceStagingPtr),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)) {}

GlobalThreadLocalSliceStore* GlobalSlicePreAggregationHandler::getThreadLocalSliceStore(uint64_t workerId) {
    if (threadLocalSliceStores.size() <= workerId) {
        //    throw WindowProcessingException("ThreadLocalSliceStore for " + std::to_string(workerId) + " is not initialized.");
    }
    return threadLocalSliceStores[workerId].get();
}

void GlobalSlicePreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize) {
    defaultState = std::make_unique<State>(entrySize);
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<GlobalThreadLocalSliceStore>(entrySize, windowSize, windowSlide, defaultState);
        threadLocalSliceStores.emplace_back(std::move(threadLocal));
    }
}

void GlobalSlicePreAggregationHandler::triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                                               Runtime::Execution::PipelineExecutionContext& ctx,
                                                               uint64_t workerId,
                                                               OriginId originId,
                                                               uint64_t sequenceNumber,
                                                               uint64_t watermarkTs) {

    auto* threadLocalSliceStore = getThreadLocalSliceStore(workerId);

    // the watermark update is an atomic process and returns the last and the current watermark.
    auto newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

    // check if the current max watermark is larger than the thread local watermark
    if (newGlobalWatermark > threadLocalSliceStore->getLastWatermark()) {
        for (auto& slice : threadLocalSliceStore->getSlices()) {
            if (slice->getEnd() > newGlobalWatermark) {
                break;
            }
            auto& sliceState = slice->getState();
            // each worker adds its local state to the staging area
            auto sliceStaging = this->weakSliceStaging.lock();
            if (!sliceStaging) {
                NES_FATAL_ERROR("SliceStaging is invalid, this should only happen after a hard stop. Drop all in flight data.");
                return;
            }
            auto [addedPartitionsToSlice, numberOfBuffers] = sliceStaging->addToSlice(slice->getEnd(), std::move(sliceState));
            if (addedPartitionsToSlice == threadLocalSliceStores.size()) {
                if (numberOfBuffers != 0) {
                    NES_DEBUG("Deploy merge task for slice " << slice->getEnd() << " with " << numberOfBuffers << " buffers.");
                    auto buffer = wctx.allocateTupleBuffer();
                    auto task = buffer.getBuffer<SliceMergeTask>();
                    task->startSlice = slice->getStart();
                    task->endSlice = slice->getEnd();
                    buffer.setNumberOfTuples(1);
                    ctx.dispatchBuffer(buffer);
                } else {
                    NES_DEBUG("Slice " << slice->getEnd() << " is empty. Don't deploy merge task.");
                }
            }
        }
        threadLocalSliceStore->removeSlicesUntilTs(newGlobalWatermark);
        threadLocalSliceStore->setLastWatermark(newGlobalWatermark);
    }
}

void GlobalSlicePreAggregationHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                             Runtime::StateManagerPtr,
                                             uint32_t) {
    NES_DEBUG("start GlobalSlicePreAggregationHandler");
}

void GlobalSlicePreAggregationHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                            Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) {
    NES_DEBUG("shutdown GlobalSlicePreAggregationHandler: " << queryTerminationType);

    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        auto sliceStaging = this->weakSliceStaging.lock();
        NES_ASSERT(sliceStaging, "SliceStaging is invalid, this should only happen after a soft stop.");
        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            for (auto& slice : threadLocalSliceStore->getSlices()) {
                auto& sliceState = slice->getState();
                // each worker adds its local state to the staging area
                auto [addedPartitionsToSlice, numberOfBuffers] = sliceStaging->addToSlice(slice->getEnd(), std::move(sliceState));
                if (addedPartitionsToSlice == threadLocalSliceStores.size()) {
                    if (numberOfBuffers != 0) {
                        NES_DEBUG("Deploy merge task for slice (" << slice->getStart() << "-" << slice->getEnd() << ") with "
                                                                  << numberOfBuffers << " buffers.");
                        auto buffer = pipelineExecutionContext->getBufferManager()->getBufferBlocking();
                        auto task = buffer.getBuffer<SliceMergeTask>();
                        task->startSlice = slice->getStart();
                        task->endSlice = slice->getEnd();
                        buffer.setNumberOfTuples(1);
                        pipelineExecutionContext->dispatchBuffer(buffer);
                    } else {
                        NES_DEBUG("Slice " << slice->getEnd() << " is empty. Don't deploy merge task.");
                    }
                }
            }
        }
    }
}
GlobalSlicePreAggregationHandler::~GlobalSlicePreAggregationHandler() { NES_DEBUG("~GlobalSlicePreAggregationHandler"); }

void GlobalSlicePreAggregationHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    // this->threadLocalSliceStores.clear();
}
const State* GlobalSlicePreAggregationHandler::getDefaultState() const { return defaultState.get(); }

}// namespace NES::Runtime::Execution::Operators