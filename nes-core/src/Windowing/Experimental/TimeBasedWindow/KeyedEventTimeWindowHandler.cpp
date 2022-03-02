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

#include <Runtime/WorkerContext.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
namespace NES::Windowing::Experimental {

KeyedEventTimeWindowHandler::KeyedEventTimeWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition)
    : windowDefinition(windowDefinition) {
    watermarkProcessor =
        NES::Experimental::LockFreeMultiOriginWatermarkProcessor::create(windowDefinition->getNumberOfInputEdges());

    if (windowDefinition->getWindowType()->isTumblingWindow()) {
        auto tumblingWindow = dynamic_pointer_cast<Windowing::TumblingWindow>(windowDefinition->getWindowType());
        // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
        sliceSize = tumblingWindow->getSize().getTime();
    } else {
        auto slidingWindow = dynamic_pointer_cast<Windowing::SlidingWindow>(windowDefinition->getWindowType());
        // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
        sliceSize = slidingWindow->getSlide().getTime();
    }
}

void KeyedEventTimeWindowHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                        NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        threadLocalSliceStores.emplace_back(hashmapFactory, sliceSize, 100);
    }
    this->factory = hashmapFactory;
}

NES::Experimental::Hashmap KeyedEventTimeWindowHandler::getHashMap() { return factory->create(); }

void KeyedEventTimeWindowHandler::triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                                          Runtime::Execution::PipelineExecutionContext& ctx,
                                                          uint64_t workerId,
                                                          uint64_t originId,
                                                          uint64_t sequenceNumber,
                                                          uint64_t watermarkTs) {

    auto& threadLocalSliceStore = getThreadLocalSliceStore(workerId);

    // the watermark update is an atomic process and returns the last and the current watermark.
    auto newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);
    // check if the current max watermark is larger than the thread local watermark
    if (newGlobalWatermark > threadLocalSliceStore.getLastWatermark()) {

        if (threadLocalSliceStore.getLastWatermark() == 0) {
            // special case for the first watermark handling
            auto currentSliceIndex = newGlobalWatermark / sliceSize;
            if (currentSliceIndex > 0) {
                threadLocalSliceStore.setFirstSliceIndex(currentSliceIndex - 1);
            }
        }
        // push the local slices to the global slice store.
        auto firstIndex = threadLocalSliceStore.getFirstIndex();
        auto lastIndex = threadLocalSliceStore.getLastIndex();
        for (uint64_t si = firstIndex; si <= lastIndex; si++) {

            const auto& slice = threadLocalSliceStore[si];
            if (slice->getEnd() > newGlobalWatermark) {
                break;
            }

            // put partitions to global slice store
            auto& sliceState = slice->getState();
            // each worker adds its local state to the staging area
            auto [addedPartitionsToSlice, numberOfBuffers] =
                sliceStaging.addToSlice(slice->getIndex(), sliceState.extractEntries());
            if (addedPartitionsToSlice == threadLocalSliceStores.size()) {
                if (numberOfBuffers != 0) {
                    NES_DEBUG("Deploy merge task for slice " << slice->getIndex() << " with " << numberOfBuffers << " buffers.");
                    auto buffer = wctx.allocateTupleBuffer();
                    auto task = buffer.getBuffer<SliceMergeTask>();
                    task->sliceIndex = slice->getIndex();
                    buffer.setNumberOfTuples(1);
                    ctx.dispatchBuffer(buffer);
                } else {
                    NES_DEBUG("Slice " << slice->getIndex() << " is empty. Don't deploy merge task.");
                }
            }

            // erase slice from thread local slice store
            threadLocalSliceStore.dropFirstSlice();
        }
        threadLocalSliceStore.setLastWatermark(newGlobalWatermark);
    }
}

void KeyedEventTimeWindowHandler::triggerSliceMerging(Runtime::WorkerContext& wctx,
                                                      Runtime::Execution::PipelineExecutionContext& ctx,
                                                      uint64_t sequenceNumber,
                                                      KeyedSlicePtr slice) {
    // add pre-aggregated slice to slice store
    auto [oldMaxSliceIndex, newMaxSliceIndex] = globalSliceStore.addSlice(sequenceNumber, slice->getIndex(), std::move(slice));
    // check if we can trigger window computation
    // ignore first sequence number
    if (newMaxSliceIndex > oldMaxSliceIndex) {
        auto buffer = wctx.allocateTupleBuffer();
        auto task = buffer.getBuffer<WindowTriggerTask>();
        // we trigger the compleation of all windows that end between startSlice and <= endSlice.
        NES_DEBUG("Deploy window trigger task for slice  ( " << oldMaxSliceIndex << "-" << newMaxSliceIndex << ")");
        task->startSlice = oldMaxSliceIndex;
        task->endSlice = newMaxSliceIndex;
        task->sequenceNumber = newMaxSliceIndex;
        buffer.setNumberOfTuples(1);
        ctx.dispatchBuffer(buffer);
    }
}

Windowing::LogicalWindowDefinitionPtr KeyedEventTimeWindowHandler::getWindowDefinition() { return windowDefinition; }

void KeyedEventTimeWindowHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start KeyedEventTimeWindowHandler");
    activeCounter++;
}

void KeyedEventTimeWindowHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedEventTimeWindowHandler");
    activeCounter--;
//    bool current = true;
    if (activeCounter == 0) {
        NES_DEBUG("shutdown KeyedEventTimeWindowHandler");
        // todo fix shutdown, currently KeyedEventTimeWindowHandler is never destructed because operators have references.
        this->threadLocalSliceStores.clear();
        this->globalSliceStore.clear();
        this->sliceStaging.clear();
    }
}

KeyedSlicePtr KeyedEventTimeWindowHandler::createKeyedSlice(uint64_t sliceIndex) {
    auto startTs = sliceIndex * sliceSize;
    auto endTs = (sliceIndex + 1) * sliceSize;
    return std::make_unique<KeyedSlice>(factory, startTs, endTs, sliceIndex);
};

}// namespace NES::Windowing::Experimental