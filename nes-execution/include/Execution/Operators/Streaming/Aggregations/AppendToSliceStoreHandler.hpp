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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPENDTOSLICESTOREHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPENDTOSLICESTOREHANDLER_HPP_
#include <Execution/Operators/Streaming/Aggregations/SlidingWindowSliceStore.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <memory>
namespace NES::Runtime::Execution::Operators {
class MultiOriginWatermarkProcessor;

/**
 * @brief This is the operator handler for the AppendToSliceStoreAction.
 * It maintains the SlidingWindowSliceStore<Slice> that stores all slices.
 * @tparam Slice
 */
template<class Slice>
class AppendToSliceStoreHandler : public OperatorHandler {
  public:
    AppendToSliceStoreHandler(uint64_t windowSize, uint64_t windowSlide);
    void start(PipelineExecutionContextPtr, uint32_t) override {}
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;
    void appendToGlobalSliceStore(std::unique_ptr<Slice> slice);
    void triggerSlidingWindows(Runtime::WorkerContext& wctx,
                               Runtime::Execution::PipelineExecutionContext& ctx,
                               SequenceData sequenceNumber,
                               uint64_t slideEnd);

  private:
    std::unique_ptr<SlidingWindowSliceStore<Slice>> sliceStore;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic<uint64_t> lastTriggerWatermark = 0;
    std::atomic<uint64_t> resultSequenceNumber = 1;
    std::mutex triggerMutex;
};

class NonKeyedSlice;
using NonKeyedAppendToSliceStoreHandler = AppendToSliceStoreHandler<NonKeyedSlice>;
class KeyedSlice;
using KeyedAppendToSliceStoreHandler = AppendToSliceStoreHandler<KeyedSlice>;
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPENDTOSLICESTOREHANDLER_HPP_
