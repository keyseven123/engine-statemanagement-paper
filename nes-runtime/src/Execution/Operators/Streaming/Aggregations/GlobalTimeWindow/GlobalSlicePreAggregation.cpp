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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getSliceStoreProxy(void* op, uint64_t workerId) {
    auto handler = static_cast<GlobalSlicePreAggregationHandler*>(op);
    return handler->getThreadLocalSliceStore(workerId);
}

void* findSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<GlobalThreadLocalSliceStore*>(ss);
    return sliceStore->findSliceByTs(ts)->getState()->ptr;
}

void triggerThreadLocalStateProxy(void* op,
                                  void* wctx,
                                  void* pctx,
                                  uint64_t workerId,
                                  uint64_t originId,
                                  uint64_t sequenceNumber,
                                  uint64_t watermarkTs) {
    auto handler = static_cast<GlobalSlicePreAggregationHandler*>(op);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->triggerThreadLocalState(*workerContext, *pipelineExecutionContext, workerId, originId, sequenceNumber, watermarkTs);
}

void setupWindowHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<GlobalSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size);
}
void* getDefaultState(void* ss) {
    auto handler = static_cast<GlobalSlicePreAggregationHandler*>(ss);
    return handler->getDefaultState()->ptr;
}

class LocalGlobalPreAggregationState : public Operators::OperatorState {
  public:
    explicit LocalGlobalPreAggregationState(const Value<MemRef>& sliceStoreState) : sliceStoreState(sliceStoreState){};
    const Value<MemRef> sliceStoreState;
};

GlobalSlicePreAggregation::GlobalSlicePreAggregation(
    uint64_t operatorHandlerIndex,
    TimeFunctionPtr timeFunction,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions)
    : operatorHandlerIndex(operatorHandlerIndex), timeFunction(std::move(timeFunction)),
      aggregationFunctions(aggregationFunctions) {}

void GlobalSlicePreAggregation::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> entrySize = (uint64_t) 0;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    Nautilus::FunctionCall("setupWindowHandler",
                           setupWindowHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           entrySize);
    auto defaultState = Nautilus::FunctionCall("getDefaultState", getDefaultState, globalOperatorHandler);
    for (const auto& function : aggregationFunctions) {
        function->reset(defaultState);
        defaultState = defaultState + function->getSize();
    }
}

void GlobalSlicePreAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto sliceStore = Nautilus::FunctionCall("getSliceStoreProxy", getSliceStoreProxy, globalOperatorHandler, ctx.getWorkerId());
    // 3. store the reference to the slice store in the local operator state.
    auto sliceStoreState = std::make_unique<LocalGlobalPreAggregationState>(sliceStore);
    ctx.setLocalOperatorState(this, std::move(sliceStoreState));
    // 4. initialize timestamp function
    timeFunction->open(ctx, rb);
}

void GlobalSlicePreAggregation::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // For each input record, we derive its timestamp, we derive the correct slice from the slice store, and we manipulate the thread local aggregate.
    // 1. derive the current ts for the record.
    auto timestampValue = timeFunction->getTs(ctx, record);
    // 2. load the reference to the slice store and find the correct slice.
    auto sliceStore = static_cast<LocalGlobalPreAggregationState*>(ctx.getLocalState(this));
    auto sliceState =
        Nautilus::FunctionCall("findSliceStateByTsProxy", findSliceStateByTsProxy, sliceStore->sliceStoreState, timestampValue);
    // 3. manipulate the current aggregate values
    uint64_t stateOffset = 0;
    for (const auto& aggregationFunction : aggregationFunctions) {
        auto state = sliceState + stateOffset;
        stateOffset = stateOffset + aggregationFunction->getSize();
        aggregationFunction->lift(state.as<MemRef>(), record);
    }
}
void GlobalSlicePreAggregation::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);

    // After we processed all records in the record buffer we call triggerThreadLocalStateProxy
    // with the current watermark ts to check if we can trigger a window.
    Nautilus::FunctionCall("triggerThreadLocalStateProxy",
                           triggerThreadLocalStateProxy,
                           globalOperatorHandler,
                           executionCtx.getWorkerContext(),
                           executionCtx.getPipelineContext(),
                           executionCtx.getWorkerId(),
                           executionCtx.getOriginId(),
                           recordBuffer.getSequenceNr(),
                           executionCtx.getWatermarkTs());
}

}// namespace NES::Runtime::Execution::Operators