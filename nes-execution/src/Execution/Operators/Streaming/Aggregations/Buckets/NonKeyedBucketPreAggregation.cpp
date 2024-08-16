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
#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketPreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

//void* getBucketStore(void* op, WorkerThreadId workerThreadId) {
void* getBucketStore(void* op, uint32_t workerThreadId) {
    auto handler = static_cast<NonKeyedBucketPreAggregationHandler*>(op);
    return handler->getThreadLocalBucketStore(WorkerThreadId(workerThreadId));
}

void* findBucketsByTs(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NonKeyedBucketStore*>(ss);
    return sliceStore->findBucketsByTs(ts);
}

uint64_t getBucketListSize(void* ptr) {
    auto buckets = static_cast<std::vector<NonKeyedSlice*>*>(ptr);
    return buckets->size();
}

int8_t* getBucket(void* ptr, uint64_t index) {
    auto buckets = static_cast<std::vector<NonKeyedSlice*>*>(ptr);
    return buckets->operator[](index)->getState()->ptr;
}

void triggerBucketsProxy(void* op,
                         void* wctx,
                         void* pctx,
                         uint64_t originId,
                         uint64_t sequenceNumber,
                         uint64_t chunkNumber,
                         bool lastChunk,
                         uint64_t watermarkTs) {
    auto handler = static_cast<NonKeyedBucketPreAggregationHandler*>(op);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->trigger(*workerContext,
                     *pipelineExecutionContext,
                     OriginId(originId),
                     {sequenceNumber, chunkNumber, lastChunk},
                     watermarkTs);
}

void setupBucketWindowHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<NonKeyedBucketPreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size);
}
int8_t* getDefaultBucketState(void* ss) {
    auto handler = static_cast<NonKeyedBucketPreAggregationHandler*>(ss);
    return handler->getDefaultState()->ptr;
}

class LocalBucketPreAggregationState : public Operators::OperatorState {
  public:
    explicit LocalBucketPreAggregationState(const MemRefVal& bucketStore) : bucketStore(bucketStore){};
    const MemRefVal bucketStore;
};

NonKeyedBucketPreAggregation::NonKeyedBucketPreAggregation(
    uint64_t operatorHandlerIndex,
    TimeFunctionPtr timeFunction,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions)
    : operatorHandlerIndex(operatorHandlerIndex), timeFunction(std::move(timeFunction)),
      aggregationFunctions(aggregationFunctions) {}

void NonKeyedBucketPreAggregation::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    UInt64Val entrySize = 0_u64;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    nautilus::invoke(setupBucketWindowHandler, globalOperatorHandler, executionCtx.getPipelineContext(), entrySize);
    auto defaultState = nautilus::invoke(getDefaultBucketState, globalOperatorHandler);
    for (const auto& function : aggregationFunctions) {
        function->reset(defaultState);
        defaultState = defaultState + nautilus::val<uint64_t>(function->getSize());
    }
}

void NonKeyedBucketPreAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto bucketStore = nautilus::invoke(getBucketStore, globalOperatorHandler, ctx.getWorkerThreadId());
    // 3. store the reference to the slice store in the local operator state.
    auto localState = std::make_unique<LocalBucketPreAggregationState>(bucketStore);
    ctx.setLocalOperatorState(this, std::move(localState));
    // 4. initialize timestamp function
    timeFunction->open(ctx, rb);
}

void NonKeyedBucketPreAggregation::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // For each input record, we derive its timestamp, we derive the correct slice from the slice store, and we manipulate the thread local aggregate.
    // 1. derive the current ts for the record.
    auto timestampValue = timeFunction->getTs(ctx, record);
    // 2. load the reference to the slice store and find the correct slice.
    auto localState = static_cast<LocalBucketPreAggregationState*>(ctx.getLocalState(this));
    auto buckets = nautilus::invoke(findBucketsByTs, localState->bucketStore, timestampValue->as<Nautilus::ExecDataUInt64>()->valueAsType<uint64_t>());
    // 3. manipulate the current aggregate values in each bucket
    auto numberOfBuckets = nautilus::invoke(getBucketListSize, buckets);
    for (UInt64Val i = 0_u64; i < numberOfBuckets; i = i + 1_u64) {
        auto bucketState = nautilus::invoke(getBucket, buckets, i);
        UInt64Val stateOffset = 0;
        for (const auto& aggregationFunction : aggregationFunctions) {
            auto state = bucketState + stateOffset;
            stateOffset = stateOffset + aggregationFunction->getSize();
            aggregationFunction->lift(state, record);
        }
    }
}
void NonKeyedBucketPreAggregation::close(ExecutionContext& ctx, RecordBuffer&) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // After we processed all records in the record buffer we call triggerThreadLocalStateProxy
    // with the current watermark ts to check if we can trigger a window.
    nautilus::invoke(triggerBucketsProxy,
                     globalOperatorHandler,
                     ctx.getWorkerContext(),
                     ctx.getPipelineContext(),
                     ctx.getOriginId(),
                     ctx.getSequenceNumber(),
                     ctx.getChunkNumber(),
                     ctx.getLastChunk(),
                     ctx.getWatermarkTs()->as<Nautilus::ExecDataUInt64>()->valueAsType<uint64_t>());
}

}// namespace NES::Runtime::Execution::Operators
