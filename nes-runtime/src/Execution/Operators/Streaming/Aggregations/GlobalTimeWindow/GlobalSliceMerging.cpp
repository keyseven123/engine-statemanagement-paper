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
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Operators {

void* createGlobalState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<GlobalSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

void* getGlobalSliceState(void* gs) {
    auto globalSlice = static_cast<GlobalSlice*>(gs);
    return globalSlice->getState()->ptr;
}

void* erasePartition(void* op, uint64_t ts) {
    auto handler = static_cast<GlobalSliceMergingHandler*>(op);
    auto partition = handler->getSliceStaging().erasePartition(ts);
    // we give nautilus the ownership, thus deletePartition must be called.
    return partition.release();
}

uint64_t getSizeOfPartition(void* p) {
    auto partition = static_cast<GlobalSliceStaging::Partition*>(p);
    return partition->partialStates.size();
}

void* getPartitionState(void* p, uint64_t index) {
    auto partition = static_cast<GlobalSliceStaging::Partition*>(p);
    return partition->partialStates[index].get()->ptr;
}

void deletePartition(void* p) {
    auto partition = static_cast<GlobalSliceStaging::Partition*>(p);
    delete partition;
}

void setupSliceMergingHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<GlobalSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size);
}
void* getDefaultMergingState(void* ss) {
    auto handler = static_cast<GlobalSliceMergingHandler*>(ss);
    return handler->getDefaultState()->ptr;
}

GlobalSliceMerging::GlobalSliceMerging(uint64_t operatorHandlerIndex,
                                       const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                                       const std::string& startTsFieldName,
                                       const std::string& endTsFieldName,
                                       uint64_t resultOriginId)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions), startTsFieldName(startTsFieldName),
      endTsFieldName(endTsFieldName), resultOriginId(resultOriginId) {}

void GlobalSliceMerging::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> entrySize = (uint64_t) 0;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    Nautilus::FunctionCall("setupSliceMergingHandler",
                           setupSliceMergingHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           entrySize);
    auto defaultState = Nautilus::FunctionCall("getDefaultMergingState", getDefaultMergingState, globalOperatorHandler);
    for (auto& function : aggregationFunctions) {
        function->reset(defaultState);
        defaultState = defaultState + function->getSize();
    }
    this->child->setup(executionCtx);
}

void GlobalSliceMerging::open(ExecutionContext& ctx, RecordBuffer& buffer) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    this->child->open(ctx, buffer);
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    Value<> startSliceTs = getMember(sliceMergeTask, SliceMergeTask, startSlice).load<UInt64>();
    Value<> endSliceTs = getMember(sliceMergeTask, SliceMergeTask, endSlice).load<UInt64>();
    // 2. load the thread local slice store according to the worker id.
    auto globalSliceState = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask, endSliceTs);
    // emit global slice when we have a tumbling window.
    emitWindow(ctx, startSliceTs, endSliceTs, globalSliceState);
}

Value<MemRef> GlobalSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                           Value<MemRef>& sliceMergeTask,
                                                           Value<>& endSliceTs) const {
    auto globalSlice = Nautilus::FunctionCall("createGlobalState", createGlobalState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);
    auto partition = Nautilus::FunctionCall("erasePartition", erasePartition, globalOperatorHandler, endSliceTs.as<UInt64>());
    auto sizeOfPartitions = Nautilus::FunctionCall("getSizeOfPartition", getSizeOfPartition, partition);
    for (Value<UInt64> i = (uint64_t) 0; i < sizeOfPartitions; i = i + (uint64_t) 1) {
        auto partitionState = Nautilus::FunctionCall("getPartitionState", getPartitionState, partition, i);
        uint64_t stateOffset = 0;
        for (const auto& function : aggregationFunctions) {
            auto globalValuePtr = globalSliceState + stateOffset;
            auto partitionValuePtr = partitionState + stateOffset;
            function->combine(globalValuePtr.as<MemRef>(), partitionValuePtr.as<MemRef>());
            stateOffset = stateOffset + function->getSize();
        }
    }
    Nautilus::FunctionCall("deletePartition", deletePartition, partition);
    return globalSlice;
}

void GlobalSliceMerging::emitWindow(ExecutionContext& ctx,
                                    Value<>& windowStart,
                                    Value<>& windowEnd,
                                    Value<MemRef>& globalSlice) const {
    ctx.setWatermarkTs(windowEnd.as<UInt64>());
    ctx.setOrigin(resultOriginId);

    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);
    Record resultWindow;
    resultWindow.write(startTsFieldName, windowStart);
    resultWindow.write(endTsFieldName, windowEnd);
    uint64_t stateOffset = 0;
    for (const auto& function : aggregationFunctions) {
        auto valuePtr = globalSliceState + stateOffset;
        function->lower(valuePtr.as<MemRef>(), resultWindow);
        stateOffset = stateOffset + function->getSize();
    }
    child->execute(ctx, resultWindow);
}

}// namespace NES::Runtime::Execution::Operators