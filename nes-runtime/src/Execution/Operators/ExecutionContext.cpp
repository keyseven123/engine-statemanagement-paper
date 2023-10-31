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
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution {

ExecutionContext::ExecutionContext(const Value<NES::Nautilus::MemRef>& workerContext,
                                   const Value<NES::Nautilus::MemRef>& pipelineContext)
    : workerContext(workerContext), pipelineContext(pipelineContext), origin(0_u64), watermarkTs(0_u64), currentTs(0_u64),
      sequenceNumber(0_u64) {}

void* allocateBufferProxy(void* workerContextPtr) {
    if (workerContextPtr == nullptr) {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    auto wkrCtx = static_cast<Runtime::WorkerContext*>(workerContextPtr);
    // We allocate a new tuple buffer for the runtime.
    // As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    // This increases the reference counter in the buffer.
    // When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    auto buffer = wkrCtx->allocateTupleBuffer();
    auto* tb = new Runtime::TupleBuffer(buffer);
    return tb;
}

Value<MemRef> ExecutionContext::allocateBuffer() {
    auto bufferPtr = Nautilus::FunctionCall("allocateBufferProxy", allocateBufferProxy, workerContext);
    return bufferPtr;
}

void emitBufferProxy(void* wc, void* pc, void* tupleBuffer) {
#ifndef UNIKERNEL_EXPORT
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(pc);
    auto workerCtx = static_cast<WorkerContext*>(wc);
    // check if buffer has values
    if (tb->getNumberOfTuples() != 0) {
        pipelineCtx->emitBuffer(*tb, *workerCtx);
    }
    // delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tb;
#else
    NES_THROW_RUNTIME_ERROR("Not Implemented");
#endif
}

void ExecutionContext::emitBuffer(const NES::Runtime::Execution::RecordBuffer& buffer) {
    FunctionCall<>("emitBufferProxy", emitBufferProxy, workerContext, pipelineContext, buffer.getReference());
}

uint64_t getWorkerIdProxy(void* workerContext) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    return wc->getId();
}

Value<UInt64> ExecutionContext::getWorkerId() { return FunctionCall("getWorkerIdProxy", getWorkerIdProxy, workerContext); }

Operators::OperatorState* ExecutionContext::getLocalState(const Operators::Operator* op) {
    auto stateEntry = localStateMap.find(op);
    if (stateEntry == localStateMap.end()) {
        NES_THROW_RUNTIME_ERROR("No local state registered for operator: " << op);
    }
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state) {
    if (localStateMap.contains(op)) {
        NES_THROW_RUNTIME_ERROR("Operators state already registered for operator: " << op);
    }
    localStateMap.emplace(op, std::move(state));
}

void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index) {
#ifndef UNIKERNEL_EXPORT
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(pc);
    auto handlers = pipelineCtx->getOperatorHandlers();
    auto size = handlers.size();
    if (index >= size) {
        NES_THROW_RUNTIME_ERROR("operator handler at index " + std::to_string(index) + " is not registered");
    }
    return handlers[index].get();
#else
    NES_THROW_RUNTIME_ERROR("Not Implemented");
#endif
}

Value<MemRef> ExecutionContext::getGlobalOperatorHandler(uint64_t handlerIndex) {
    Value<UInt64> handlerIndexValue = Value<UInt64>(handlerIndex);
    return FunctionCall<>("getGlobalOperatorHandlerProxy", getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
}

const Value<MemRef>& ExecutionContext::getWorkerContext() const { return workerContext; }
const Value<MemRef>& ExecutionContext::getPipelineContext() const { return pipelineContext; }

const Value<UInt64>& ExecutionContext::getWatermarkTs() const { return watermarkTs; }

void ExecutionContext::setWatermarkTs(Value<UInt64> watermarkTs) { this->watermarkTs = watermarkTs; }

const Value<UInt64>& ExecutionContext::getSequenceNumber() const { return sequenceNumber; }

void ExecutionContext::setSequenceNumber(Value<UInt64> sequenceNumber) { this->sequenceNumber = sequenceNumber; }

const Value<UInt64>& ExecutionContext::getOriginId() const { return origin; }

void ExecutionContext::setOrigin(Value<UInt64> origin) { this->origin = origin; }

const Value<UInt64>& ExecutionContext::getCurrentTs() const { return currentTs; }

void ExecutionContext::setCurrentTs(Value<UInt64> currentTs) { this->currentTs = currentTs; }

}// namespace NES::Runtime::Execution