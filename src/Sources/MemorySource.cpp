/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/MemorySource.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <cmath>
namespace NES {

MemorySource::MemorySource(SchemaPtr schema,
                           std::shared_ptr<uint8_t> memoryArea,
                           size_t memoryAreaSize,
                           NodeEngine::BufferManagerPtr bufferManager,
                           NodeEngine::QueryManagerPtr queryManager,
                           uint64_t numBuffersToProcess,
                           uint64_t gatheringValue,
                           OperatorId operatorId,
                           size_t numSourceLocalBuffers,
                           GatheringMode gatheringMode,
                           std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numBuffersToProcess,
                      operatorId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      successors),
      memoryArea(memoryArea), memoryAreaSize(memoryAreaSize), currentPositionInBytes(0) {
    this->numBuffersToProcess = numBuffersToProcess;
    if (gatheringMode == GatheringMode::FREQUENCY_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << gatheringMode);
    }

    //if the memory area is smaller than a buffer
    if (memoryAreaSize <= globalBufferManager->getBufferSize()) {
        numberOfTuplesToProduce = std::floor(double(memoryAreaSize) / double(this->schema->getSchemaSizeInBytes()));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / this->schema->getSchemaSizeInBytes();
        auto numberOfTuplesPerBuffer =
            std::floor(double(globalBufferManager->getBufferSize()) / double(this->schema->getSchemaSizeInBytes()));
        if (restTuples > numberOfTuplesPerBuffer) {
            numberOfTuplesToProduce = numberOfTuplesPerBuffer;
        } else {
            numberOfTuplesToProduce = restTuples;
        }
    }

    NES_DEBUG("MemorySource() numBuffersToProcess=" << numBuffersToProcess << " memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

std::optional<NodeEngine::TupleBuffer> MemorySource::receiveData() {
    NES_DEBUG("MemorySource::receiveData called on operatorId=" << operatorId);
    auto buffer = this->bufferManager->getBufferTimeout(NES::NodeEngine::DEFAULT_BUFFER_TIMEOUT);

    if (memoryAreaSize > buffer->getBufferSize()) {
        if (currentPositionInBytes + numberOfTuplesToProduce * schema->getSchemaSizeInBytes() > memoryAreaSize) {
            if (numBuffersToProcess != 0) {
                NES_DEBUG("MemorySource::receiveData: reset buffer to 0");
                currentPositionInBytes = 0;
            } else {
                NES_DEBUG("MemorySource::receiveData: return as mem sry is empty");
                return std::nullopt;
            }
        }
    }

    NES_ASSERT2_FMT(numberOfTuplesToProduce * schema->getSchemaSizeInBytes() <= buffer->getBufferSize(),
                    "value to write is larger than the buffer");

    if (!buffer) {
        NES_ERROR("Buffer invalid after waiting on timeout");
        return std::nullopt;
    }
    memcpy(buffer->getBuffer(), memoryArea.get() + currentPositionInBytes, buffer->getBufferSize());

    //        TODO: replace copy with inplace add like with the wraparound #1853
    //    auto buffer2 = NodeEngine::TupleBuffer::wrapMemory(memoryArea.get() + currentPositionInBytes, buffer->getBufferSize(), this);

    if (memoryAreaSize > buffer->getBufferSize()) {
        NES_DEBUG("MemorySource::receiveData: add offset=" << buffer->getBufferSize()
                                                           << " to currentpos=" << currentPositionInBytes);
        currentPositionInBytes += buffer->getBufferSize();
    }

    buffer->setNumberOfTuples(numberOfTuplesToProduce);

    generatedTuples += buffer->getNumberOfTuples();
    generatedBuffers++;

    NES_DEBUG("MemorySource::receiveData filled buffer with tuples=" << buffer->getNumberOfTuples());
    if (buffer->getNumberOfTuples() == 0) {
        return std::nullopt;
    } else {
        return buffer;
    }
}

const std::string MemorySource::toString() const { return "MemorySource"; }

NES::SourceType MemorySource::getType() const { return MEMORY_SOURCE; }
}// namespace NES