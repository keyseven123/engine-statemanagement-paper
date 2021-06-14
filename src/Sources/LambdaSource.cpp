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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/LambdaSource.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <utility>
namespace NES {

LambdaSource::LambdaSource(
    SchemaPtr schema,
    NodeEngine::BufferManagerPtr bufferManager,
    NodeEngine::QueryManagerPtr queryManager,
    uint64_t numbersOfBufferToProduce,
    uint64_t gatheringValue,
    std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    OperatorId operatorId,
    size_t numSourceLocalBuffers,
    GatheringMode gatheringMode,
    std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numbersOfBufferToProduce,
                      operatorId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      successors),
      generationFunction(std::move(generationFunction)) {
    NES_DEBUG("Create LambdaSource with id=" << operatorId << "func is " << (generationFunction ? "callable" : "not callable"));
    if (this->gatheringMode == GatheringMode::FREQUENCY_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (this->gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << gatheringMode);
    }
    numberOfTuplesToProduce = this->globalBufferManager->getBufferSize() / this->schema->getSchemaSizeInBytes();
    wasGracefullyStopped = false;
}

std::optional<NodeEngine::TupleBuffer> LambdaSource::receiveData() {
    NES_DEBUG("LambdaSource::receiveData called on operatorId=" << operatorId);
    using namespace std::chrono_literals;

    auto buffer = this->globalBufferManager->getBufferTimeout(NES::NodeEngine::DEFAULT_BUFFER_TIMEOUT);
    if (!buffer) {
        NES_ERROR("Buffer invalid after waiting on timeout");
        return std::nullopt;
    }

    NES_ASSERT2_FMT(numberOfTuplesToProduce * schema->getSchemaSizeInBytes() <= buffer->getBufferSize(),
                    "value to write is larger than the buffer");

    generationFunction(buffer.value(), numberOfTuplesToProduce);

    buffer->setNumberOfTuples(numberOfTuplesToProduce);
    generatedTuples += buffer->getNumberOfTuples();
    generatedBuffers++;

    NES_DEBUG("LambdaSource::receiveData filled buffer with tuples=" << buffer->getNumberOfTuples()
                                                                     << " outOrgID=" << buffer->getOriginId());
    //    NES_DEBUG("bufferContent before write=" << UtilityFunctions::prettyPrintTupleBuffer(buffer.value(), schema) << '\n');

    if (buffer->getNumberOfTuples() == 0) {
        NES_ASSERT(false, "this should not happen");
        return std::nullopt;
    } else {
        return buffer;
    }
}

const std::string LambdaSource::toString() const { return "LambdaSource"; }

bool LambdaSource::stop(bool) { return this->DataSource::stop(false); }

SourceType LambdaSource::getType() const { return LAMBDA_SOURCE; }
}// namespace NES
