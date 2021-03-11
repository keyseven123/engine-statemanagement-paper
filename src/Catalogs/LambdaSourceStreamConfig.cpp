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

#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>

namespace NES {

namespace detail {}// namespace detail

LambdaSourceStreamConfig::LambdaSourceStreamConfig(
    std::string sourceType, std::string physicalStreamName, std::string logicalStreamName,
    std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    uint64_t numBuffersToProcess, uint64_t frequency)
    : PhysicalStreamConfig(SourceConfig::create()), sourceType(sourceType), generationFunction(std::move(generationFunction)) {
    // nop
    this->physicalStreamName = physicalStreamName;
    this->logicalStreamName = logicalStreamName;
    this->numberOfBuffersToProduce = numBuffersToProcess;
    this->sourceFrequency = std::chrono::milliseconds(frequency);
}

const std::string LambdaSourceStreamConfig::getSourceType() { return sourceType; }

const std::string LambdaSourceStreamConfig::toString() { return sourceType; }

const std::string LambdaSourceStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

const std::string LambdaSourceStreamConfig::getLogicalStreamName() { return logicalStreamName; }

SourceDescriptorPtr LambdaSourceStreamConfig::build(SchemaPtr schema) {
    return std::make_shared<LambdaSourceDescriptor>(schema, std::move(generationFunction), this->numberOfBuffersToProduce,
                                                    this->sourceFrequency);
}

AbstractPhysicalStreamConfigPtr LambdaSourceStreamConfig::create(
    std::string sourceType, std::string physicalStreamName, std::string logicalStreamName,
    std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    uint64_t numBuffersToProcess, uint64_t frequency) {
    return std::make_shared<LambdaSourceStreamConfig>(sourceType, physicalStreamName, logicalStreamName,
                                                      std::move(generationFunction), numBuffersToProcess, frequency);
}

}// namespace NES