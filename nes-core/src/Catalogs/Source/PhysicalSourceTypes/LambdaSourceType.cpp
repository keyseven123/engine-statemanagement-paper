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

#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
#include <utility>

namespace NES {

LambdaSourceType::LambdaSourceType(std::function<void(NES::Runtime::TupleBuffer&, uint64_t)>&& generationFunction,
                                   uint64_t numBuffersToProduce,
                                   uint64_t gatheringValue,
                                   GatheringMode gatheringMode,
                                   uint64_t sourceAffinity,
                                   uint64_t taskQueueId,
                                   uint64_t numberOfQueues)
    : PhysicalSourceType(SourceType::LAMBDA_SOURCE), generationFunction(std::move(generationFunction)),
      numBuffersToProduce(numBuffersToProduce), gatheringValue(gatheringValue), gatheringMode(std::move(gatheringMode)),
      sourceAffinity(sourceAffinity), taskQueueId(taskQueueId), numberOfQueues(numberOfQueues) {}

LambdaSourceTypePtr LambdaSourceType::create(
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    uint64_t numBuffersToProcess,
    uint64_t gatheringValue,
    GatheringMode gatheringMode,
    uint64_t sourceAffinity,
    uint64_t taskQueueId,
    uint64_t numberOfQueues) {
    return std::make_shared<LambdaSourceType>(LambdaSourceType(std::move(generationFunction),
                                                               numBuffersToProcess,
                                                               gatheringValue,
                                                               gatheringMode,
                                                               sourceAffinity,
                                                               taskQueueId,
                                                               numberOfQueues));
}

std::function<void(NES::Runtime::TupleBuffer&, uint64_t)> LambdaSourceType::getGenerationFunction() const {
    return generationFunction;
}

uint64_t LambdaSourceType::getNumBuffersToProduce() const { return numBuffersToProduce; }

uint64_t LambdaSourceType::getGatheringValue() const { return gatheringValue; }

GatheringMode LambdaSourceType::getGatheringMode() const { return gatheringMode; }

uint64_t LambdaSourceType::getSourceAffinity() const { return sourceAffinity; }
uint64_t LambdaSourceType::getTaskQueueId() const { return taskQueueId; }
uint64_t LambdaSourceType::getNumberOfQueues() const { return numberOfQueues; }

std::string LambdaSourceType::toString() {
    std::stringstream ss;
    ss << "LambdaSourceType => {\n";
    ss << "NumberOfBuffersToProduce :" << numBuffersToProduce;
    ss << "GatheringValue :" << gatheringValue;
    ss << "GatheringMode :" << std::string(magic_enum::enum_name(gatheringMode));
    ss << "sourceAffinity :" << sourceAffinity;
    ss << "taskQueueId :" << taskQueueId;
    ss << "numberOfQueues:  :" << numberOfQueues;
    ss << "\n}";
    return ss.str();
}

bool LambdaSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<LambdaSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<LambdaSourceType>();
    return numBuffersToProduce == otherSourceConfig->numBuffersToProduce && gatheringValue == otherSourceConfig->gatheringValue
        && gatheringMode == otherSourceConfig->gatheringMode;
}

void LambdaSourceType::reset() {
    //Nothing
}
}// namespace NES