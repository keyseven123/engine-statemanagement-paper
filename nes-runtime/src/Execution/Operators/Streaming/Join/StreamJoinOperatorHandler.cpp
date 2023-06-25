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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/StreamHashJoinWindow.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <optional>

namespace NES::Runtime::Execution::Operators {

StreamWindowPtr StreamJoinOperatorHandler::createNewWindow(uint64_t timestamp) {
    std::unique_lock lock(windowCreateLock);
    for (auto& curWindow : windows) {
        if (curWindow->getWindowStart() <= timestamp && timestamp < curWindow->getWindowEnd()) {
            return curWindow;
        }
    }

    auto windowStart = sliceAssigner.getSliceStartTs(timestamp);
    auto windowEnd = sliceAssigner.getSliceEndTs(timestamp);
    if (joinStrategy == JoinStrategy::NESTED_LOOP_JOIN) {
        NES_DEBUG2("Create NLJ Window for window start={} windowend={} for ts={}", windowStart, windowEnd, timestamp);
        windows.emplace_back(std::make_unique<NLJWindow>(windowStart, windowEnd));
    } else {
        StreamHashJoinOperatorHandler* ptr = static_cast<StreamHashJoinOperatorHandler*>(this);
        auto newWindow = std::make_shared<StreamHashJoinWindow>(numberOfWorkerThreads,
                                                                joinSchemaLeft->getSchemaSizeInBytes(),
                                                                joinSchemaRight->getSchemaSizeInBytes(),
                                                                windowStart,
                                                                windowEnd,
                                                                ptr->getTotalSizeForDataStructures(),
                                                                ptr->getPageSize(),
                                                                ptr->getPreAllocPageSizeCnt(),
                                                                ptr->getNumPartitions());
        windows.emplace_back(newWindow);
        NES_DEBUG2("Create Hash Window for window start={} windowend={} for ts={}", windowStart, windowEnd, timestamp);
        return newWindow;
    }
    return getWindowByTimestampOrCreateIt(timestamp);
}

const std::string& StreamJoinOperatorHandler::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& StreamJoinOperatorHandler::getJoinFieldNameRight() const { return joinFieldNameRight; }

SchemaPtr StreamJoinOperatorHandler::getJoinSchemaLeft() const { return joinSchemaLeft; }
SchemaPtr StreamJoinOperatorHandler::getJoinSchemaRight() const { return joinSchemaRight; }

void StreamJoinOperatorHandler::deleteWindow(uint64_t windowIdentifier) {
    NES_DEBUG2("StreamJoinOperatorHandler deletes window with id={}", windowIdentifier);
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        windows.remove(window.value());
    }
}

StreamWindowPtr StreamJoinOperatorHandler::getWindowByTimestampOrCreateIt(uint64_t timestamp) {
    size_t i = 0;
    for (auto& curWindow : windows) {
        if (curWindow->getWindowStart() <= timestamp && timestamp < curWindow->getWindowEnd()) {
            return curWindow;
        }
    }
    return createNewWindow(timestamp);
}

StreamWindow* StreamJoinOperatorHandler::getWindowByTimestampOrCreateIt2(uint64_t timestamp) {
    size_t i = 0;
    for (auto& curWindow : windows) {
        if (curWindow->getWindowStart() <= timestamp && timestamp < curWindow->getWindowEnd()) {
            return curWindow.get();
        }
    }
    auto newWindow = createNewWindow(timestamp);
    return newWindow.get();
}

uint64_t StreamJoinOperatorHandler::getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide) {
    const auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        return window.value()->getNumberOfTuples(sizeOfTupleInByte, isLeftSide);
    }

    return -1;
}
uint64_t StreamJoinOperatorHandler::getNumberOfWindows() { return windows.size(); }

uint8_t* StreamJoinOperatorHandler::getFirstTuple(uint64_t windowIdentifier, bool isLeftSide) {
    const auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        if (joinStrategy == JoinStrategy::NESTED_LOOP_JOIN) {
            return static_cast<NLJWindow*>(window->get())->getTuple(sizeOfTupleInByte, 0, isLeftSide);
        }
        //TODO For hash window it is not clear what this would be
    }
    return std::nullptr_t();
}

std::optional<StreamWindowPtr> StreamJoinOperatorHandler::getWindowByWindowIdentifier(uint64_t windowIdentifier) {
    for (auto& curWindow : windows) {
        if (curWindow->getWindowIdentifier() == windowIdentifier) {
            return curWindow;
        }
    }
    return std::nullopt;
}

SchemaPtr StreamJoinOperatorHandler::getSchema(bool isLeftSide) {
    if (isLeftSide) {
        return joinSchemaLeft;
    } else {
        return joinSchemaRight;
    }
}

std::pair<uint64_t, uint64_t> StreamJoinOperatorHandler::getWindowStartEnd(uint64_t windowIdentifier) {
    const auto& window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        return {window.value()->getWindowStart(), window.value()->getWindowEnd()};
    }
    return std::pair<uint64_t, uint64_t>();
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const SchemaPtr& joinSchemaLeft,
                                                     const SchemaPtr& joinSchemaRight,
                                                     const std::string& joinFieldNameLeft,
                                                     const std::string& joinFieldNameRight,
                                                     const std::vector<OriginId>& origins,
                                                     size_t windowSize,
                                                     const JoinStrategy joinStrategy)
    : sliceAssigner(windowSize, windowSize), watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)),
      joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight), joinFieldNameLeft(joinFieldNameLeft),
      joinFieldNameRight(joinFieldNameRight), joinStrategy(joinStrategy), sequenceNumber(0) {}

void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG2("start HashJoinOperatorHandler");
}

void StreamJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG2("stop HashJoinOperatorHandler");
}

uint64_t StreamJoinOperatorHandler::getNextSequenceNumber() { return sequenceNumber++; }

void StreamJoinOperatorHandler::setup(uint64_t newNumberOfWorkerThreads) {
    if (alreadySetup) {
        NES_DEBUG2("StreamJoinOperatorHandler::setup was called already!");
        return;
    }
    alreadySetup = true;

    NES_DEBUG2("HashJoinOperatorHandler::setup was called!");
    // It does not matter here if we put true or false as a parameter
    this->numberOfWorkerThreads = newNumberOfWorkerThreads;
}

uint64_t StreamJoinOperatorHandler::getLastWatermark() { return watermarkProcessor->getCurrentWatermark(); }
void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, uint64_t workerId) {
    workerIdToWatermarkMap[workerId] = watermark;
}

void StreamJoinOperatorHandler::addOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

OperatorId StreamJoinOperatorHandler::getOperatorId() { return operatorId; }

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() {
    auto minVal =
        std::min_element(std::begin(workerIdToWatermarkMap), std::end(workerIdToWatermarkMap), [](const auto& l, const auto& r) {
            return l.second < r.second;
        });
    return minVal == workerIdToWatermarkMap.end() ? -1 : minVal->second;
}

std::vector<uint64_t>
StreamJoinOperatorHandler::checkWindowsTrigger(uint64_t watermarkTs, uint64_t sequenceNumber, OriginId originId) {
    std::vector<uint64_t> triggerableWindowIdentifiers;

    //The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

    NES_DEBUG2("newGlobalWatermark {} watermarkTs {} sequenceNumber {} originId {} watermarkstatus={}",
               newGlobalWatermark,
               watermarkTs,
               sequenceNumber,
               originId,
               watermarkProcessor->getCurrentStatus());
    for (auto& window : windows) {
        if (window->getWindowEnd() > newGlobalWatermark) {
            continue;
        }

        auto expected = StreamWindow::WindowState::BOTH_SIDES_FILLING;
        if (window->compareExchangeStrong(expected, StreamWindow::WindowState::EMITTED_TO_SINK)) {
            triggerableWindowIdentifiers.emplace_back(window->getWindowIdentifier());
            NES_DEBUG2("Added window with id {} to the triggerable windows...", window->getWindowIdentifier());
        }
    }

    return triggerableWindowIdentifiers;
}

}// namespace NES::Runtime::Execution::Operators