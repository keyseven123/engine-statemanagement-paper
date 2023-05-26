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
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/StreamHashJoinWindow.hpp>

namespace NES::Runtime::Execution {
Operators::LocalHashTable* StreamHashJoinWindow::getLocalHashTable(size_t index, bool leftSide) {
    if (leftSide) {
        index = index % localHashTableLeftSide.size();
        return localHashTableLeftSide[index].get();
    } else {
        index = index % localHashTableRightSide.size();
        return localHashTableRightSide[index].get();
    }
}

Operators::SharedJoinHashTable& StreamHashJoinWindow::getSharedJoinHashTable(bool isLeftSide) {
    if (isLeftSide) {
        return leftSideHashTable;
    } else {
        return rightSideHashTable;
    }
}

StreamHashJoinWindow::StreamHashJoinWindow(size_t maxNoWorkerThreads,
                                           uint64_t counterFinishedBuildingStart,
                                           uint64_t counterFinishedSinkStart,
                                           size_t totalSizeForDataStructures,
                                           size_t sizeOfRecordLeft,
                                           size_t sizeOfRecordRight,
                                           uint64_t windowStart,
                                           uint64_t windowEnd,
                                           size_t pageSize,
                                           size_t numPartitions)
    : leftSideHashTable(Operators::SharedJoinHashTable(numPartitions)),
      rightSideHashTable(Operators::SharedJoinHashTable(numPartitions)), windowStart(windowStart), windowEnd(windowEnd),
      fixedPagesAllocator(totalSizeForDataStructures) {

    counterFinishedBuilding.store(counterFinishedBuildingStart);
    counterFinishedSink.store(counterFinishedSinkStart);

    for (auto i = 0UL; i < maxNoWorkerThreads; ++i) {
        localHashTableLeftSide.emplace_back(
            std::make_unique<Operators::LocalHashTable>(sizeOfRecordLeft, numPartitions, fixedPagesAllocator, pageSize));
    }

    for (auto i = 0UL; i < maxNoWorkerThreads; ++i) {
        localHashTableRightSide.emplace_back(
            std::make_unique<Operators::LocalHashTable>(sizeOfRecordRight, numPartitions, fixedPagesAllocator, pageSize));
    }
}

uint64_t StreamHashJoinWindow::fetchSubBuild(uint64_t sub) { return counterFinishedBuilding.fetch_sub(sub); }

uint64_t StreamHashJoinWindow::fetchSubSink(uint64_t sub) { return counterFinishedSink.fetch_sub(sub); }

uint64_t StreamHashJoinWindow::getWindowEnd() const { return windowEnd; }

uint64_t StreamHashJoinWindow::getWindowStart() const { return windowStart; }

std::map<uint64_t, TupleBuffer>& StreamHashJoinWindow::getMapEmittableBuffers() { return workerIdToEmittableBuffer; }

}// namespace NES::Runtime::Execution