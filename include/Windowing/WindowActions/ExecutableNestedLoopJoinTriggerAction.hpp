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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType, class InputTypeLeft, class InputTypeRight>
class ExecutableNestedLoopJoinTriggerAction : public BaseExecutableJoinAction<KeyType, InputTypeLeft, InputTypeRight> {
  public:
    static ExecutableNestedLoopJoinTriggerActionPtr<KeyType, InputTypeLeft, InputTypeRight>
    create(LogicalJoinDefinitionPtr joinDefintion) {
        return std::make_shared<Join::ExecutableNestedLoopJoinTriggerAction<KeyType, InputTypeLeft, InputTypeRight>>(
            joinDefintion);
    }

    explicit ExecutableNestedLoopJoinTriggerAction(LogicalJoinDefinitionPtr joinDefinition) : joinDefinition(joinDefinition) {
        windowSchema = joinDefinition->getOutputSchema();
        id = UtilityFunctions::getGlobalId();
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << " join output schema=" << windowSchema->toString());
        windowTupleLayout = NodeEngine::createRowLayout(windowSchema);
    }

    virtual ~ExecutableNestedLoopJoinTriggerAction() { NES_DEBUG("~ExecutableNestedLoopJoinTriggerAction " << id << ":()"); }

    bool doAction(StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeLeft>*>* leftJoinState,
                  StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeRight>*>* rightJoinSate,
                  uint64_t currentWatermark, uint64_t lastWatermark) override {

        // get the reference to the shared ptr.
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableNestedLoopJoinTriggerAction " << id << ":: the weakExecutionContext was already expired!");
            return false;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = executionContext->allocateTupleBuffer();
        // iterate over all keys in both window states and perform the join
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":: doing the nested loop join");
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":: leftHashTable" << toString()
                                                               << " check key=" << leftHashTable.first
                                                               << " nextEdge=" << leftHashTable.second->nextEdge);
            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":: rightHashTable" << toString()
                                                                   << " check key=" << rightHashTable.first
                                                                   << " nextEdge=" << rightHashTable.second->nextEdge);
                {
                    if (leftHashTable.first == rightHashTable.first) {

                        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":: found join pair for key "
                                                                           << leftHashTable.first);
                        joinWindows(leftHashTable.first, leftHashTable.second, rightHashTable.second, tupleBuffer,
                                    currentWatermark, lastWatermark);
                    }
                }
            }
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            //write remaining buffer
            tupleBuffer.setOriginId(this->originId);
            tupleBuffer.setWatermark(currentWatermark);
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction "
                      << id << ":: Dispatch last buffer output buffer with " << tupleBuffer.getNumberOfTuples()
                      << " records, content=" << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, windowSchema)
                      << " originId=" << tupleBuffer.getOriginId() << " watermark=" << tupleBuffer.getWatermark()
                      << "windowAction=" << toString() << std::endl);

            //forward buffer to next  pipeline stage
            executionContext->dispatchBuffer(tupleBuffer);
        }

        return true;
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "ExecutableNestedLoopJoinTriggerAction " << id;
        return ss.str();
    }

    /**
     * @brief Execute the joining of all possible slices and join pairs for a given key
     * @param key the target key of the join
     * @param leftStore left content of the state
     * @param rightStore right content of the state
     * @param tupleBuffer the output buffer
     * @param currentWatermark current watermark on the left side and right side
     * @param lastWatermark last watermark on the left side and right side
     */
    void joinWindows(KeyType key, Windowing::WindowedJoinSliceListStore<InputTypeLeft>* leftStore,
                     Windowing::WindowedJoinSliceListStore<InputTypeRight>* rightStore, NodeEngine::TupleBuffer& tupleBuffer,
                     uint64_t currentWatermark, uint64_t lastWatermark) {
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":::joinWindows:leftStore currentWatermark is="
                                                           << currentWatermark << " lastWatermark=" << lastWatermark);

        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableNestedLoopJoinTriggerAction " << id << ":: the weakExecutionContext was already expired!");
            return;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto windows = std::vector<Windowing::WindowState>();

        auto leftLock = std::unique_lock(leftStore->mutex());
        auto listLeft = leftStore->getAppendList();
        auto slicesLeft = leftStore->getSliceMetadata();
        NES_TRACE("content left side for key=" << key);
        size_t id = 0;
        for (auto& left : slicesLeft) {
            NES_TRACE("left start=" << left.getStartTs() << " left end=" << left.getEndTs() << " id=" << id++);
        }

        NES_TRACE("ExecutableNestedLoopJoinTriggerAction " << id << ":: leftStore trigger " << windows.size() << " windows, on "
                                                           << slicesLeft.size() << " slices");
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            NES_TRACE("ExecutableNestedLoopJoinTriggerAction " << id << "::leftStore trigger sliceid=" << sliceId
                                                               << " start=" << slicesLeft[sliceId].getStartTs()
                                                               << " end=" << slicesLeft[sliceId].getEndTs());
        }

        uint64_t slideSize = joinDefinition->getWindowType()->getSize().getTime();

        auto rightLock = std::unique_lock(leftStore->mutex());
        auto slicesRight = rightStore->getSliceMetadata();
        auto listRight = rightStore->getAppendList();
        NES_TRACE("ExecutableNestedLoopJoinTriggerAction " << id << ":: content right side for key=" << key);
        id = 0;
        for (auto& right : slicesRight) {
            NES_TRACE("right start=" << right.getStartTs() << " right end=" << right.getEndTs() << " id=" << id++);
        }

        if (currentWatermark > lastWatermark) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":: joinWindows trigger because currentWatermark="
                                                               << currentWatermark << " > lastWatermark=" << lastWatermark);
            joinDefinition->getWindowType()->triggerWindows(windows, lastWatermark, currentWatermark);
        } else {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction "
                      << id << ":: aggregateWindows No trigger because NOT currentWatermark=" << currentWatermark
                      << " > lastWatermark=" << lastWatermark);
        }

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction "
                  << id << ":: leftStore trigger Complete or combining window for slices=" << slicesLeft.size()
                  << " windows=" << windows.size());
        int64_t largestClosedWindow = 0;

        for (size_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            for (size_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                largestClosedWindow = std::max((int64_t) window.getEndTs(), largestClosedWindow);

                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction "
                          << id << ":: window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()="
                          << slicesLeft[sliceId].getStartTs() << " window.getEndTs()=" << window.getEndTs()
                          << " slices[sliceId].getEndTs()=" << slicesLeft[sliceId].getEndTs());
                if (window.getStartTs() <= slicesLeft[sliceId].getStartTs()
                    && window.getEndTs() >= slicesLeft[sliceId].getEndTs()) {
                    size_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();

                    if (slicesLeft[sliceId].getStartTs() == slicesRight[sliceId].getStartTs()
                        && slicesLeft[sliceId].getEndTs() == slicesRight[sliceId].getEndTs()) {
                        NES_DEBUG("size left=" << listLeft[sliceId].size() << " size right=" << listRight[sliceId].size());
                        for (auto& left : listLeft[sliceId]) {
                            for (auto& right : listRight[sliceId]) {
                                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction "
                                          << id << ":: write key=" << key << " window.start()=" << window.getStartTs()
                                          << " window.getEndTs()=" << window.getEndTs() << " windowId=" << windowId
                                          << " sliceId=" << sliceId);
                                writeResultRecord(tupleBuffer, currentNumberOfTuples, window.getStartTs(), window.getEndTs(), key,
                                                  left, right);
                                currentNumberOfTuples++;
                                if (currentNumberOfTuples * windowSchema->getSchemaSizeInBytes() > tupleBuffer.getBufferSize()) {
                                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                                    executionContext->dispatchBuffer(tupleBuffer);
                                    tupleBuffer = executionContext->allocateTupleBuffer();
                                    currentNumberOfTuples = 0;
                                }
                            }
                        }
                        tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                    }
                }
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction " << id << ":: largestClosedWindow=" << largestClosedWindow
                                                                   << " slideSize=" << slideSize);

                //TODO: we have to re-activate the deletion once we are sure that it is working again
                if (largestClosedWindow != 0) {
                    //                    leftStore->removeSlicesUntil(std::abs(largestClosedWindow - (int64_t) slideSize));
                    //                    rightStore->removeSlicesUntil(std::abs(largestClosedWindow - (int64_t) slideSize));
                }
            }
        }
    }

    /**
    * @brief Writes a value to the output buffer with the following schema
    * -- start_ts, end_ts, key, value --
    * @tparam ValueType Type of the particular value
    * @param tupleBuffer reference to the tuple buffer we want to write to
    * @param index record index
    * @param startTs start ts of the window/slice
    * @param endTs end ts of the window/slice
    * @param key key of the value
    * @param value value
    */
    void writeResultRecord(NodeEngine::TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key,
                           InputTypeLeft& leftValue, InputTypeRight& rightValue) {
        NES_TRACE("write sizes left=" << sizeof(leftValue) << " right=" << sizeof(rightValue)
                                      << " typeL=" << sizeof(InputTypeLeft) << " typeR=" << sizeof(InputTypeRight));
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
        constexpr auto headerSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(KeyType);
        // copy the left record at position at the index-th row with offset=headerSize
        memcpy(tupleBuffer.getBuffer() + index * (headerSize + sizeof(InputTypeLeft) + sizeof(InputTypeRight)) + headerSize,
               &leftValue, sizeof(InputTypeLeft));
        // copy the right record at position at the index-th row with offset=headerSize+sizeof(InputTypeLeft)
        memcpy(tupleBuffer.getBuffer() + index * (headerSize + sizeof(InputTypeLeft) + sizeof(InputTypeRight)) + headerSize
                   + sizeof(InputTypeLeft),
               &rightValue, sizeof(InputTypeRight));
    }

    SchemaPtr getJoinSchema() override { return windowSchema; }

  private:
    LogicalJoinDefinitionPtr joinDefinition;
    SchemaPtr windowSchema;
    NodeEngine::MemoryLayoutPtr windowTupleLayout;
    uint64_t id;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction
