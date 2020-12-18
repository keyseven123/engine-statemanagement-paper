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

#ifndef INCLUDE_JOIN_WINDOW_HPP_
#define INCLUDE_JOIN_WINDOW_HPP_

#include <Util/Logger.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>

namespace NES::Join {

enum JoinSides { leftSide = 0, rightSide = 1 };

/**
 * @brief The abstract window handler is the base class for all window handlers
 */
class AbstractJoinHandler : public std::enable_shared_from_this<AbstractJoinHandler> {
  public:
    explicit AbstractJoinHandler(Join::LogicalJoinDefinitionPtr joinDefinition, Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger)
        : joinDefinition(std::move(joinDefinition)), executablePolicyTrigger(std::move(executablePolicyTrigger)) {
        // nop
    }

    template<class Type>
    auto as() {
        return std::dynamic_pointer_cast<Type>(shared_from_this());
    }

    /**
    * @brief Starts thread to check if the window should be triggered.
    * @return boolean if the window thread is started
    */
    virtual bool start() = 0;

    /**
     * @brief Stops the window thread.
     * @return
     */
    virtual bool stop() = 0;

    /**
     * @brief triggers all ready windows.
     * @return
     */
    virtual void trigger() = 0;

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    virtual bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline,
                       uint32_t pipelineStageId, uint64_t originId) = 0;

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    virtual Windowing::WindowManagerPtr getWindowManager() = 0;

    virtual std::string toString() = 0;

    LogicalJoinDefinitionPtr getJoinDefinition() { return joinDefinition; }

    /**
     * @brief Gets the last processed watermark
     * @param side
     * @return watermark
     */
    [[nodiscard]] uint64_t getLastWatermark(JoinSides side) const {
        switch (side) {
            case leftSide: return lastWatermarkLeft;
            case rightSide: return lastWatermarkRight;
            default: NES_ERROR("getLastWatermark: invalid side"); return 0;
        }
    }

    /**
     * @brief Sets the last watermark
     * @param lastWatermark
     * @param side
     */
    void setLastWatermark(uint64_t lastWatermark, JoinSides side) {
        switch (side) {
            case leftSide: lastWatermarkLeft = lastWatermark; break;
            case rightSide: lastWatermarkRight = lastWatermark; break;
            default: NES_ERROR("setLastWatermark: invalid side");
        }
    }

    /**
     * @brief Gets the maximal processed ts per origin id.
     * @param originId
     * @param side
     * @return max ts.
     */
    uint32_t getMaxTs(uint32_t originId, JoinSides side) {
        switch (side) {
            case leftSide: return originIdToMaxTsMapLeft[originId];
            case rightSide: return originIdToMaxTsMapRight[originId];
            default: NES_ERROR("getMaxTs: invalid side"); return 0;
        }
    };

    /**
     * @brief Gets number of mappings.
     * @param side
     * @return size of origin map.
     */
    uint64_t getNumberOfMappings(JoinSides side) {
        switch (side) {
            case leftSide: return originIdToMaxTsMapLeft.size();
            case rightSide: return originIdToMaxTsMapRight.size();
            default: NES_ERROR("getNumberOfMappings: invalid side"); return 0;
        }
    };

    /**
     * @brief Calculate the min watermark for left or right side for among each input edge
     * @return MinAggregationDescriptor watermark
     */
    uint64_t getMinWatermark(JoinSides side) {
        if (side == leftSide) {
            if (originIdToMaxTsMapLeft.size() == numberOfInputEdgesLeft) {
                std::map<uint64_t, uint64_t>::iterator min =
                    std::min_element(originIdToMaxTsMapLeft.begin(), originIdToMaxTsMapLeft.end(),
                                     [](const std::pair<uint64_t, uint64_t>& a, const std::pair<uint64_t, uint64_t>& b) -> bool {
                                         return a.second < b.second;
                                     });
                NES_DEBUG("getMinWatermark() originIdToMaxTsMapLeft return min=" << min->second);
                return min->second;
            } else {
                NES_DEBUG("getMinWatermark() return 0 because there is no mapping yet current number of mappings for "
                          "originIdToMaxTsMapLeft="
                          << originIdToMaxTsMapLeft.size() << " expected mappings=" << numberOfInputEdgesLeft);
                return 0;//TODO: we have to figure out how many downstream positions are there
            }
        } else if (side == rightSide) {
            if (originIdToMaxTsMapRight.size() == numberOfInputEdgesRight) {
                std::map<uint64_t, uint64_t>::iterator min =
                    std::min_element(originIdToMaxTsMapRight.begin(), originIdToMaxTsMapRight.end(),
                                     [](const std::pair<uint64_t, uint64_t>& a, const std::pair<uint64_t, uint64_t>& b) -> bool {
                                         return a.second < b.second;
                                     });
                NES_DEBUG("getMinWatermark() originIdToMaxTsMapRight return min=" << min->second);
                return min->second;
            } else {
                NES_DEBUG("getMinWatermark() return 0 because there is no mapping yet current number of mappings for "
                          "originIdToMaxTsMapRight="
                          << originIdToMaxTsMapRight.size() << " expected mappings=" << numberOfInputEdgesRight);
                return 0;//TODO: we have to figure out how many downstream positions are there
            }
        } else {
            NES_THROW_RUNTIME_ERROR("getNumberOfMappings: invalid side");
        }
    }

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    virtual void updateMaxTs(uint64_t ts, uint64_t originId) {
        NES_DEBUG("JoinHandler: updateAllMaxTs with ts=" << ts << " originId=" << originId);
        //TODO this is not correct as we have to distinguish between left and rigt side
        if (joinDefinition->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnWatermarkChange) {
            auto beforeMinLeft = getMinWatermark(leftSide);
            auto beforeMinRight = getMinWatermark(rightSide);
            originIdToMaxTsMapLeft[originId] = std::max(originIdToMaxTsMapLeft[originId], ts);
            originIdToMaxTsMapRight[originId] = std::max(originIdToMaxTsMapRight[originId], ts);
            auto afterMinLeft = getMinWatermark(leftSide);
            auto afterMinRight = getMinWatermark(rightSide);
            if (beforeMinLeft < afterMinLeft || beforeMinRight < afterMinRight) {
                trigger();
            }
        } else {
            originIdToMaxTsMapLeft[originId] = std::max(originIdToMaxTsMapLeft[originId], ts);
            originIdToMaxTsMapRight[originId] = std::max(originIdToMaxTsMapRight[originId], ts);
        }
    }

  protected:
    LogicalJoinDefinitionPtr joinDefinition;
    Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
    std::atomic_bool running{false};
    Windowing::WindowManagerPtr windowManager;
    PipelineStagePtr nextPipeline;
    uint32_t pipelineStageId;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    uint64_t originId;
    std::map<uint64_t, uint64_t> originIdToMaxTsMapLeft;
    std::map<uint64_t, uint64_t> originIdToMaxTsMapRight;
    uint64_t lastWatermarkLeft;
    uint64_t lastWatermarkRight;
    uint64_t numberOfInputEdgesLeft;
    uint64_t numberOfInputEdgesRight;
};

}// namespace NES::Join
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
