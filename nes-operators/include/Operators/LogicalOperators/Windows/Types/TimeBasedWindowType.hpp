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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_TIMEBASEDWINDOWTYPE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_TIMEBASEDWINDOWTYPE_HPP_

#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <vector>
namespace NES::Windowing {

class TimeBasedWindowType : public WindowType {

  public:
    enum TimeBasedSubWindowType { SLIDINGWINDOW, TUMBLINGWINDOW };

    explicit TimeBasedWindowType(TimeCharacteristicPtr timeCharacteristic);

    virtual ~TimeBasedWindowType() = default;

    /**
     * @brief getter for the SubWindowType, i.e., SlidingWindow, TumblingWindow
     * @return the SubWindowType
    */
    virtual TimeBasedSubWindowType getTimeBasedSubWindowType() = 0;

    /**
      * Calculates the next window end based on a given timestamp
      * @param currentTs
      * @return the next window end
      */
    [[nodiscard]] virtual uint64_t calculateNextWindowEnd(uint64_t currentTs) const = 0;

    /**
     * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
     * @param windows vector of windows
     * @param lastWatermark
     * @param currentWatermark
     */
    virtual void triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const = 0;

    /**
     * @brief Get the time characteristic of the window.
     * @return
     */
    [[nodiscard]] TimeCharacteristicPtr getTimeCharacteristic() const;

    /**
     * @brief method to get the window size
     * @return size of window
     */
    virtual TimeMeasure getSize() = 0;

    /**
     * @brief method to get the window slide
     * @return slide of the window
     */
    virtual TimeMeasure getSlide() = 0;

    /**
    * @return true if this is a sliding window
    */
    bool isTimeBasedWindowType() override;

    /**
     * @brief Infer stamp of time based window type
     * @param schema : the schema of the window
     * @return true if success else false
     */
    bool inferStamp(const SchemaPtr& schema) override;

  protected:
    TimeCharacteristicPtr timeCharacteristic;
};

}// namespace NES::Windowing

#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_TIMEBASEDWINDOWTYPE_HPP_
