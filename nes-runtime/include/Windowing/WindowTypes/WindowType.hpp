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

#ifndef NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_

#include "../WindowingForwardRefs.hpp"
#include <vector>
namespace NES::Windowing {

class WindowType {
  public:
    explicit WindowType(TimeCharacteristicPtr timeCharacteristic);

    virtual ~WindowType() = default;
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
     * @return true if this is a tumbling window
     */
    virtual bool isTumblingWindow();

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
    virtual bool isSlidingWindow();

    virtual std::string toString() = 0;

    /**
     * @brief Infer stamp of the window type
     * @param schema : the schema of the window
     * @return true if success else false
     */
    bool inferStamp(const SchemaPtr& schema);

    /**
     * @brief Check equality of this window type with the input window type
     * @param otherWindowType : the other window type to compare with
     * @return true if equal else false
     */
    virtual bool equal(WindowTypePtr otherWindowType) = 0;

  protected:
    TimeCharacteristicPtr timeCharacteristic;
};

}// namespace NES::Windowing

#endif// NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
