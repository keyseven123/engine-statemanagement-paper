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

#ifndef NES_INCLUDE_UTIL_PLACEMENTTYPE_HPP_
#define NES_INCLUDE_UTIL_PLACEMENTTYPE_HPP_
#include <cinttypes>
#include <stdint.h>
#include <string>
#include <unordered_map>

namespace NES {
class PlacementStrategy {

  public:
    enum Value : uint8_t {
        TopDown = 0,
        BottomUp = 1,
        IFCOP = 2,
        ILP = 3,
        Manual = 4,
        MlHeuristic = 4
        // FIXME: enable them with issue #755
        // LowLatency,
        //  HighThroughput,
        //  MinimumResourceConsumption,
        //  MinimumEnergyConsumption,
        // HighAvailability
    };

    /**
     * @brief Get Placement Strategy from string
     * @param placementStrategy : string representation of placement strategy
     * @return enum representing Placement Strategy
     */
    static Value getFromString(const std::string placementStrategy);

    /**
     * @brief Get Placement Strategy in string representation
     * @param placementStrategy : enum value of the Placement Strategy
     * @return string representation of Placement Strategy
     */
    static std::string toString(const Value placementStrategy);
};

}// namespace NES
#endif// NES_INCLUDE_UTIL_PLACEMENTTYPE_HPP_
