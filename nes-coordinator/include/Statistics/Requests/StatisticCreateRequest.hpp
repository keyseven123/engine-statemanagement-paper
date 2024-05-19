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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICCREATEREQUEST_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICCREATEREQUEST_HPP_

#include <Statistics/Requests/StatisticRequest.hpp>
#include <Util/StatisticCollectorType.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::Experimental::Statistics {

/**
 * @brief the inherited class that defines what is needed to build statistics
 */
class StatisticCreateRequest : public StatisticRequest {
  public:
    StatisticCreateRequest(const std::string& logicalSourceName,
                           const std::string& fieldName,
                           const std::string& timestampField,
                           StatisticCollectorType statisticCollectorType,
                           uint64_t windowSize,
                           uint64_t windowSlide,
                           uint64_t depth,
                           uint64_t width);

    [[nodiscard]] const std::string& getTimestampField() const;

    [[nodiscard]] uint64_t getWindowSize() const;

    [[nodiscard]] uint64_t getWindowSlide() const;

    [[nodiscard]] uint64_t getDepth() const;

    [[nodiscard]] uint64_t getWidth() const;

  private:
    std::string timestampField;
    const uint64_t windowSize;
    const uint64_t windowSlide;
    const uint64_t depth;
    const uint64_t width;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICCREATEREQUEST_HPP_
