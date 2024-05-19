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

#include <Statistics/Requests/StatisticCreateRequest.hpp>

namespace NES::Experimental::Statistics {

StatisticCreateRequest::StatisticCreateRequest(const std::string& logicalSourceName,
                                               const std::string& fieldName,
                                               const std::string& timestampField,
                                               StatisticCollectorType statisticCollectorType,
                                               const uint64_t windowSize,
                                               const uint64_t windowSlide,
                                               const uint64_t depth,
                                               const uint64_t width)
    : StatisticRequest(logicalSourceName, fieldName, statisticCollectorType), timestampField(timestampField),
      windowSize(windowSize), windowSlide(windowSlide), depth(depth), width(width) {}

const std::string& StatisticCreateRequest::getTimestampField() const { return timestampField; }

uint64_t StatisticCreateRequest::getWindowSize() const { return windowSize; }

uint64_t StatisticCreateRequest::getWindowSlide() const { return windowSlide; }

uint64_t StatisticCreateRequest::getDepth() const { return depth; }

uint64_t StatisticCreateRequest::getWidth() const { return width; }
}// namespace NES::Experimental::Statistics
