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

#ifndef NES_CORE_INCLUDE_SERVICES_PATTERNPARSINGSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_PATTERNPARSINGSERVICE_HPP_

#include <memory>

namespace NES {

class Query;
using QueryPtr = std::shared_ptr<Query>;

/**
 * This class is responsible for transforming a pattern string into a query object.
 */
class PatternParsingService {
  public:
    /**
    *  @brief Creates a Query Object from a pattern string
    *  @param pattern as a string
    *  @return Query object pointer
    */
    NES::QueryPtr createPatternFromCodeString(const std::string& patternString);
};

}// namespace NES

#endif // NES_CORE_INCLUDE_SERVICES_PATTERNPARSINGSERVICE_HPP_
