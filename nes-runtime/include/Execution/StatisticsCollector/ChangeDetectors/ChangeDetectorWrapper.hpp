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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDETECTORWRAPPER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDETECTORWRAPPER_HPP_

#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetector.hpp>
#include <memory>

namespace NES::Runtime::Execution {

class ChangeDetectorWrapper{

  public:
    explicit ChangeDetectorWrapper(std::unique_ptr<ChangeDetector> changeDetector);

    /**
     * @brief Insert value into change detector.
     * @return true if change detected, false otherwise
     */
    bool insertValue(double& value);

    /**
     * @brief Get estimated mean.
     * @return estimated mean
     */
    double getMeanEstimation();

  private:
    std::unique_ptr<ChangeDetector> changeDetector;

};
} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDETECTORWRAPPER_HPP_