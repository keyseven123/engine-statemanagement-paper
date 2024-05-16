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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICCOLLECTION_PROBEQUERYGENERATOR_ABSTRACTPROBEQUERYGENERATOR_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICCOLLECTION_PROBEQUERYGENERATOR_ABSTRACTPROBEQUERYGENERATOR_HPP_

#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Statistics/StatisticRequests.hpp>
#include <Statistics/Statistic.hpp>

namespace NES::Statistic {

class AbstractProbeQueryGenerator {
  public:
    virtual ~AbstractProbeQueryGenerator() = default;
    virtual DecomposedQueryPlanPtr generateProbeQuery(const StatisticProbeRequest& statisticProbeRequest, const Statistic& statistic) = 0;
};

}// namespace NES::Statistic

#endif//NES_NES_RUNTIME_INCLUDE_STATISTICCOLLECTION_PROBEQUERYGENERATOR_ABSTRACTPROBEQUERYGENERATOR_HPP_
