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

#include <Operators/Expressions/ExpressionNode.hpp>
#include <Statistics/CountMin.hpp>
#include <Statistics/ReservoirSample.hpp>
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticCollectorStorage.hpp>
#include <Statistics/SynopsisProbeParameter/StatisticProbeParameter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

StatisticCollectorStorage::StatisticCollectorStorage() = default;

void StatisticCollectorStorage::addStatistic(StatisticCollectorIdentifier& statisticCollectorIdentifier,
                                             const StatisticPtr statistic) {
    auto currentTime = std::chrono::high_resolution_clock::now();
    uint64_t currentTimeInMilliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
    auto test = statistic->getCompletionTimestamp();
    auto diff = currentTimeInMilliseconds - test;
    NES_ERROR("Sketch emit time: {} Sketch write time: {} Difference/Latency: {} WindowStart: {} WindowEnd {}",
              test,
              currentTimeInMilliseconds,
              diff,
              statistic->getStatisticCollectorIdentifier()->getStartTime(),
              statistic->getStatisticCollectorIdentifier()->getEndTime());
    trackedStatistics[statisticCollectorIdentifier] = statistic;
    return;
}

void StatisticCollectorStorage::addStatistics(std::vector<StatisticPtr> allStatistics) {
    for (auto& statistic : allStatistics) {
        addStatistic(*(statistic->getStatisticCollectorIdentifier().get()),
                     std::dynamic_pointer_cast<Statistic>(std::shared_ptr<Statistic>(statistic)));
    }
}

double StatisticCollectorStorage::probeStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier,
                                                 StatisticProbeParameterPtr& probeParameter) {

    auto statIt = trackedStatistics.find(statisticCollectorIdentifier);
    if (statIt != trackedStatistics.end()) {
        auto statistic = statIt->second.get();
        return statistic->probe(probeParameter);
    } else {
        // ToDo create Statistic object to return instead. Issue:4354
        return -1.0;
    }
}
void StatisticCollectorStorage::deleteStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier) {
    trackedStatistics.erase(statisticCollectorIdentifier);
}

StatisticPtr StatisticCollectorStorage::getStatistic(StatisticCollectorIdentifier& statisticCollectorIdentifier) {
    auto statisticIt = trackedStatistics.find(statisticCollectorIdentifier);
    if (statisticIt != trackedStatistics.end()) {
        return statisticIt->second;
    } else {
        NES_ERROR("No statistic found for StatisticCollectorIdentifier with LogicalSourceName: {} PhysicalSourceName: {} Built "
                  "on Field: {} with StartTime: {} and EndTime: {}\n",
                  statisticCollectorIdentifier.getLogicalSourceName(),
                  statisticCollectorIdentifier.getPhysicalSourceName(),
                  statisticCollectorIdentifier.getFieldName(),
                  statisticCollectorIdentifier.getStartTime(),
                  statisticCollectorIdentifier.getEndTime());
        return nullptr;
    }
}

}// namespace NES::Experimental::Statistics
