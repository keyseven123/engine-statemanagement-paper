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

#include <util/BenchmarkUtils.hpp>

#include "../../../tests/util/DummySink.hpp"
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Version/version.hpp>
#include <cstdint>
#include <list>
#include <random>
#include <vector>

namespace NES::Benchmarking {

uint64_t BenchmarkUtils::runSingleExperimentSeconds;
uint64_t BenchmarkUtils::periodLengthInSeconds;

void BenchmarkUtils::createUniformData(std::list<uint64_t>& dataList, uint64_t totalNumberOfTuples) {
    // uniform distribution
    int min = 0, max = 999;

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distrib(min, max);

    for (uint64_t i = 0; i < totalNumberOfTuples; ++i) {
        int tmp;
        do {
            tmp = distrib(generator);
        } while (tmp < min || tmp > max);

        dataList.emplace_back(tmp);
    }
}

uint64_t BenchmarkUtils::calcExpectedTuplesSelectivity(std::list<uint64_t> list, uint64_t selectivity) {
    uint64_t countExpectedTuples = 0;

    for (unsigned long& listIterator : list) {
        if (listIterator < selectivity)
            ++countExpectedTuples;
    }

    return countExpectedTuples;
}

void BenchmarkUtils::recordStatistics(std::vector<NodeEngine::QueryStatistics*>& statisticsVec,
                                      NodeEngine::NodeEnginePtr nodeEngine) {

    for (uint64_t i = 0; i < BenchmarkUtils::runSingleExperimentSeconds + 1; ++i) {
        int64_t nextPeriodStartTime = BenchmarkUtils::periodLengthInSeconds * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(1);
        for (auto it : queryStatisticsPtrs) {

            auto* statistics = new NodeEngine::QueryStatistics(0, 0);
            statistics->setProcessedBuffers(it->getProcessedBuffers());
            statistics->setProcessedTasks(it->getProcessedTasks());
            statistics->setProcessedTuple(it->getProcessedTuple());

            statisticsVec.push_back(statistics);
            NES_WARNING("Statistic: " << it->getQueryStatisticsAsString());
        }
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }
}

void BenchmarkUtils::computeDifferenceOfStatistics(std::vector<NodeEngine::QueryStatistics*>& statisticsVec) {
    for (uint64_t i = statisticsVec.size() - 1; i > 1; --i) {
        statisticsVec[i]->setProcessedTuple(statisticsVec[i]->getProcessedTuple() - statisticsVec[i - 1]->getProcessedTuple());
        statisticsVec[i]->setProcessedBuffers(statisticsVec[i]->getProcessedBuffers()
                                              - statisticsVec[i - 1]->getProcessedBuffers());
        statisticsVec[i]->setProcessedTasks(statisticsVec[i]->getProcessedTasks() - statisticsVec[i - 1]->getProcessedTasks());
    }

    // Deleting first Element as it serves no purpose anymore
    statisticsVec.erase(statisticsVec.begin());
}

std::string BenchmarkUtils::getCurDateTimeStringWithNESVersion() {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%d-%m-%Y_%H-%M-%S") << "_v" << NES_VERSION;
    return oss.str();
}

std::string BenchmarkUtils::getStatisticsAsCSV(NodeEngine::QueryStatistics* statistic, SchemaPtr schema) {
    return "," + std::to_string(statistic->getProcessedBuffers()) + "," + std::to_string(statistic->getProcessedTasks()) + ","
        + std::to_string(statistic->getProcessedTuple()) + ","
        + std::to_string(statistic->getProcessedTuple() * schema->getSchemaSizeInBytes());
}

void BenchmarkUtils::printOutConsole(NodeEngine::QueryStatistics* statistic, SchemaPtr schema) {
    std::cout << "numberOfTuples/sec=" << statistic << schema;
}

void BenchmarkUtils::runBenchmark(std::vector<NodeEngine::QueryStatistics*>& statisticsVec,
                                  std::vector<DataSourcePtr> benchmarkSource,
                                  std::shared_ptr<Benchmarking::SimpleBenchmarkSink> benchmarkSink,
                                  NodeEngine::NodeEnginePtr nodeEngine, Query query) {

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generateableOperators = translatePhase->transform(query.getQueryPlan()->getRootOperators()[0]);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSink(benchmarkSink)
                       .addOperatorQueryPlan(generateableOperators);

    for (auto src : benchmarkSource) {
        builder.addSource(src);
    }
    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    NES_INFO("BenchmarkUtils: QEP for " << queryPlan->toString() << " was registered in NodeEngine. Starting query now...");

    NES_INFO("BenchmarkUtils: Starting query...");
    nodeEngine->startQuery(1);
    recordStatistics(statisticsVec, nodeEngine);

    while (!benchmarkSink->completed.get_future().get())
        ;
    NES_WARNING("BenchmarkUtils: completed is true!!");

    NES_WARNING("BenchmarkUtils: Stopping query...");
    nodeEngine->stopQuery(1);
    NES_WARNING("Query was stopped!");

    computeDifferenceOfStatistics(statisticsVec);
}

}// namespace NES::Benchmarking