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

#include "../../tests/util/DummySink.hpp"
#include "../../tests/util/TestQuery.hpp"
#include <filesystem>
#include <iostream>
#include <util/BenchmarkUtils.hpp>
#include <util/SimpleBenchmarkSink.hpp>
#include <util/SimpleBenchmarkSource.hpp>
#include <vector>

using namespace NES;
using namespace NES::Benchmarking;

/**
 * @brief This file/main shows how a benchmark can be created. The benchmark seen below is a filter query that was implemented by using the BM_AddBenchmark macro from <util/BenchmarkUtils.hpp>.
 */
int main() {

    // All ingestion rates from 90M to 120M in a step range of 10M
    std::vector<uint64_t> allIngestionRates;
    BenchmarkUtils::createRangeVector<uint64_t>(allIngestionRates, 220 * 1000 * 1000, 250 * 1000 * 1000, 10 * 1000 * 1000);
    //    BenchmarkUtils::createRangeVector<uint64_t>(allIngestionRates, 200 * 1000 * 1000, 550 * 1000 * 1000, 10 * 1000 * 1000);

    std::vector<uint64_t> allExperimentsDuration;
    BenchmarkUtils::createRangeVector<uint64_t>(allExperimentsDuration, 10, 20, 10);

    std::vector<uint64_t> allPeriodLengths;
    BenchmarkUtils::createRangeVector<uint64_t>(allPeriodLengths, 1, 2, 1);

    std::vector<uint16_t> allWorkerThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allWorkerThreads, 1, 5, 1);

    std::vector<uint16_t> allDataSources;
    BenchmarkUtils::createRangeVector<uint16_t>(allDataSources, 1, 2, 1);

    std::string benchmarkFolderName = "FilterQueries_" + BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName))
        throw RuntimeException("Could not create folder " + benchmarkFolderName);

    //-----------------------------------------Start of BM_SimpleFilterQuery----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector<uint64_t>(allSelectivities, 500, 700, 100);

    for (auto selectivity : allSelectivities) {
        auto benchmarkSchema = Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16);
        BM_AddBenchmark("BM_SimpleFilterQuery",
                        TestQuery::from(thisSchema).filter(Attribute("key") < selectivity).sink(DummySink::create()),
                        SimpleBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                      benchmarkSchema, ingestionRate, 1),
                        SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()), ",Selectivity",
                        "," + std::to_string(selectivity))
    }

    //-----------------------------------------End of BM_SimpleFilterQuery-----------------------------------------------------------------------------------------------

    return 0;
}