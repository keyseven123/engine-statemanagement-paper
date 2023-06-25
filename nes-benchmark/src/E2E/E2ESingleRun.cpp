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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <E2E/E2ESingleRun.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Services/QueryService.hpp>
#include <Sources/LambdaSource.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Version/version.hpp>

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/cppkafka.h>
#endif

#include <fstream>

namespace NES::Benchmark {

void E2ESingleRun::setupCoordinatorConfig() {
    NES_INFO2("Creating coordinator and worker configuration...");
    coordinatorConf = Configurations::CoordinatorConfiguration::createDefault();

    // Coordinator configuration
    coordinatorConf->rpcPort = rpcPortSingleRun;
    coordinatorConf->restPort = restPortSingleRun;
    coordinatorConf->enableMonitoring = false;
    coordinatorConf->optimizer.distributedWindowChildThreshold = 100;
    coordinatorConf->optimizer.distributedWindowCombinerThreshold = 100;

    // Worker configuration
    coordinatorConf->worker.numWorkerThreads = configPerRun.numberOfWorkerThreads->getValue();
    coordinatorConf->worker.bufferSizeInBytes = configPerRun.bufferSizeInBytes->getValue();

    coordinatorConf->worker.numberOfBuffersInGlobalBufferManager = configPerRun.numberOfBuffersInGlobalBufferManager->getValue();
    coordinatorConf->worker.numberOfBuffersInSourceLocalBufferPool =
        configPerRun.numberOfBuffersInSourceLocalBufferPool->getValue();

    coordinatorConf->worker.coordinatorIp = coordinatorConf->coordinatorIp.getValue();
    coordinatorConf->worker.localWorkerIp = coordinatorConf->coordinatorIp.getValue();
    coordinatorConf->worker.queryCompiler.windowingStrategy =
        QueryCompilation::QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL;
    coordinatorConf->worker.numaAwareness = true;
    coordinatorConf->worker.queryCompiler.useCompilationCache = true;
    coordinatorConf->worker.enableMonitoring = false;
    coordinatorConf->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    coordinatorConf->worker.queryCompiler.queryCompilerDumpMode =
        QueryCompilation::QueryCompilerOptions::DumpMode::FILE_AND_CONSOLE;
    coordinatorConf->worker.queryCompiler.nautilusBackend =
        QueryCompilation::QueryCompilerOptions::NautilusBackend::MLIR_COMPILER;

    coordinatorConf->worker.queryCompiler.pageSize = configPerRun.pageSize->getValue();
    coordinatorConf->worker.queryCompiler.numberOfPartitions = configPerRun.numberOfPartitions->getValue();
    coordinatorConf->worker.queryCompiler.preAllocPageCnt = configPerRun.preAllocPageCnt->getValue();
    coordinatorConf->worker.queryCompiler.maxHashTableSize = configPerRun.maxHashTableSize->getValue();

    if (configOverAllRuns.joinStrategy->getValue() == "HASH_JOIN_LOCAL") {
        coordinatorConf->worker.queryCompiler.joinStrategy = NES::Runtime::Execution::JoinStrategy::HASH_JOIN_LOCAL;
    } else if (configOverAllRuns.joinStrategy->getValue() == "HASH_JOIN_GLOBAL_LOCKING") {
        coordinatorConf->worker.queryCompiler.joinStrategy = NES::Runtime::Execution::JoinStrategy::HASH_JOIN_GLOBAL_LOCKING;
    } else if (configOverAllRuns.joinStrategy->getValue() == "HASH_JOIN_GLOBAL_LOCK_FREE") {
        coordinatorConf->worker.queryCompiler.joinStrategy = NES::Runtime::Execution::JoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE;
    } else if (configOverAllRuns.joinStrategy->getValue() == "NESTED_LOOP_JOIN") {
        coordinatorConf->worker.queryCompiler.joinStrategy = NES::Runtime::Execution::JoinStrategy::NESTED_LOOP_JOIN;
    } else {
        NES_THROW_RUNTIME_ERROR("Join Strategy " << configOverAllRuns.joinStrategy->getValue() << " not supported");
    }

    if (configOverAllRuns.sourceSharing->getValue() == "on") {
        coordinatorConf->worker.enableSourceSharing = true;
        coordinatorConf->worker.queryCompiler.useCompilationCache = true;
    }

    NES_INFO2("Created coordinator and worker configuration!");
}

void E2ESingleRun::createSources() {
    size_t sourceCnt = 0;
    NES_INFO2("Creating sources and the accommodating data generation and data providing...");

    for (const auto& item : configOverAllRuns.sourceNameToDataGenerator) {
        auto logicalSourceName = item.first;
        auto dataGenerator = item.second.get();
        auto schema = dataGenerator->getSchema();
        auto logicalSource = LogicalSource::create(logicalSourceName, schema);
        coordinatorConf->logicalSources.add(logicalSource);

        auto numberOfPhysicalSrc = configPerRun.logicalSrcToNoPhysicalSrc[logicalSource->getLogicalSourceName()];
        auto numberOfTotalBuffers = configOverAllRuns.numberOfPreAllocatedBuffer->getValue() * numberOfPhysicalSrc;
        auto bufferManager =
            std::make_shared<Runtime::BufferManager>(configPerRun.bufferSizeInBytes->getValue(), numberOfTotalBuffers);
        dataGenerator->setBufferManager(bufferManager);

        allBufferManagers.emplace_back(bufferManager);

        NES_INFO2("Creating #{}physical sources for logical source {}",
                  numberOfPhysicalSrc,
                  logicalSource->getLogicalSourceName());

        for (uint64_t i = 0; i < numberOfPhysicalSrc; i++) {

            auto physicalStreamName = "physical_input" + std::to_string(sourceCnt);

            auto createdBuffers = dataGenerator->createData(configOverAllRuns.numberOfPreAllocatedBuffer->getValue(),
                                                            configPerRun.bufferSizeInBytes->getValue());

            size_t sourceAffinity = std::numeric_limits<uint64_t>::max();

            //TODO #3336: static query manager mode is currently not ported therefore only one queue
            size_t taskQueueId = 0;
            if (dataGenerator->getName() == "YSBKafka") {
#ifdef ENABLE_KAFKA_BUILD
                //Kafka is not using a data provider as Kafka itself is the provider
                auto connectionStringVec =
                    NES::Util::splitWithStringDelimiter<std::string>(configOverAllRuns.connectionString->getValue(), ",");

                std::string destinationTopic;

                NES_DEBUG2("Source no={} connects to topic={}", sourceCnt, connectionStringVec[1])
                auto kafkaSourceType = KafkaSourceType::create();
                kafkaSourceType->setBrokers(connectionStringVec[0]);
                kafkaSourceType->setTopic(connectionStringVec[1]);
                kafkaSourceType->setConnectionTimeout(1000);

                //we use the group id
                kafkaSourceType->setGroupId(std::to_string(i));
                kafkaSourceType->setNumberOfBuffersToProduce(configOverAllRuns.numberOfBuffersToProduce->getValue());
                kafkaSourceType->setBatchSize(configOverAllRuns.batchSize->getValue());

                auto physicalSource = PhysicalSource::create(logicalSourceName, physicalStreamName, kafkaSourceType);
                coordinatorConf->worker.physicalSources.add(physicalSource);

#else
                NES_THROW_RUNTIME_ERROR("Kafka not supported on OSX");
#endif
            } else {
                auto dataProvider =
                    DataProvision::DataProvider::createProvider(/* sourceIndex */ sourceCnt, configOverAllRuns, createdBuffers);

                // Adding necessary items to the corresponding vectors
                allDataProviders.emplace_back(dataProvider);

                size_t generatorQueueIndex = 0;
                auto dataProvidingFunc = [this, sourceCnt, generatorQueueIndex](Runtime::TupleBuffer& buffer, uint64_t) {
                    allDataProviders[sourceCnt]->provideNextBuffer(buffer, generatorQueueIndex);
                };

                LambdaSourceTypePtr sourceConfig =
                    LambdaSourceType::create(dataProvidingFunc,
                                             configOverAllRuns.numberOfBuffersToProduce->getValue(),
                                             /* gatheringValue */ 0,
                                             GatheringMode::INTERVAL_MODE,
                                             sourceAffinity,
                                             taskQueueId);

                auto physicalSource = PhysicalSource::create(logicalSourceName, physicalStreamName, sourceConfig);
                coordinatorConf->worker.physicalSources.add(physicalSource);
            }
            sourceCnt += 1;
            NES_INFO2("Created physical source #{} for {}", numberOfPhysicalSrc, logicalSource->getLogicalSourceName());
        }
    }
    NES_INFO2("Created sources and the accommodating data generation and data providing!");
}

void E2ESingleRun::runQuery() {
    NES_INFO2("Starting nesCoordinator...");
    coordinator = std::make_shared<NesCoordinator>(coordinatorConf);
    auto rpcPort = coordinator->startCoordinator(/* blocking */ false);
    NES_INFO2("Started nesCoordinator at {}", rpcPort);

    auto queryService = coordinator->getQueryService();
    auto queryCatalog = coordinator->getQueryCatalogService();

    for (size_t i = 0; i < configPerRun.numberOfQueriesToDeploy->getValue(); i++) {
        QueryId queryId = 0;
        NES_INFO2("E2EBase: Submit query = {}", configOverAllRuns.query->getValue());
        queryId = queryService->validateAndQueueAddQueryRequest(configOverAllRuns.query->getValue(), "BottomUp");

        submittedIds.push_back(queryId);

        for (auto id : submittedIds) {
            bool res = waitForQueryToStart(id, queryCatalog, std::chrono::seconds(180));
            if (!res) {
                NES_THROW_RUNTIME_ERROR("run does not succeed for id=" << id);
            }
            NES_INFO2("E2EBase: query started with id={}", id);
        }
    }
    NES_DEBUG2("Starting the data providers...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->start();
    }

    // Wait for the system to come to a steady state
    NES_INFO2("Now waiting for {} s to let the system come to a steady state!",
              configOverAllRuns.startupSleepIntervalInSeconds->getValue());
    sleep(configOverAllRuns.startupSleepIntervalInSeconds->getValue());

    // For now, we only support one way of collecting the measurements
    NES_INFO2("Starting to collect measurements...");

    uint64_t found = 0;
    while (found != submittedIds.size()) {
        for (auto id : submittedIds) {
            auto stats = coordinator->getNodeEngine()->getQueryStatistics(id);
            for (auto iter : stats) {
                while (iter->getProcessedTuple() < 1) {
                    NES_DEBUG2("Query with id {} not ready with no. tuples = {}. Sleeping for a second now...",
                               id,
                               iter->getProcessedTuple());
                    sleep(1);
                }
                NES_INFO2("Query with id {} Ready with no. tuples = {}", id, iter->getProcessedTuple());
                ++found;
            }
        }
    }

    NES_INFO2("Starting measurements...");
    // We have to measure once more than the required numMeasurementsToCollect as we calculate deltas later on
    for (uint64_t cnt = 0; cnt <= configOverAllRuns.numMeasurementsToCollect->getValue(); ++cnt) {
        int64_t nextPeriodStartTime = configOverAllRuns.experimentMeasureIntervalInSeconds->getValue() * 1000;
        nextPeriodStartTime +=
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        uint64_t timeStamp =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::map<uint64_t, uint64_t> previousMap;
        measurements.addNewTimestamp(timeStamp);
        for (auto id : submittedIds) {
            auto statisticsCoordinator = coordinator->getNodeEngine()->getQueryStatistics(id);
            size_t processedTasks = 0;
            size_t processedBuffers = 0;
            size_t processedTuples = 0;
            size_t latencySum = 0;
            size_t queueSizeSum = 0;
            size_t availGlobalBufferSum = 0;
            size_t availFixedBufferSum = 0;
            for (auto subPlanStatistics : statisticsCoordinator) {
                if (subPlanStatistics->getProcessedBuffers() != 0) {
                    processedTasks += subPlanStatistics->getProcessedTasks();
                    processedBuffers += subPlanStatistics->getProcessedBuffers();
                    processedTuples += subPlanStatistics->getProcessedTuple();
                    latencySum += (subPlanStatistics->getLatencySum() / subPlanStatistics->getProcessedBuffers());
                    queueSizeSum += (subPlanStatistics->getQueueSizeSum() / subPlanStatistics->getProcessedBuffers());
                    availGlobalBufferSum +=
                        (subPlanStatistics->getAvailableGlobalBufferSum() / subPlanStatistics->getProcessedBuffers());
                    availFixedBufferSum +=
                        (subPlanStatistics->getAvailableFixedBufferSum() / subPlanStatistics->getProcessedBuffers());
                }

                measurements.addNewMeasurement(processedTasks,
                                               processedBuffers,
                                               processedTuples,
                                               latencySum,
                                               queueSizeSum,
                                               availGlobalBufferSum,
                                               availFixedBufferSum,
                                               timeStamp);
                std::stringstream ss;
                size_t pipeCnt = 0;

                ss << "time=" << timeStamp << " subplan=" << subPlanStatistics->getSubQueryId()
                   << " procTasks=" << processedTasks;
                for (auto& pipe : subPlanStatistics->getPipelineIdToTaskMap()) {
                    for (auto& worker : pipe.second) {
                        ss << " pipeNo:" << pipe.first << " worker=" << worker.first << " tasks=" << worker.second;
                    }
                }
                std::cout << ss.str() << std::endl;
            }
        }

        // Calculating the time to sleep
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        auto sleepTime = nextPeriodStartTime - curTime;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    }

    NES_INFO2("Done measuring!");
    NES_INFO2("Done with single run!");
}

void E2ESingleRun::stopQuery() {
    NES_INFO2("Stopping the query...");

    auto queryService = coordinator->getQueryService();
    auto queryCatalog = coordinator->getQueryCatalogService();

    for (auto id : submittedIds) {
        // Sending a stop request to the coordinator with a timeout of 30 seconds
        queryService->validateAndQueueStopQueryRequest(id);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + stopQueryTimeoutInSec) {
            NES_TRACE2("checkStoppedOrTimeout: check query status for {}", id);
            if (queryCatalog->getEntryForQuery(id)->getQueryStatus() == QueryStatus::STOPPED) {
                NES_TRACE2("checkStoppedOrTimeout: status for {} reached stopped", id);
                break;
            }
            NES_DEBUG2("checkStoppedOrTimeout: status not reached for {} as status is={}",
                       id,
                       queryCatalog->getEntryForQuery(id)->getQueryStatusAsString());
            std::this_thread::sleep_for(stopQuerySleep);
        }
        NES_TRACE2("checkStoppedOrTimeout: expected status not reached within set timeout");
    }

    NES_DEBUG2("Stopping data providers...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->stop();
    }
    NES_DEBUG2("Stopped data providers!");

    // Starting a new thread that waits
    std::shared_ptr<std::promise<bool>> stopPromiseCord = std::make_shared<std::promise<bool>>();
    std::thread waitThreadCoordinator([this, stopPromiseCord]() {
        std::future<bool> stopFutureCord = stopPromiseCord->get_future();
        bool satisfied = false;
        while (!satisfied) {
            switch (stopFutureCord.wait_for(std::chrono::seconds(1))) {
                case std::future_status::ready: {
                    satisfied = true;
                }
                case std::future_status::timeout:
                case std::future_status::deferred: {
                    if (coordinator->isCoordinatorRunning()) {
                        NES_DEBUG2("Waiting for stop wrk cause #tasks in the queue: {}",
                                   coordinator->getNodeEngine()->getQueryManager()->getNumberOfTasksInWorkerQueues());
                    } else {
                        NES_DEBUG2("worker stopped");
                    }
                    break;
                }
            }
        }
    });

    NES_INFO2("Stopping coordinator...");
    bool retStoppingCoord = coordinator->stopCoordinator(true);
    stopPromiseCord->set_value(retStoppingCoord);
    NES_ASSERT(stopPromiseCord, retStoppingCoord);

    waitThreadCoordinator.join();
    NES_INFO2("Coordinator stopped!");

    NES_INFO2("Stopped the query!");
}

void E2ESingleRun::writeMeasurementsToCsv() {
    NES_INFO2("Writing the measurements to {}", configOverAllRuns.outputFile->getValue());
    std::stringstream resultOnConsole;
    auto schemaSizeInB = configOverAllRuns.getTotalSchemaSize();
    std::string queryString = configOverAllRuns.query->getValue();
    std::replace(queryString.begin(), queryString.end(), ',', ' ');

    std::stringstream outputCsvStream;

    for (const auto& measurementsCsv :
         measurements.getMeasurementsAsCSV(schemaSizeInB, configPerRun.numberOfQueriesToDeploy->getValue())) {
        outputCsvStream << "\"" << configOverAllRuns.benchmarkName->getValue() << "\"";
        outputCsvStream << "," << NES_VERSION << "," << schemaSizeInB;
        outputCsvStream << "," << measurementsCsv;
        outputCsvStream << "," << configPerRun.numberOfWorkerThreads->getValue();
        outputCsvStream << "," << configPerRun.numberOfQueriesToDeploy->getValue();
        outputCsvStream << ","
                        << "\"" << configPerRun.getStringLogicalSourceToNumberOfPhysicalSources() << "\"";
        outputCsvStream << "," << configPerRun.bufferSizeInBytes->getValue();
        outputCsvStream << "," << configOverAllRuns.inputType->getValue();
        outputCsvStream << "," << configOverAllRuns.dataProviderMode->getValue();
        outputCsvStream << ","
                        << "\"" << queryString << "\"";
        outputCsvStream << std::endl;
    }

    std::ofstream ofs;
    ofs.open(configOverAllRuns.outputFile->getValue(), std::ofstream::app);
    NES_DEBUG("write to file=" << configOverAllRuns.outputFile->getValue());
    ofs << outputCsvStream.str();
    ofs.close();

    NES_INFO2("Done writing the measurements to {}", configOverAllRuns.outputFile->getValue());
    NES_INFO2("Statistics are: {}", outputCsvStream.str())
    std::cout << "Throughput=" << measurements.getThroughputAsString() << std::endl;
}

E2ESingleRun::E2ESingleRun(E2EBenchmarkConfigPerRun& configPerRun,
                           E2EBenchmarkConfigOverAllRuns& configOverAllRuns,
                           uint16_t rpcPort,
                           uint16_t restPort)
    : configPerRun(configPerRun), configOverAllRuns(configOverAllRuns), rpcPortSingleRun(rpcPort), restPortSingleRun(restPort) {}

E2ESingleRun::~E2ESingleRun() {
    coordinatorConf.reset();
    coordinator.reset();

    for (auto& dataProvider : allDataProviders) {
        dataProvider.reset();
    }
    allDataProviders.clear();

    for (auto& bufferManager : allBufferManagers) {
        bufferManager.reset();
    }
    allBufferManagers.clear();
}

void E2ESingleRun::run() {
    setupCoordinatorConfig();
    createSources();
    runQuery();
    stopQuery();
    writeMeasurementsToCsv();
}

bool E2ESingleRun::waitForQueryToStart(QueryId queryId,
                                       const QueryCatalogServicePtr& queryCatalogService,
                                       std::chrono::seconds timeoutInSec) {
    NES_TRACE2("TestUtils: wait till the query {} gets into Running status.", queryId);
    auto start_timestamp = std::chrono::system_clock::now();

    NES_TRACE2("TestUtils: Keep checking the status of query {} until a fixed time out", queryId);
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        auto queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
        if (!queryCatalogEntry) {
            NES_ERROR2("TestUtils: unable to find the entry for query {} in the query catalog.", queryId);
            return false;
        }
        NES_TRACE2("TestUtils: Query {} is now in status {}", queryId, queryCatalogEntry->getQueryStatusAsString());
        QueryStatus status = queryCatalogEntry->getQueryStatus();

        switch (queryCatalogEntry->getQueryStatus()) {
            case QueryStatus::MARKED_FOR_HARD_STOP:
            case QueryStatus::MARKED_FOR_SOFT_STOP:
            case QueryStatus::SOFT_STOP_COMPLETED:
            case QueryStatus::SOFT_STOP_TRIGGERED:
            case QueryStatus::STOPPED:
            case QueryStatus::RUNNING: {
                return true;
            }
            case QueryStatus::FAILED: {
                NES_ERROR2("Query failed to start. Expected: Running or Optimizing but found {}",
                           std::string(magic_enum::enum_name(status)));
                return false;
            }
            default: {
                NES_WARNING2("Expected: Running or Scheduling but found {}", std::string(magic_enum::enum_name(status)));
                break;
            }
        }

        std::this_thread::sleep_for(E2ESingleRun::sleepDuration);
    }
    NES_TRACE2("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
    return false;
}

const CoordinatorConfigurationPtr& E2ESingleRun::getCoordinatorConf() const { return coordinatorConf; }

Measurements::Measurements& E2ESingleRun::getMeasurements() { return measurements; }

}// namespace NES::Benchmark
