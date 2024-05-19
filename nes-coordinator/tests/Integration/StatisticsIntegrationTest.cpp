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

#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <Statistics/CountMin.hpp>
#include <Statistics/Requests/StatisticCreateRequest.hpp>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticCoordinator/StatisticCoordinator.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>
#include <Util/StatisticCollectorType.hpp>

#include <API/Expressions/Expressions.hpp>
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>

#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>

#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/StatisticUtil.hpp>

using namespace std;

namespace NES {

class StatisticsIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticsIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticsIntegrationTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("StatisticsIntegrationTest: Setup StatisticsIntegrationTest test class.");
        BaseIntegrationTest::SetUp();
    }
};

TEST_F(StatisticsIntegrationTest, createTest) {
    auto defaultLogicalSourceName = std::string("defaultLogicalSourceName");
    std::vector<std::string> physicalSourceNames(1, std::string("defaultPhysicalSourceName"));
    auto defaultFieldName = std::string("f1");
    auto timestampField = std::string("ts");
    auto windowSize = 5000;
    auto windowSlide = 5000;
    auto depth = 3;
    auto width = 8;
    auto startTime = 0;
    auto endTime = 5000;

    auto expressionNodePtr = (Attribute(defaultLogicalSourceName + "$" + defaultFieldName) == 1);
    EXPECT_EQ(expressionNodePtr->instanceOf<EqualsExpressionNode>(), true);

    auto createObj =
        Experimental::Statistics::StatisticCreateRequest(defaultLogicalSourceName,
                                                         defaultFieldName,
                                                         timestampField,
                                                         Experimental::Statistics::StatisticCollectorType::COUNT_MIN,
                                                         windowSize,
                                                         windowSlide,
                                                         depth,
                                                         width);

    auto probeObj = Experimental::Statistics::StatisticProbeRequest(defaultLogicalSourceName,
                                                                    defaultFieldName,
                                                                    Experimental::Statistics::StatisticCollectorType::COUNT_MIN,
                                                                    expressionNodePtr,
                                                                    physicalSourceNames,
                                                                    startTime,
                                                                    endTime);

    auto deleteObj = Experimental::Statistics::StatisticDeleteRequest(defaultLogicalSourceName,
                                                                      defaultFieldName,
                                                                      Experimental::Statistics::StatisticCollectorType::COUNT_MIN,
                                                                      endTime);

    // create coordinator
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->worker.queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;
    coordinatorConfig->worker.numWorkerThreads = 1;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("createProbeAndDeleteTest: Start coordinator");
    auto crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    auto port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()
                          ->addField(createField(defaultFieldName, BasicType::UINT64))
                          ->addField(createField(timestampField, BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource(defaultLogicalSourceName, testSchema);
    NES_DEBUG("createProbeAndDeleteTest: Coordinator started successfully");

    NES_DEBUG("createProbeAndDeleteTest: Start worker 1");
    auto workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    workerConfig1->queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;
    auto csvSourceType = CSVSourceType::create(defaultLogicalSourceName, physicalSourceNames[0]);
    const std::string filepath = std::filesystem::path(TEST_DATA_DIRECTORY) / "countMinInput.csv";
//        const std::string filepath =
//            "/home/moritz/Desktop/dataGenerator/"
//            "dist_Uniform_min_0_max_1999_numTups_600000_startTime_0_endTime_60008_outOfOrderness_0.1_worker1.csv";
    csvSourceType->setFilePath(filepath);
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(6);
    csvSourceType->setNumberOfBuffersToProduce(1);
    workerConfig1->physicalSourceTypes.add(csvSourceType);
    workerConfig1->numWorkerThreads = 1;
    auto wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    auto retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("createProbeAndDeleteTest: Worker1 started successfully");

    auto statCoordinator = crd->getStatCoordinator();
    auto success = statCoordinator->createStatistic(createObj);

    // checks if the query ID is 1
    EXPECT_EQ(success, 1);
    NES_INFO("Query successfully started");

    sleep(10);

    auto stats = statCoordinator->probeStatistic(probeObj);

    EXPECT_EQ(stats[0], 0.2);
    if (stats[0] == 0.2) {
        NES_INFO("Value successfully probed");
    }

    success = statCoordinator->deleteStatistic(deleteObj);

    EXPECT_EQ(success, true);
    NES_INFO("Query successfully stopped");
}
}// namespace NES
