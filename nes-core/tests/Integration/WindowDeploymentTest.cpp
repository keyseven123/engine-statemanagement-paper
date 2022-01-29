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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>

#include <Common/ExecutableType/Array.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class WindowDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("WindowDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WindowDeploymentTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down WindowDeploymentTest class." << std::endl; }
};

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerCentralTumblingWindowQueryEventTimeForExdra) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0ull);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "exdra.csv");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    auto windowStream = PhysicalSource::create("exdra", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testDeployOneWorkerCentralTumblingWindowQueryEventTimeForExdra.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"exdra\").window(TumblingWindow::of(EventTime(Attribute(\"metadata_generated\")), "
                   "Seconds(10))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"features_properties_capacity\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "exdra$start:INTEGER,exdra$end:INTEGER,exdra$id:INTEGER,exdra$features_properties_capacity:INTEGER\n"
                             "1262343610000,1262343620000,1,736\n"
                             "1262343620000,1262343630000,2,1348\n"
                             "1262343630000,1262343640000,3,4575\n"
                             "1262343640000,1262343650000,4,1358\n"
                             "1262343650000,1262343660000,5,1288\n"
                             "1262343660000,1262343670000,6,3458\n"
                             "1262343670000,1262343680000,7,1128\n"
                             "1262343680000,1262343690000,8,1079\n"
                             "1262343690000,1262343700000,9,2071\n"
                             "1262343700000,1262343710000,10,2632\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testYSBWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string input =
        R"(Schema::create()->addField("ysb$user_id", UINT64)->addField("ysb$page_id", UINT64)->addField("ysb$campaign_id", UINT64)->addField("ysb$ad_type", UINT64)->addField("ysb$event_type", UINT64)->addField("ysb$current_ms", UINT64)->addField("ysb$ip", UINT64)->addField("ysb$d1", UINT64)->addField("ysb$d2", UINT64)->addField("ysb$d3", UINT32)->addField("ysb$d4", UINT16);)";
    NES_ASSERT(crd->getStreamCatalogService()->registerLogicalStream("ysb", input), "failed to create logical stream ysb");
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);

    auto ysbSchema = Schema::create()
                         ->addField("ysb$user_id", UINT64)
                         ->addField("ysb$page_id", UINT64)
                         ->addField("ysb$campaign_id", UINT64)
                         ->addField("ysb$ad_type", UINT64)
                         ->addField("ysb$event_type", UINT64)
                         ->addField("ysb$current_ms", UINT64)
                         ->addField("ysb$ip", UINT64)
                         ->addField("ysb$d1", UINT64)
                         ->addField("ysb$d2", UINT64)
                         ->addField("ysb$d3", UINT32)
                         ->addField("ysb$d4", UINT16);

    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct __attribute__((packed)) YsbRecord {
            YsbRecord() = default;
            YsbRecord(uint64_t userId,
                      uint64_t pageId,
                      uint64_t campaignId,
                      uint64_t adType,
                      uint64_t eventType,
                      uint64_t currentMs,
                      uint64_t ip)
                : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType),
                  currentMs(currentMs), ip(ip) {}

            uint64_t userId{};
            uint64_t pageId{};
            uint64_t campaignId{};
            uint64_t adType{};
            uint64_t eventType{};
            uint64_t currentMs{};
            uint64_t ip{};

            // placeholder to reach 78 bytes
            uint64_t dummy1{0};
            uint64_t dummy2{0};
            uint32_t dummy3{0};
            uint16_t dummy4{0};

            YsbRecord(const YsbRecord& rhs) {
                userId = rhs.userId;
                pageId = rhs.pageId;
                campaignId = rhs.campaignId;
                adType = rhs.adType;
                eventType = rhs.eventType;
                currentMs = rhs.currentMs;
                ip = rhs.ip;
            }
            [[nodiscard]] std::string toString() const {
                return "YsbRecord(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId)
                    + ", campaignId=" + std::to_string(campaignId) + ", adType=" + std::to_string(adType) + ", eventType="
                    + std::to_string(eventType) + ", currentMs=" + std::to_string(currentMs) + ", ip=" + std::to_string(ip);
            }
        };

        auto* records = buffer.getBuffer<YsbRecord>();
        auto ts =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();

        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            //                    memset(&records, 0, sizeof(YsbRecord));
            records[u].userId = 1;
            records[u].pageId = 0;
            records[u].adType = 0;
            records[u].campaignId = rand() % 10000;
            records[u].eventType = u % 3;
            records[u].currentMs = ts;
            records[u].ip = 0x01020304;
        }
        NES_WARNING("Lambda last entry is=" << records[numberOfTuplesToProduce - 1].toString());
    };

    auto lambdaSourceType = LambdaSourceType::create(func, 10, 100, "frequency");
    auto physicalSource = PhysicalSource::create("ysb", "YSB_phy", lambdaSourceType);
    workerConfig->addPhysicalSource(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "ysb.out";
    NES::AbstractPhysicalStreamConfigPtr conf =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "YSB_phy", "ysb", func, 10, 100, "frequency");

    wrk1->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"ysb\").window(TumblingWindow::of(EventTime(Attribute(\"current_ms\")), "
                   "Milliseconds(10))).byKey(Attribute(\"campaign_id\")).apply(Sum(Attribute(\"user_id\"))).sink("
                   "FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(1, outputFilePath));

    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    //here we can only check if the file exists and has some content

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testCentralWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 1},
                                          {2000, 3000, 1, 2},
                                          {1000, 2000, 4, 1},
                                          {2000, 3000, 11, 2},
                                          {1000, 2000, 12, 1},
                                          {2000, 3000, 16, 2}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testCentralWindowEventTimeWithTimeUnit) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp"), Seconds()), Minutes(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{960000, 1020000, 1, 1},
                                          {1980000, 2040000, 1, 2},
                                          {960000, 1020000, 4, 1},
                                          {1980000, 2040000, 11, 2},
                                          {960000, 1020000, 12, 1},
                                          {1980000, 2040000, 16, 2}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralSlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();


    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 1, 51},
                                          {5000, 15000, 1, 95},
                                          {10000, 20000, 1, 145},
                                          {0, 10000, 4, 1},
                                          {0, 10000, 11, 5},
                                          {0, 10000, 12, 1},
                                          {0, 10000, 16, 2}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedTumblingWindowQueryEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->setCoordinatorPort(rpcPort);
    auto windowStream = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getStreamCatalogService()->registerLogicalStream("window", testSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,34\n"
                             "2000,3000,2,56\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedTumblingWindowQueryEventTimeTimeUnit) {
    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts"), Seconds()), Minutes(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{960000, 1020000, 1, 34}, {1980000, 2040000, 2, 56}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed sliding window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerDistributedSlidingWindowQueryEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 1, 102},
                                          {5000, 15000, 1, 190},
                                          {10000, 20000, 1, 290},
                                          {0, 10000, 4, 2},
                                          {0, 10000, 11, 10},
                                          {0, 10000, 12, 2},
                                          {0, 10000, 16, 4}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(1))).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 3}, {2000, 3000, 6}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 60}, {5000, 15000, 95}, {10000, 20000, 145}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeyTumblingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(1))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 6}, {2000, 3000, 12}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeySlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 120}, {5000, 15000, 190}, {10000, 20000, 290}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testCentralWindowIngestionTimeIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(5);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowStream = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);
    workerConfig->setCoordinatorPort(rpcPort);

    //creating schema
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getStreamCatalogService()->registerLogicalStream("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();


    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testDistributedWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(5);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowStream = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);
    workerConfig->setCoordinatorPort(rpcPort);

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getStreamCatalogService()->registerLogicalStream("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();


    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(6);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowStream = PhysicalSource::create("windowStream", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);
    workerConfig->setCoordinatorPort(rpcPort);

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getStreamCatalogService()->registerLogicalStream("windowStream", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowStream = PhysicalSource::create("windowStream", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);
    workerConfig->setCoordinatorPort(rpcPort);

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getStreamCatalogService()->registerLogicalStream("windowStream", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedWithMergingTumblingWindowQueryEventTimeWithMergeAndComputeOnDifferentNodes) {
    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness
        .addLogicalSource("window", testSchema)
        .attachWorkerToCoordinator()
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 5UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 68}, {2000, 3000, 2, 112}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedWithMergingTumblingWindowQueryEventTimeWithMergeAndComputeOnSameNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getStreamCatalogService()->registerLogicalStream("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    // create source
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    auto windowStream = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 3");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 30);
    workerConfig->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(workerConfig);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);

    EXPECT_TRUE(retStart3);
    NES_INFO("WindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 4");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 40);
    workerConfig->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(workerConfig);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 5");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 60);
    workerConfig->setDataPort(port + 61);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(workerConfig);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("WindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,68\n"
                             "2000,3000,2,112\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("WindowDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("WindowDeploymentTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithDoubleKey) {
    struct Car {
        double key;
        uint64_t value1;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createDouble())
                         ->addField("value1", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value1"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
        .addLogicalSource("car", carSchema)
        .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1.2, 2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1.5, 4, 4, 1500}, 2);
    testHarness.pushElement<Car>({1.7, 5, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        double key;
        uint64_t value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1.2, 2}, {1000, 2000, 1.5, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatKey) {
    struct Car {
        float key;
        uint32_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFloat())
                         ->addField("value1", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value1"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1.2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1.5, 4, 1500}, 2);
    testHarness.pushElement<Car>({1.7, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        float key;
        uint32_t value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1.2, 2}, {1000, 2000, 1.5, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithBoolKey) {
    struct Car {
        bool key;
        std::array<char, 3> value1;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createBoolean())
                         ->addField("value1", DataTypeFactory::createFixedChar(3))
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value2"))).project(Attribute("value2")))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::array<char, 3> charArrayValue = {'A', 'B', 'C'};
    testHarness.pushElement<Car>({true, charArrayValue, 2, 1000}, 2);
    testHarness.pushElement<Car>({false, charArrayValue, 4, 1500}, 2);
    testHarness.pushElement<Car>({true, charArrayValue, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{2}, {4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWitCharKey) {
    struct Car {
        char key;
        std::array<char, 3> value1;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createChar())
                         ->addField("value1", DataTypeFactory::createFixedChar(3))
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value2"))).project(Attribute("value2")))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::array<char, 3> charArrayValue = {'A', 'B', 'C'};
    testHarness.pushElement<Car>({'A', charArrayValue, 2, 1000}, 2);
    testHarness.pushElement<Car>({'B', charArrayValue, 4, 1500}, 2);
    testHarness.pushElement<Car>({'C', charArrayValue, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{2}, {4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFixedChar) {
    struct Car {
        NES::ExecutableTypes::Array<char, 4> key;
        uint32_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFixedChar(4))
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    NES::ExecutableTypes::Array<char, 4> keyOne = "aaa";
    NES::ExecutableTypes::Array<char, 4> keyTwo = "bbb";
    NES::ExecutableTypes::Array<char, 4> keyThree = "ccc";

    testHarness.pushElement<Car>({keyOne, 2, 1000}, 2);
    testHarness.pushElement<Car>({keyTwo, 4, 1500}, 2);
    testHarness.pushElement<Car>({keyThree, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        std::array<char, 4> key;
        uint32_t value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, keyOne, 2}, {1000, 2000, keyTwo, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the avg aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithAvgAggregation) {
    struct Car {
        uint64_t key;
        uint64_t value1;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value1", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Avg(Attribute("value1"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1, 4, 4, 1500}, 2);
    testHarness.pushElement<Car>({1, 5, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        double value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 3}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the max aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregation) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Max(Attribute("value"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 15, 1000}, 2);
    testHarness.pushElement<Car>({1, 99, 1500}, 2);
    testHarness.pushElement<Car>({1, 20, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 99}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the max aggregation of negative values can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithNegativeValues) {
    struct Car {
        int32_t key;
        int32_t value;
        int64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createInt32())
                         ->addField("value", DataTypeFactory::createInt32())
                         ->addField("timestamp", DataTypeFactory::createInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, -15, 1000}, 2);
    testHarness.pushElement<Car>({1, -99, 1500}, 2);
    testHarness.pushElement<Car>({1, -20, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int32_t key;
        int32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, -15}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the max aggregation with uint64 data type can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithUint64AggregatedField) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("value", DataTypeFactory::createUInt64())
                         ->addField("id", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("id")).apply(Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setSkipHeader(false);

    testHarness
        .addLogicalSource("car", carSchema)
        .attachWorkerWithCSVSourceToCoordinator("car", sourceConfig)
        .validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput =
        {{0, 10000, 1, 9}, {10000, 20000, 1, 19}, {0, 10000, 4, 1}, {0, 10000, 11, 3}, {0, 10000, 12, 1}, {0, 10000, 16, 2}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the min aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMinAggregation) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Min(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 15, 1000}, 2);
    testHarness.pushElement<Car>({1, 99, 1500}, 2);
    testHarness.pushElement<Car>({1, 20, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the min aggregation with float data type can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatMinAggregation) {
    struct Car {
        uint32_t key;
        float value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createFloat())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Min(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 15.0, 1000}, 2);
    testHarness.pushElement<Car>({1, 99.0, 1500}, 2);
    testHarness.pushElement<Car>({1, 20.0, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        float value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the Count aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithCountAggregation) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Count()))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t count;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && count == rhs.count && start == rhs.start && end == rhs.end);
        }
    };
    auto outputsize = sizeof(Output);
    NES_DEBUG(outputsize);
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 2}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the Median aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMedianAggregation) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Median(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1ULL, 30ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 90ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 1800ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 60ULL, 20ULL, 2000ULL}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        double median;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && median == rhs.median && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000ULL, 2000ULL, 1ULL, 30}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test aggregation with field rename
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFieldRename) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Count()->as(Attribute("Frequency"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t count;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && count == rhs.count && start == rhs.start && end == rhs.end);
        }
    };
    auto outputsize = sizeof(Output);
    NES_DEBUG(outputsize);
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 2}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES
