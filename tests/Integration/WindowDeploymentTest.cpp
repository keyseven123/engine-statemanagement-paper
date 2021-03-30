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

#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gmock/gmock.h>
#include <iostream>

using namespace std;

namespace NES {

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

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down WindowDeploymentTest class." << std::endl; }
};

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerCentralTumblingWindowQueryEventTimeForExdra) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/exdra.csv");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("exdra");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testDeployOneWorkerCentralTumblingWindowQueryEventTimeForExdra.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query =
        "Query::from(\"exdra\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"metadata_generated\")), "
        "Seconds(10)), Sum(Attribute(\"features_properties_capacity\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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

TEST_F(WindowDeploymentTest, testCentralWindowEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query =
        "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
        "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,1\n"
                             "2000,3000,1,2\n"
                             "1000,2000,4,1\n"
                             "2000,3000,11,2\n"
                             "1000,2000,12,1\n"
                             "2000,3000,16,2\n";

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

TEST_F(WindowDeploymentTest, testCentralWindowEventTimeWithTimeUnit) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), "
                   "TumblingWindow::of(EventTime(Attribute(\"timestamp\"), Seconds()), "
                   "Minutes(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "960000,1020000,1,1\n"
                             "1980000,2040000,1,2\n"
                             "960000,1020000,4,1\n"
                             "1980000,2040000,11,2\n"
                             "960000,1020000,12,1\n"
                             "1980000,2040000,16,2\n";

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
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralSlidingWindowEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), "
                   "SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), "
                   "Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("wakeup");

    ifstream my_file("outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "10000,20000,1,870\n"
                             "5000,15000,1,570\n"
                             "0,10000,1,307\n"
                             "0,10000,4,6\n"
                             "0,10000,11,30\n"
                             "0,10000,12,7\n"
                             "0,10000,16,12\n";

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
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedTumblingWindowQueryEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query =
        "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\"), Seconds()), "
        "Minutes(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "960000,1020000,1,34\n"
                             "1980000,2040000,2,56\n";

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
 * @brief test distributed sliding window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerDistributedSlidingWindowQueryEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), "
                   "SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), "
                   "Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    ifstream my_file("outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,614\n"
                             "5000,15000,1,1140\n"
                             "10000,20000,1,1740\n"
                             "0,10000,4,12\n"
                             "0,10000,11,60\n"
                             "0,10000,12,14\n"
                             "0,10000,16,24\n";

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
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("windowStream");

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "windowStream$start:INTEGER,windowStream$end:INTEGER,windowStream$value:INTEGER\n"
                             "1000,2000,3\n"
                             "2000,3000,6\n";

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
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)),"
                   " Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("wakeup");

    ifstream my_file("outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$value:INTEGER\n"
                             "10000,20000,870\n"
                             "5000,15000,570\n"
                             "0,10000,362\n";

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
TEST_F(WindowDeploymentTest, testDistributedNonKeyTumblingWindowEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("windowStream");

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "windowStream$start:INTEGER,windowStream$end:INTEGER,windowStream$value:INTEGER\n"
                             "1000,2000,6\n"
                             "2000,3000,12\n";

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
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeySlidingWindowEventTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    //size slide
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)),"
                   " Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$value:INTEGER\n"
                             "0,10000,724\n"
                             "5000,15000,1140\n"
                             "10000,20000,1740\n";

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

TEST_F(WindowDeploymentTest, testCentralWindowIngestionTimeIngestionTime) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(5);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(5);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(6);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("windowStream");

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("windowStream");

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 3");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 30);
    workerConfig->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);

    EXPECT_TRUE(retStart3);
    NES_INFO("WindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 4");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 40);
    workerConfig->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 5");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 60);
    workerConfig->setDataPort(port + 61);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("WindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    wrk2->registerPhysicalStream(conf);
    wrk3->registerPhysicalStream(conf);
    wrk4->registerPhysicalStream(conf);
    wrk5->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedWithMergingTumblingWindowQueryEventTimeWithMergeAndComputeOnSameNodes) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 3");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 30);
    workerConfig->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(workerConfig, NodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);

    EXPECT_TRUE(retStart3);
    NES_INFO("WindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 4");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 40);
    workerConfig->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(workerConfig, NodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 5");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 60);
    workerConfig->setDataPort(port + 61);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(workerConfig, NodeType::Worker);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("WindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    //    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);
    wrk3->registerPhysicalStream(conf);
    wrk4->registerPhysicalStream(conf);
    wrk5->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
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
        R"(Query::from("car").windowByKey(Attribute("key"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    testHarness.addMemorySource("car", carSchema, "car1");

    ASSERT_EQ(testHarness.getWorkerCount(), 1);

    testHarness.pushElement<Car>({1, 15, 1000}, 0);
    testHarness.pushElement<Car>({1, 99, 1500}, 0);
    testHarness.pushElement<Car>({1, 20, 2000}, 0);

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
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

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
        R"(Query::from("car").windowByKey(Attribute("key"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    testHarness.addMemorySource("car", carSchema, "car1");

    ASSERT_EQ(testHarness.getWorkerCount(), 1);

    testHarness.pushElement<Car>({1, -15, 1000}, 0);
    testHarness.pushElement<Car>({1, -99, 1500}, 0);
    testHarness.pushElement<Car>({1, -20, 2000}, 0);

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
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

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
        R"(Query::from("car").windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)), Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setLogicalStreamName("car");
    sourceConfig->setPhysicalStreamName("car");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setSkipHeader(false);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    testHarness.addCSVSource(conf, carSchema);

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

    std::vector<Output> expectedOutput = {{0, 10000, 1, 9},  {10000, 20000, 1, 19}, {0, 10000, 4, 1},
                                          {0, 10000, 11, 3}, {0, 10000, 12, 1},     {0, 10000, 16, 2}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

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
        R"(Query::from("car").windowByKey(Attribute("key"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Min(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    testHarness.addMemorySource("car", carSchema, "car1");

    ASSERT_EQ(testHarness.getWorkerCount(), 1);

    testHarness.pushElement<Car>({1, 15, 1000}, 0);
    testHarness.pushElement<Car>({1, 99, 1500}, 0);
    testHarness.pushElement<Car>({1, 20, 2000}, 0);

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
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

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
        R"(Query::from("car").windowByKey(Attribute("key"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Min(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    testHarness.addMemorySource("car", carSchema, "car1");

    ASSERT_EQ(testHarness.getWorkerCount(), 1);

    testHarness.pushElement<Car>({1, 15.0, 1000}, 0);
    testHarness.pushElement<Car>({1, 99.0, 1500}, 0);
    testHarness.pushElement<Car>({1, 20.0, 2000}, 0);

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
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

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
        R"(Query::from("car").windowByKey(Attribute("key"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Count()))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort);

    testHarness.addMemorySource("car", carSchema, "car1");

    ASSERT_EQ(testHarness.getWorkerCount(), 1);

    testHarness.pushElement<Car>({1, 15, 15, 1000}, 0);
    testHarness.pushElement<Car>({1, 99, 88, 1500}, 0);
    testHarness.pushElement<Car>({1, 20, 20, 2000}, 0);

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
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES
