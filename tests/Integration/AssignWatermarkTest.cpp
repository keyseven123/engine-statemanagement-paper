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
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class AssignWatermarkTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("AssignWatermarkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AssignWatermarkTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down AssignWatermarkTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

/*
 * @brief test event time watermark for central tumbling window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentCentralTumblingWindow) {
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    // register physical stream with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window-out-of-order.csv", 1, 3,
                                                                4, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testWatermarkAssignmentCentralTumblingWindow.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,12\n"
                             "2000,3000,1,24\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for distributed tumbling window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentDistributedTumblingWindow) {
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker 1 started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AssignWatermarkTest: Worker 2 started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AssignWatermarkTest: Worker 3 started successfully");

    std::string outputFilePath = "testWatermarkAssignmentDistributedTumblingWindow.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("AssignWatermarkTest: Submit query");

    //register logical stream
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    // register physical stream with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window-out-of-order.csv", 1, 3,
                                                                4, "test_stream", "window", false);
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);
    wrk3->registerPhysicalStream(conf);

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,36\n"
                             "2000,3000,1,72\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for central sliding window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentCentralSlidingWindow) {
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    // register physical stream with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window-out-of-order.csv", 1, 3,
                                                                4, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testWatermarkAssignmentCentralSlidingWindow.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query =
        "Query::from(\"window\")"
        ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
        "Milliseconds()))"
        ".windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1),Milliseconds(500)), "
        "Sum(Attribute(\"value\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "2500,3500,1,10\n"
                             "2000,3000,1,24\n"
                             "1500,2500,1,30\n"
                             "1000,2000,1,12\n"
                             "500,1500,1,6\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for distributed sliding window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentDistributedSlidingWindow) {
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker 1 started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AssignWatermarkTest: Worker 2 started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AssignWatermarkTest: Worker 3 started successfully");

    std::string outputFilePath = "testWatermarkAssignmentDistributedSlidingWindow.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("AssignWatermarkTest: Submit query");

    //register logical stream
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    // register physical stream with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window-out-of-order.csv", 1, 3,
                                                                4, "test_stream", "window", false);
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);
    wrk3->registerPhysicalStream(conf);
    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query =
        "Query::from(\"window\")"
        ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50),Milliseconds()))"
        ".windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1),Milliseconds(500)), "
        "Sum(Attribute(\"value\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "500,1500,1,18\n"
                             "1000,2000,1,36\n"
                             "1500,2500,1,90\n"
                             "2000,3000,1,72\n"
                             "2500,3500,1,30\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

}// namespace NES
