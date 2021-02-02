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
#include <iostream>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class MergeDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MergeDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MergeDeploymentTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down MergeDeploymentTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

/**
 * Test deploying merge query with source on two different worker node using bottom up strategy.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testDeployTwoWorkerMergeUsingBottomUp) {
    NES_INFO("MergeDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingBottomUp.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest: Submit query");
    string query =
        "Query::from(\"car\").merge(Query::from(\"truck\")).sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------+";

    NES_INFO("MergeDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): content=" << content);
    NES_INFO("MergeDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("MergeDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MergeDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MergeDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MergeDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest: Test finished");
}

/**
 * Test deploying merge query with source on two different worker node using top down strategy.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testDeployTwoWorkerMergeUsingTopDown) {
    NES_INFO("MergeDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").merge(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------+";

    NES_INFO("MergeDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content=" << content);
    NES_INFO("MergeDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("MergeDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MergeDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MergeDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MergeDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest: Test finished");
}

/**
 * Test deploying merge query with source on two different worker node using top down strategy.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testDeployTwoWorkerMergeUsingTopDownWithDifferentSpeed) {
    NES_INFO("MergeDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "", 0, 0, 3, "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").merge(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------+";

    NES_INFO("MergeDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content=" << content);
    NES_INFO("MergeDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("MergeDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MergeDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MergeDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MergeDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest: Test finished");
}

/**
 * Test deploying merge query with source on two different worker node using top down strategy.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testMergeTwoDifferentStreams) {
    NES_INFO("MergeDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    std::string testSchema2 = "Schema::create()->addField(\"id\", BasicType::UINT16)->addField(\"id2\", BasicType::UINT64);";
    std::string testSchemaFileName2 = "testSchema2.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << testSchema2;
    out2.close();
    wrk2->registerLogicalStream("truck", testSchemaFileName2);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 3, "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").merge(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    cout << "queryid=" << queryId << endl;
    ASSERT_TRUE(!TestUtils::waitForQueryToStart(queryId, queryCatalog));

    NES_INFO("MergeDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MergeDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MergeDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest: Test finished");
}

/**
 * Test deploying filter-push-down on merge query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are above a merge operator and will be pushed down towards both of the available sources.
 *       2 filter operators are already below merge operator and need to be pushed down normally towards its respective source.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams) {
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string rareStonesSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT32))->addField(createField("id", BasicType::UINT32))->addField(createField("timestamp", BasicType::INT32));)";
    std::string rareStonesSchemaFileName = "rareStonesSchema.hpp";
    std::ofstream out(rareStonesSchemaFileName);
    out << rareStonesSchema;
    out.close();

    wrk1->registerLogicalStream("ruby", rareStonesSchemaFileName);
    wrk2->registerLogicalStream("diamond", rareStonesSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confStreamRuby =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 28, 1, "physical_ruby", "ruby", false);

    PhysicalStreamConfigPtr confStreamDiamond = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 28,
                                                                             1, "physical_diamond", "diamond", false);

    wrk1->registerPhysicalStream(confStreamRuby);
    wrk2->registerPhysicalStream(confStreamDiamond);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".filter(Attribute(\"id\") < 12)"
                   ".merge(Query::from(\"diamond\")"
                   ".filter(Attribute(\"value\") < 15))"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"value\") < 17)"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"value\") > 1)"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|2|1|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|1|2|\n"
                                        "|3|11|2|\n"
                                        "|3|1|2|\n"
                                        "|3|1|2|\n"
                                        "|4|1|2|\n"
                                        "|5|1|2|\n"
                                        "|6|1|2|\n"
                                        "|7|1|2|\n"
                                        "|8|1|2|\n"
                                        "|9|1|2|\n"
                                        "|10|1|2|\n"
                                        "|11|1|2|\n"
                                        "|12|1|2|\n"
                                        "|13|1|2|\n"
                                        "|14|1|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|2|1|2|\n"
                                         "|2|11|2|\n"
                                         "|3|1|2|\n"
                                         "|3|11|2|\n"
                                         "|3|1|2|\n"
                                         "|3|1|2|\n"
                                         "|4|1|2|\n"
                                         "|5|1|2|\n"
                                         "|6|1|2|\n"
                                         "|7|1|2|\n"
                                         "|8|1|2|\n"
                                         "|9|1|2|\n"
                                         "|10|1|2|\n"
                                         "|11|1|2|\n"
                                         "|12|1|2|\n"
                                         "|13|1|2|\n"
                                         "|14|1|2|\n"
                                         "|15|1|2|\n"
                                         "|16|1|2|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): content="
             << content);
    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentSubQry="
             << expectedContentSubQry);
    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentMainQry="
             << expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Test finished");
}

/**
 * Test deploying filter-push-down on merge query with source on two different worker node using top down strategy.
 * Case: 1 filter operator is above a merge operator and will be pushed down towards both of the available sources.
 *       1 filter operator is already below merge operator and needs to be pushed down normally towards its own source.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testOneFilterPushDownWithMergeOfTwoDifferentStreams) {
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testOneFilterPushDownWithMergeOfTwoDifferentStreams.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string rareStonesSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT32))->addField(createField("id", BasicType::UINT32))->addField(createField("timestamp", BasicType::INT32));)";
    std::string rareStonesSchemaFileName = "rareStonesSchema.hpp";
    std::ofstream out(rareStonesSchemaFileName);
    out << rareStonesSchema;
    out.close();

    wrk1->registerLogicalStream("ruby", rareStonesSchemaFileName);
    wrk2->registerLogicalStream("diamond", rareStonesSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confStreamRuby =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 28, 1, "physical_ruby", "ruby", false);

    PhysicalStreamConfigPtr confStreamDiamond = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 28,
                                                                             1, "physical_diamond", "diamond", false);

    wrk1->registerPhysicalStream(confStreamRuby);
    wrk2->registerPhysicalStream(confStreamDiamond);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".merge(Query::from(\"diamond\")"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"id\") > 3))"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"id\") > 4)"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|1|12|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|11|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|1|12|2|\n"
                                         "|2|11|2|\n"
                                         "|2|16|2|\n"
                                         "|3|11|2|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("MergeDeploymentTest(testOneFilterPushDownWithMergeOfTwoDifferentStreams): content=" << content);
    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentSubQry="
             << expectedContentSubQry);
    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentMainQry="
             << expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Test finished");
}

/**
 * Test deploying filter-push-down on merge query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are already below merge operator and needs to be pushed down normally towards their respective source.
 *       Here the filters don't need to be pushed down over an existing merge operator.
 */
//FIXME: Enabled while solving #1467
TEST_F(MergeDeploymentTest, DISABLED_testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentStreams) {
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Coordinator started successfully");

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Worker1 started successfully");

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentStreams.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string rareStonesSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT32))->addField(createField("id", BasicType::UINT32))->addField(createField("timestamp", BasicType::INT32));)";
    std::string rareStonesSchemaFileName = "rareStonesSchema.hpp";
    std::ofstream out(rareStonesSchemaFileName);
    out << rareStonesSchema;
    out.close();

    wrk1->registerLogicalStream("ruby", rareStonesSchemaFileName);
    wrk2->registerLogicalStream("diamond", rareStonesSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confStreamRuby =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 28, 1, "physical_ruby", "ruby", false);

    PhysicalStreamConfigPtr confStreamDiamond = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 28,
                                                                             1, "physical_diamond", "diamond", false);

    wrk1->registerPhysicalStream(confStreamRuby);
    wrk2->registerPhysicalStream(confStreamDiamond);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"value\") < 9)"
                   ".merge(Query::from(\"diamond\")"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"id\") < 12)"
                   ".filter(Attribute(\"value\") < 6))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|1|1|2|\n"
                                        "|1|12|2|\n"
                                        "|1|4|2|\n"
                                        "|2|1|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|1|2|\n"
                                        "|3|11|2|\n"
                                        "|3|1|2|\n"
                                        "|3|1|2|\n"
                                        "|4|1|2|\n"
                                        "|5|1|2|\n"
                                        "|6|1|2|\n"
                                        "|7|1|2|\n"
                                        "|8|1|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|1|1|1|\n"
                                         "|1|4|1|\n"
                                         "|2|1|1|\n"
                                         "|2|11|1|\n"
                                         "|3|1|1|\n"
                                         "|3|11|1|\n"
                                         "|3|1|1|\n"
                                         "|3|1|1|\n"
                                         "|4|1|1|\n"
                                         "|5|1|1|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentStreams): content=" << content);
    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentSubQry="
             << expectedContentSubQry);
    NES_INFO("MergeDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentMainQry="
             << expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MergeDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MergeDeploymentTest For Filter-Push-Down: Test finished");
}
}// namespace NES
