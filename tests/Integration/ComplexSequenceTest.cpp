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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Util/Logger.hpp>
#include <Util/TestHarness.hpp>
#include <iostream>

using namespace std;

namespace NES {

class ComplexSequenceTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ComplexSequenceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ComplexSequenceTest test class.");
    }

    void SetUp() {
        restPort = restPort + 2;
        rpcPort = rpcPort + 30;
    }

    void TearDown() { std::cout << "Tear down ComplexSequenceTest class." << std::endl; }

    uint32_t restPort = 8080;
    uint32_t rpcPort = 4000;
};

/*
 * @brief Test a query with a single window operator and a single join operator running on a single node
 */
TEST_F(ComplexSequenceTest, complexTestSingleNodeSingleWindowSingleJoin) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinOperator =
        R"(Query::from("window1")
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .windowByKey(Attribute("window1window2$key"), TumblingWindow::of(EventTime(Attribute("window1$timestamp")), Seconds(2)), Sum(Attribute("window1$id1")))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);

    testHarness.addMemorySource("window1", window1Schema, "window1");
    testHarness.addMemorySource("window2", window2Schema, "window2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);

    testHarness.pushElement<Window1>({1, 1000}, 0);
    testHarness.pushElement<Window2>({12, 1001}, 0);
    testHarness.pushElement<Window2>({4, 1002}, 0);
    testHarness.pushElement<Window2>({1, 2000}, 0);
    testHarness.pushElement<Window2>({11, 2001}, 0);
    testHarness.pushElement<Window2>({16, 2002}, 0);
    testHarness.pushElement<Window2>({1, 3000}, 0);

    testHarness.pushElement<Window2>({21, 1003}, 1);
    testHarness.pushElement<Window2>({12, 1011}, 1);
    testHarness.pushElement<Window2>({4, 1102}, 1);
    testHarness.pushElement<Window2>({4, 1112}, 1);
    testHarness.pushElement<Window2>({1, 2010}, 1);
    testHarness.pushElement<Window2>({11, 2301}, 1);
    testHarness.pushElement<Window2>({33, 3100}, 1);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{0, 2000, 4, 8}, {0, 2000, 12, 12}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
//|--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
/*
 * @brief Test a query with a single window operator and a single join operator running on a single node
 */
TEST_F(ComplexSequenceTest, complexTestDistributedNodeSingleWindowSingleJoin) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinOperator =
        R"(Query::from("window1")
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .windowByKey(Attribute("window1window2$key"), TumblingWindow::of(EventTime(Attribute("window1$timestamp")), Seconds(1)), Sum(Attribute("window1$id1")))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addMemorySource("window1", window1Schema, "window1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("window2", window2Schema, "window2", testHarness.getWorkerId(1));

    ASSERT_EQ(testHarness.getWorkerCount(), 4);

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 1);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 1);

    testHarness.pushElement<Window1>({1, 1000}, 2);
    testHarness.pushElement<Window2>({12, 1001}, 2);
    testHarness.pushElement<Window2>({4, 1002}, 2);
    testHarness.pushElement<Window2>({1, 2000}, 2);
    testHarness.pushElement<Window2>({11, 2001}, 2);
    testHarness.pushElement<Window2>({16, 2002}, 2);
    testHarness.pushElement<Window2>({1, 3000}, 2);

    testHarness.pushElement<Window2>({21, 1003}, 3);
    testHarness.pushElement<Window2>({12, 1011}, 3);
    testHarness.pushElement<Window2>({4, 1102}, 3);
    testHarness.pushElement<Window2>({4, 1112}, 3);
    testHarness.pushElement<Window2>({1, 2010}, 3);
    testHarness.pushElement<Window2>({11, 2301}, 3);
    testHarness.pushElement<Window2>({33, 3100}, 3);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 8}, {1000, 2000, 12, 12}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test a query with a multiple window operator and a multiple join operator running on a single node
 * Placement:
 * ExecutionNode(id:1, ip:127.0.0.1, topologyNodeId:1)
 *  | QuerySubPlan(queryId:1, querySubPlanId:1)
 *  |  SINK(13)
 *  |    CENTRALWINDOW(14)
 *  |      WATERMARKASSIGNER(11)
 *  |        CENTRALWINDOW(15)
 *  |          WATERMARKASSIGNER(9)
 *  |            Join(8)
 *  |              Join(5)
 *  |                WATERMARKASSIGNER(3)
 *  |                  SOURCE(18,)
 *  |                WATERMARKASSIGNER(4)
 *  |                  SOURCE(20,)
 *  |              WATERMARKASSIGNER(7)
 *  |                SOURCE(16,)
 *  |--ExecutionNode(id:4, ip:127.0.0.1, topologyNodeId:4)
 *  |  | QuerySubPlan(queryId:1, querySubPlanId:4)
 *  |  |  SINK(17)
 *  |  |    SOURCE(6,window3)
 *  |--ExecutionNode(id:2, ip:127.0.0.1, topologyNodeId:2)
 *  |  | QuerySubPlan(queryId:1, querySubPlanId:2)
 *  |  |  SINK(19)
 *  |  |    SOURCE(1,window1)
 *  |--ExecutionNode(id:3, ip:127.0.0.1, topologyNodeId:3)
 *  |  | QuerySubPlan(queryId:1, querySubPlanId:3)
 *  |  |  SINK(21)
 *  |  |    SOURCE(2,window2)
 */
TEST_F(ComplexSequenceTest, ComplexTestSingleNodeMultipleWindowsMultipleJoins) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    struct Window3 {
        uint64_t id3;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window3Schema = Schema::create()
                             ->addField("id3", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window3), window3Schema->getSchemaSizeInBytes());

    std::string queryWithJoinAndWindowOperator =
        R"(Query::from("window1")
            .project(Attribute("window1$id1"), Attribute("window1$timestamp"))
            .filter(Attribute("window1$id1")<16)
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
            .joinWith(Query::from("window3"), Attribute("id1"), Attribute("id3"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)))
            .windowByKey(Attribute("window1window2window3$key"), SlidingWindow::of(EventTime(Attribute("window2$timestamp")),Milliseconds(10),Milliseconds(5)), Sum(Attribute("window1window2$key")))
            .windowByKey(Attribute("window1window2window3$key"), TumblingWindow::of(EventTime(Attribute("window1window2window3$start")),Milliseconds(10)), Sum(Attribute("window1window2$key")))
            .map(Attribute("window1window2$key") = Attribute("window1window2$key") * 2)
        )";
    TestHarness testHarness = TestHarness(queryWithJoinAndWindowOperator, restPort, rpcPort);

    testHarness.addMemorySource("window1", window1Schema, "window1");
    testHarness.addMemorySource("window2", window2Schema, "window2");
    testHarness.addMemorySource("window3", window3Schema, "window3");

    ASSERT_EQ(testHarness.getWorkerCount(), 3);

    testHarness.pushElement<Window1>({1, 1000}, 0);
    testHarness.pushElement<Window2>({12, 1001}, 0);
    testHarness.pushElement<Window2>({4, 1002}, 0);
    testHarness.pushElement<Window2>({4, 1005}, 0);
    testHarness.pushElement<Window2>({4, 1006}, 0);
    testHarness.pushElement<Window2>({1, 2000}, 0);
    testHarness.pushElement<Window2>({11, 2001}, 0);
    testHarness.pushElement<Window2>({16, 2002}, 0);
    testHarness.pushElement<Window2>({4, 2802}, 0);
    testHarness.pushElement<Window2>({4, 3642}, 0);
    testHarness.pushElement<Window2>({1, 3000}, 0);

    testHarness.pushElement<Window2>({21, 1003}, 1);
    testHarness.pushElement<Window2>({12, 1011}, 1);
    testHarness.pushElement<Window2>({12, 1013}, 1);
    testHarness.pushElement<Window2>({12, 1015}, 1);
    testHarness.pushElement<Window2>({4, 1102}, 1);
    testHarness.pushElement<Window2>({4, 1112}, 1);
    testHarness.pushElement<Window2>({1, 2010}, 1);
    testHarness.pushElement<Window2>({11, 2301}, 1);
    testHarness.pushElement<Window2>({4, 2022}, 1);
    testHarness.pushElement<Window2>({4, 3012}, 1);
    testHarness.pushElement<Window2>({33, 3100}, 1);

    testHarness.pushElement<Window3>({4, 1013}, 2);
    testHarness.pushElement<Window3>({12, 1010}, 2);
    testHarness.pushElement<Window3>({8, 1105}, 2);
    testHarness.pushElement<Window3>({76, 1132}, 2);
    testHarness.pushElement<Window3>({19, 2210}, 2);
    testHarness.pushElement<Window3>({1, 2501}, 2);
    testHarness.pushElement<Window2>({4, 2432}, 2);
    testHarness.pushElement<Window2>({4, 3712}, 2);
    testHarness.pushElement<Window3>({45, 3120}, 2);

    // output 2 join 1 window
    struct Output {
        uint64_t window1window2window3$start;
        uint64_t window1window2window3$end;
        uint64_t window1window2window3$key;
        uint64_t window1window2$key;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2window3$start == rhs.window1window2window3$start
                    && window1window2window3$end == rhs.window1window2window3$end
                    && window1window2window3$key == rhs.window1window2window3$key
                    && window1window2$key == rhs.window1window2$key);
        }
    };

    std::vector<Output> expectedOutput = {{1090, 1100, 4, 48}, {1000, 1010, 12, 96}, {1010, 1020, 12, 192}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
//|--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
/*
 * @brief Test a query with a single window operator and a single join operator running multiple nodes
 */
TEST_F(ComplexSequenceTest, complexTestDistributedNodeMultipleWindowsMultipleJoinsWithTopDown) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    struct Window3 {
        uint64_t id3;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window3Schema = Schema::create()
                             ->addField("id3", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window3), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinAndWindowOperator =
        R"(Query::from("window1")
            .project(Attribute("window1$id1"), Attribute("window1$timestamp"))
            .filter(Attribute("window1$id1")<16)
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
            .joinWith(Query::from("window3"), Attribute("id1"), Attribute("id3"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)))
            .windowByKey(Attribute("window1window2window3$key"), SlidingWindow::of(EventTime(Attribute("window2$timestamp")),Milliseconds(10),Milliseconds(5)), Sum(Attribute("window1window2$key")))
            .windowByKey(Attribute("window1window2window3$key"), TumblingWindow::of(EventTime(Attribute("window1window2window3$start")),Milliseconds(10)), Sum(Attribute("window1window2$key")))
            .map(Attribute("window1window2$key") = Attribute("window1window2$key") * 2)
        )";
    TestHarness testHarness = TestHarness(queryWithJoinAndWindowOperator, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addMemorySource("window1", window1Schema, "window1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("window2", window2Schema, "window2", testHarness.getWorkerId(1));
    testHarness.addMemorySource("window3", window3Schema, "window3", testHarness.getWorkerId(2));

    ASSERT_EQ(testHarness.getWorkerCount(), 6);

    testHarness.getTopology()->print();

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren().size(), 3);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 1);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 1);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[2]->getChildren().size(), 1);

    testHarness.pushElement<Window1>({1, 1000}, 3);
    testHarness.pushElement<Window1>({12, 1001}, 3);
    testHarness.pushElement<Window1>({4, 1002}, 3);
    testHarness.pushElement<Window1>({4, 1005}, 3);
    testHarness.pushElement<Window1>({4, 1006}, 3);
    testHarness.pushElement<Window1>({1, 2000}, 3);
    testHarness.pushElement<Window1>({11, 2001}, 3);
    testHarness.pushElement<Window1>({16, 2002}, 3);
    testHarness.pushElement<Window1>({4, 2802}, 3);
    testHarness.pushElement<Window1>({4, 3642}, 3);
    testHarness.pushElement<Window1>({1, 3000}, 3);

    testHarness.pushElement<Window2>({21, 1003}, 4);
    testHarness.pushElement<Window2>({12, 1011}, 4);
    testHarness.pushElement<Window2>({12, 1013}, 4);
    testHarness.pushElement<Window2>({12, 1015}, 4);
    testHarness.pushElement<Window2>({4, 1102}, 4);
    testHarness.pushElement<Window2>({4, 1112}, 4);
    testHarness.pushElement<Window2>({1, 2010}, 4);
    testHarness.pushElement<Window2>({11, 2301}, 4);
    testHarness.pushElement<Window2>({4, 2022}, 4);
    testHarness.pushElement<Window2>({4, 3012}, 4);
    testHarness.pushElement<Window2>({33, 3100}, 4);

    testHarness.pushElement<Window3>({4, 1013}, 5);
    testHarness.pushElement<Window3>({12, 1010}, 5);
    testHarness.pushElement<Window3>({8, 1105}, 5);
    testHarness.pushElement<Window3>({76, 1132}, 5);
    testHarness.pushElement<Window3>({19, 2210}, 5);
    testHarness.pushElement<Window3>({1, 2501}, 5);
    testHarness.pushElement<Window3>({4, 2432}, 5);
    testHarness.pushElement<Window3>({4, 3712}, 5);
    testHarness.pushElement<Window3>({45, 3120}, 5);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{1090, 1100, 4, 48}, {1000, 1010, 12, 96}, {1010, 1020, 12, 192}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown");
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]

// Placement:
//ExecutionNode(id:1, ip:127.0.0.1, topologyNodeId:1)
//| QuerySubPlan(queryId:1, querySubPlanId:5)
//|  SINK(16)
//|    MAP(15)
//|      CENTRALWINDOW(17)
//|        WATERMARKASSIGNER(13)
//|          CENTRALWINDOW(18)
//|            WATERMARKASSIGNER(11)
//|              Join(10)
//|                SOURCE(19,)
//|                SOURCE(25,)
//|--ExecutionNode(id:2, ip:127.0.0.1, topologyNodeId:2)
//|  | QuerySubPlan(queryId:1, querySubPlanId:1)
//|  |  SINK(20)
//|  |    WATERMARKASSIGNER(9)
//|  |      SOURCE(8,window3)
//|  | QuerySubPlan(queryId:1, querySubPlanId:4)
//|  |  SINK(26)
//|  |    Join(7)
//|  |      SOURCE(21,)
//|  |      SOURCE(23,)
//|  |--ExecutionNode(id:4, ip:127.0.0.1, topologyNodeId:4)
//|  |  | QuerySubPlan(queryId:1, querySubPlanId:2)
//|  |  |  SINK(22)
//|  |  |    WATERMARKASSIGNER(6)
//|  |  |      SOURCE(4,window2)
//|  |--ExecutionNode(id:3, ip:127.0.0.1, topologyNodeId:3)
//|  |  | QuerySubPlan(queryId:1, querySubPlanId:3)
//|  |  |  SINK(24)
//|  |  |    WATERMARKASSIGNER(5)
//|  |  |      FILTER(3)
//|  |  |        PROJECTION(2, schema=window1$id1:INTEGER window1$timestamp:INTEGER )
//|  |  |          SOURCE(1,window1)

/*
 * @brief Test a query with a single window operator and a single join operator running multiple nodes
 */
TEST_F(ComplexSequenceTest, complexTestDistributedNodeMultipleWindowsMultipleJoinsWithBottomUp) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    struct Window3 {
        uint64_t id3;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window3Schema = Schema::create()
                             ->addField("id3", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window3), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinAndWindowOperator =
        R"(Query::from("window1")
            .project(Attribute("window1$id1"), Attribute("window1$timestamp"))
            .filter(Attribute("window1$id1")<16)
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
            .joinWith(Query::from("window3"), Attribute("id1"), Attribute("id3"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)))
            .windowByKey(Attribute("window1window2window3$key"), SlidingWindow::of(EventTime(Attribute("window2$timestamp")),Milliseconds(10),Milliseconds(5)), Sum(Attribute("window1window2$key")))
            .windowByKey(Attribute("window1window2window3$key"), TumblingWindow::of(EventTime(Attribute("window1window2window3$start")),Milliseconds(10)), Sum(Attribute("window1window2$key")))
            .map(Attribute("window1window2$key") = Attribute("window1window2$key") * 2)
        )";
    TestHarness testHarness = TestHarness(queryWithJoinAndWindowOperator, restPort, rpcPort);
    testHarness.addNonSourceWorker();                                                            //wrk0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));                                  //wrk1
    testHarness.addMemorySource("window3", window3Schema, "window3", testHarness.getWorkerId(0));//wrk2
    testHarness.addMemorySource("window1", window1Schema, "window1", testHarness.getWorkerId(1));//wrk3
    testHarness.addMemorySource("window2", window2Schema, "window2", testHarness.getWorkerId(1));//wrk4

    ASSERT_EQ(testHarness.getWorkerCount(), 5);

    testHarness.getTopology()->print();

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren().size(), 1);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren()[1]->getChildren().size(), 0);

    testHarness.pushElement<Window1>({1, 1000}, 3);
    testHarness.pushElement<Window1>({12, 1001}, 3);
    testHarness.pushElement<Window1>({4, 1002}, 3);
    testHarness.pushElement<Window1>({4, 1005}, 3);
    testHarness.pushElement<Window1>({4, 1006}, 3);
    testHarness.pushElement<Window1>({1, 2000}, 3);
    testHarness.pushElement<Window1>({11, 2001}, 3);
    testHarness.pushElement<Window1>({16, 2002}, 3);
    testHarness.pushElement<Window1>({4, 2802}, 3);
    testHarness.pushElement<Window1>({4, 3642}, 3);
    testHarness.pushElement<Window1>({1, 3000}, 3);

    testHarness.pushElement<Window2>({21, 1003}, 4);
    testHarness.pushElement<Window2>({12, 1011}, 4);
    testHarness.pushElement<Window2>({12, 1013}, 4);
    testHarness.pushElement<Window2>({12, 1015}, 4);
    testHarness.pushElement<Window2>({4, 1102}, 4);
    testHarness.pushElement<Window2>({4, 1112}, 4);
    testHarness.pushElement<Window2>({1, 2010}, 4);
    testHarness.pushElement<Window2>({11, 2301}, 4);
    testHarness.pushElement<Window2>({4, 2022}, 4);
    testHarness.pushElement<Window2>({4, 3012}, 4);
    testHarness.pushElement<Window2>({33, 3100}, 4);

    testHarness.pushElement<Window3>({4, 1013}, 2);
    testHarness.pushElement<Window3>({12, 1010}, 2);
    testHarness.pushElement<Window3>({8, 1105}, 2);
    testHarness.pushElement<Window3>({76, 1132}, 2);
    testHarness.pushElement<Window3>({19, 2210}, 2);
    testHarness.pushElement<Window3>({1, 2501}, 2);
    testHarness.pushElement<Window3>({4, 2432}, 2);
    testHarness.pushElement<Window3>({4, 3712}, 2);
    testHarness.pushElement<Window3>({45, 3120}, 2);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{1090, 1100, 4, 48}, {1000, 1010, 12, 96}, {1010, 1020, 12, 192}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES
