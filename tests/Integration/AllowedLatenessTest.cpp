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

class AllowedLatenessTest : public testing::Test {
  public:
    PhysicalStreamConfigPtr outOfOrderConf;
    PhysicalStreamConfigPtr inOrderConf;
    SchemaPtr inputSchema;

    static void SetUpTestCase() {
        NES::setupLogging("AllowedLatenessTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AllowedLatenessTest test class.");
    }

    void SetUp() {
        // window-out-of-order.csv contains 12 rows
        SourceConfigPtr outOfOrderSourceConfig = SourceConfig::create();
        outOfOrderSourceConfig->setSourceType("CSVSource");
        outOfOrderSourceConfig->setSourceConfig("../tests/test_data/window-out-of-order.csv");
        outOfOrderSourceConfig->setSourceFrequency(1);
        outOfOrderSourceConfig->setNumberOfTuplesToProducePerBuffer(2);
        outOfOrderSourceConfig->setNumberOfBuffersToProduce(6);
        outOfOrderSourceConfig->setPhysicalStreamName("outOfOrderPhysicalSource");
        outOfOrderSourceConfig->setLogicalStreamName("OutOfOrderStream");
        outOfOrderSourceConfig->setSkipHeader(false);

        outOfOrderConf = PhysicalStreamConfig::create(outOfOrderSourceConfig);

        SourceConfigPtr inOrderSourceConfig = SourceConfig::create();
        // window-out-of-order.csv contains 12 rows
        inOrderSourceConfig = SourceConfig::create();
        inOrderSourceConfig->setSourceType("CSVSource");
        inOrderSourceConfig->setSourceConfig("../tests/test_data/window-in-order.csv");
        inOrderSourceConfig->setSourceFrequency(1);
        inOrderSourceConfig->setNumberOfTuplesToProducePerBuffer(2);
        inOrderSourceConfig->setNumberOfBuffersToProduce(6);
        inOrderSourceConfig->setPhysicalStreamName("inOrderPhysicalSource");
        inOrderSourceConfig->setLogicalStreamName("inOrderStream");
        inOrderSourceConfig->setSkipHeader(false);

        inOrderConf = PhysicalStreamConfig::create(inOrderSourceConfig);

        restPort = restPort + 2;
        rpcPort = rpcPort + 30;

        inputSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());
    }

    void TearDown() { std::cout << "Tear down AllowedLatenessTest class." << std::endl; }

    std::string testName = "AllowedLatenessTest";

    struct Output {
        uint64_t _$start;
        uint64_t _$end;
        uint64_t window$id;
        uint64_t window$value;

        bool operator==(Output const& rhs) const {
            return (_$start == rhs._$start && _$end == rhs._$end && window$id == rhs.window$id
                    && window$value == rhs.window$value);
        }
    };

    uint32_t restPort = 8080;
    uint32_t rpcPort = 4000;
};

// Test name abbreviations
// SPS: Single Physical Source
// MPS: Multiple Physical Sources
// FT: Flat Topology
// HT: Hierarchical Topology
// IO: In Order
// OO: Out of Order
/*
 * @brief Test allowed lateness using single source, flat topology, in-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_IO_0ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(inOrderConf, inputSchema);

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}, {2000, 3000, 1, 30}, {3000, 4000, 1, 21}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, in-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_IO_10ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(inOrderConf, inputSchema);

    // with allowed lateness=10, the 3000-4000 is closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}, {2000, 3000, 1, 30}, {3000, 4000, 1, 21}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, in-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_IO_250ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(inOrderConf, inputSchema);

    // with allowed lateness=250, the 3000-4000 window is not yet closed and up to {5,1,1990} included to the 1000-2000 window
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}, {2000, 3000, 1, 30}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, out-of-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_OO_0ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);

    // with allowed lateness = 0, {6,1,1990} is not included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 6}, {2000, 3000, 1, 24}, {3000, 4000, 1, 22}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, out-of-order stream with 50ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_OO_10ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);

    // with allowed lateness = 10, {6,1,1990} is included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 12}, {2000, 3000, 1, 24}, {3000, 4000, 1, 22}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, out-of-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_OO_250ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);

    // with allowed lateness=250, {9,1,1900} included in 1000-2000 window, while the 3000-4000 window is not yet closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 21}, {2000, 3000, 1, 24}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, in-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_IO_0ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(inOrderConf, inputSchema);
    testHarness.addCSVSource(inOrderConf, inputSchema);
    testHarness.addCSVSource(inOrderConf, inputSchema);

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 45}, {2000, 3000, 1, 90}, {3000, 4000, 1, 63}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, in-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_IO_10ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(inOrderConf, inputSchema);
    testHarness.addCSVSource(inOrderConf, inputSchema);
    testHarness.addCSVSource(inOrderConf, inputSchema);

    // with allowed lateness=10, the 3000-4000 is closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 45}, {2000, 3000, 1, 90}, {3000, 4000, 1, 63}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, in-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_IO_250ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(inOrderConf, inputSchema);
    testHarness.addCSVSource(inOrderConf, inputSchema);
    testHarness.addCSVSource(inOrderConf, inputSchema);

    // with allowed lateness=250, the 3000-4000 window is not yet closed and up to {5,1,1990} included to the 1000-2000 window
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 45}, {2000, 3000, 1, 90}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, out-of-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_OO_0ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);

    // with allowed lateness = 0, {6,1,1990} is not included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 18}, {2000, 3000, 1, 72}, {3000, 4000, 1, 66}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, out-of-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_OO_10ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);

    // with allowed lateness = 10, {6,1,1990} is included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 36}, {2000, 3000, 1, 72}, {3000, 4000, 1, 66}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, out-of-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_OO_250ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);
    testHarness.addCSVSource(outOfOrderConf, inputSchema);

    // with allowed lateness=250, {9,1,1900} included in 1000-2000 window, while the 3000-4000 window is not yet closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 63}, {2000, 3000, 1, 72}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//----Test with Hierarchical Topology----//
//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
//|  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, in-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_IO_0ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(1));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(1));

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2);

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 60}, {2000, 3000, 1, 120}, {3000, 4000, 1, 84}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, in-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_IO_10ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(1));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(1));

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2);

    // with allowed lateness=10, the 3000-4000 is closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 60}, {2000, 3000, 1, 120}, {3000, 4000, 1, 84}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, in-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_IO_250ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(1));
    testHarness.addCSVSource(inOrderConf, inputSchema, testHarness.getWorkerId(1));

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2);

    // with allowed lateness=250, the 3000-4000 window is not yet closed and up to {5,1,1990} included to the 1000-2000 window
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 60}, {2000, 3000, 1, 120}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, out-of-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_OO_0ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(1));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(1));

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2);

    // with allowed lateness = 0, {6,1,1990} is not included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 24}, {2000, 3000, 1, 96}, {3000, 4000, 1, 88}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, out-of-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_OO_10ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(1));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(1));

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2);

    // with allowed lateness = 10, {6,1,1990} is included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 48}, {2000, 3000, 1, 96}, {3000, 4000, 1, 88}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, out-of-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_OO_250ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)), "
                   "Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(1));
    testHarness.addCSVSource(outOfOrderConf, inputSchema, testHarness.getWorkerId(1));

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2);

    // with allowed lateness=250, {9,1,1900} included in 1000-2000 window, while the 3000-4000 window is not yet closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 84}, {2000, 3000, 1, 96}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
