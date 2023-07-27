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
#include <Util/TestHarness/TestHarness.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <NesBaseTest.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock-matchers.h>


namespace NES::Runtime::Execution {

/**
 * @brief Struct for storing all csv file params
 */
struct CsvFileParams {
    CsvFileParams(const string& csvFileLeft, const string& csvFileRight, const string& expectedFile)
        : csvFileLeft(csvFileLeft), csvFileRight(csvFileRight), expectedFile(expectedFile) {}

    const std::string csvFileLeft;
    const std::string csvFileRight;
    const std::string expectedFile;
};

/**
 * @brief Struct for storing all parameter for the join
 */
struct JoinParams {
    JoinParams(const SchemaPtr& leftSchema,
               const SchemaPtr& rightSchema,
               const string& joinFieldName)
        : leftSchema(leftSchema), rightSchema(rightSchema), joinFieldName(joinFieldName) {
        outputSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldName);
    }

    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    SchemaPtr outputSchema;
    const std::string joinFieldName;
};

/**
 * @brief Creates a schema that consists of (value, id, timestamp) with all fields being a UINT64
 * @return SchemaPtr
 */
static SchemaPtr createValueIdTimeStamp() {
    return Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                    ->addField("value", BasicType::UINT64)
                    ->addField("id", BasicType::UINT64)
                    ->addField("timestamp", BasicType::UINT64);
}

class JoinDeploymentTest : public Testing::NESBaseTest,
                           public ::testing::WithParamInterface<QueryCompilation::StreamJoinStrategy> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup JoinDeploymentTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup JoinDeploymentTest test class.");
        NESBaseTest::SetUp();

        joinStrategy = this->GetParam();
        bufferManager = std::make_shared<BufferManager>();
    }

    template<typename ResultRecord>
    void runJoinQueryTwoLogicalStreams(const Query& query, const CsvFileParams& csvFileParams, const JoinParams& joinParams) {
        auto createSourceConfig = [&](const std::string& fileName) {
            CSVSourceTypePtr sourceConfig = CSVSourceType::create();
            sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + fileName);
            sourceConfig->setGatheringInterval(0);
            sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
            sourceConfig->setNumberOfBuffersToProduce(0);
            return sourceConfig;
        };
        auto sourceConfig1 = createSourceConfig(csvFileParams.csvFileLeft);
        auto sourceConfig2 = createSourceConfig(csvFileParams.csvFileRight);
        auto expectedSinkBuffer = TestUtils::fillBufferFromCsv(csvFileParams.expectedFile, joinParams.outputSchema,
                                                               bufferManager)[0];
        auto expectedSinkVector = TestUtils::createVecFromTupleBuffer<ResultRecord>(expectedSinkBuffer);
        ASSERT_EQ(sizeof(ResultRecord), joinParams.outputSchema->getSchemaSizeInBytes());


        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .enableNautilus()
                                      .setJoinStrategy(joinStrategy)
                                      .addLogicalSource("test1", joinParams.leftSchema)
                                      .addLogicalSource("test2", joinParams.rightSchema)
                                      .attachWorkerWithCSVSourceToCoordinator("test1", sourceConfig1)
                                      .attachWorkerWithCSVSourceToCoordinator("test2", sourceConfig2)
                                      .validate().setupTopology();

        auto actualResult = testHarness.getOutput<ResultRecord>(expectedSinkBuffer.getNumberOfTuples());

        ASSERT_EQ(actualResult.size(), expectedSinkBuffer.getNumberOfTuples());
        EXPECT_THAT(actualResult, ::testing::UnorderedElementsAreArray(expectedSinkVector));
    }



    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy;
};

/**
* Test deploying join with same data and same schema
 * */
TEST_P(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };
    JoinParams joinParams(createValueIdTimeStamp(), createValueIdTimeStamp(), "id");
    CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    JoinParams joinParams(leftSchema, rightSchema, "id1");
    CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    JoinParams joinParams(leftSchema, rightSchema, "id1");
    CsvFileParams csvFileParams("window.csv", "window2.csv", "window_sink2.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp  && test2Id == rhs.test2Id && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    JoinParams joinParams(leftSchema, rightSchema, "id1");
    CsvFileParams csvFileParams("window.csv", "window3.csv", "window_sink3.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
// TODO this test can be enabled once #3353 is merged
TEST_P(JoinDeploymentTest, DISABLED_testJoinWithDifferentSourceSlidingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };

    JoinParams joinParams(createValueIdTimeStamp(), createValueIdTimeStamp(), "id");
    CsvFileParams csvFileParams("window.csv", "window2.csv", "window_sink5.csv");
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")),
                                               Milliseconds(windowSize),
                                               Milliseconds(windowSlide)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
// TODO this test can be enabled once #3353 is merged
TEST_P(JoinDeploymentTest, DISABLED_testSlidingWindowDifferentAttributes) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp  && test2Id == rhs.test2Id && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    JoinParams joinParams(leftSchema, rightSchema, "id1");
    CsvFileParams csvFileParams("window.csv", "window3.csv", "window_sink6.csv");
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")),
                                               Milliseconds(windowSize),
                                               Milliseconds(windowSlide)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * @brief Test a join query that uses fixed-array as keys
 */
// TODO this test can be enabled once #3638 is merged
TEST_P(JoinDeploymentTest, DISABLED_testJoinWithFixedCharKey) {
    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        char test1Id[7];
        uint64_t test1Timestamp;
        char test2Id[7];
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Timestamp == rhs.test1Timestamp
                && (std::strcmp(test1Id, rhs.test1Id) == 0)
                && (std::strcmp(test2Id, rhs.test2Id) == 0)
                && test2Timestamp == rhs.test2Timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$id1", BasicType::TEXT)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::TEXT)
                                 ->addField("test2$timestamp", BasicType::UINT64);
    JoinParams joinParams(leftSchema, rightSchema, "id1");
    CsvFileParams csvFileParams("window5.csv", "window6.csv", "window_sink4.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

INSTANTIATE_TEST_CASE_P(
    testJoinQueries,
    JoinDeploymentTest,
    ::testing::Values(QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN),
        //                                          TODO Enable the disabled test and fix them #3926
        //                                          QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING,
        //                                          QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE,
        //                                          QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL),
    [](const testing::TestParamInfo<JoinDeploymentTest::ParamType>& info) {
        return std::string(magic_enum::enum_name(info.param));
    });
}// namespace NES::Runtime::Execution
