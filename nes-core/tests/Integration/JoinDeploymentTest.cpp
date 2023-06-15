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

#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <NesBaseTest.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>

namespace NES::Runtime::Execution {

class JoinDeploymentTest : public Testing::TestWithErrorHandling,
                           public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("QueryExecutionTest: Setup JoinDeploymentTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO2("QueryExecutionTest: Setup JoinDeploymentTest test class.");
        Testing::TestWithErrorHandling::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO2("QueryExecutionTest: Tear down JoinDeploymentTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("QueryExecutionTest: Tear down JoinDeploymentTest test class."); }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

std::istream& operator>>(std::istream& is, std::string& l) {
    std::getline(is, l);
    return is;
}

Runtime::MemoryLayouts::DynamicTupleBuffer fillBuffer(const std::string& csvFileName,
                                                      Runtime::MemoryLayouts::DynamicTupleBuffer buffer,
                                                      const SchemaPtr schema,
                                                      BufferManagerPtr bufferManager) {

    auto fullPath = std::string(TEST_DATA_DIRECTORY) + csvFileName;
    NES_DEBUG("read file=" << fullPath);
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(fullPath)), "File " << fullPath << " does not exist!!!");
    const std::string delimiter = ",";
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);

    std::ifstream inputFile(fullPath);
    std::istream_iterator<std::string> beginIt(inputFile);
    std::istream_iterator<std::string> endIt;
    auto tupleCount = 0;
    for (auto it = beginIt; it != endIt; ++it) {
        std::string line = *it;
        parser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, bufferManager);
        tupleCount++;
    }
    buffer.setNumberOfTuples(tupleCount);
    return buffer;
}

/**
* Test deploying join with same data and same schema
 * */
TEST_P(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$value", BasicType::UINT64)
                                ->addField("test1$id", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$value", BasicType::UINT64)
                                 ->addField("test2$id", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id";
    const auto joinFieldNameRight = "test2$id";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window.csv");
    const std::string fileNameBuffersSink("window_sink.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);

    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$value1", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$value2", BasicType::UINT64)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window.csv");
    const std::string fileNameBuffersSink("window_sink.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$value1", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$value2", BasicType::UINT64)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersSink("window_sink2.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$win", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window3.csv");
    const std::string fileNameBuffersSink("window_sink3.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
// TODO this test can be enabled once #3353 is merged
TEST_P(JoinDeploymentTest, DISABLED_testJoinWithDifferentSourceSlidingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$win1", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$win2", BasicType::UINT64)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersSink("window_sink5.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 40);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query =
        TestQuery::from(testSourceDescriptorLeft)
            .joinWith(TestQuery::from(testSourceDescriptorRight))
            .where(Attribute(joinFieldNameLeft))
            .equalsTo(Attribute(joinFieldNameRight))
            .window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
            .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 40);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
// TODO this test can be enabled once #3353 is merged
TEST_P(JoinDeploymentTest, DISABLED_testSlidingWindowDifferentAttributes) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$win", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window3.csv");
    const std::string fileNameBuffersSink("window_sink6.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 40);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query =
        TestQuery::from(testSourceDescriptorLeft)
            .joinWith(TestQuery::from(testSourceDescriptorRight))
            .where(Attribute(joinFieldNameLeft))
            .equalsTo(Attribute(joinFieldNameRight))
            .window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
            .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 40);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * @brief Test a join query that uses fixed-array as keys
 */
// TODO this test can be enabled once #3638 is merged
TEST_P(JoinDeploymentTest, DISABLED_testJoinWithFixedCharKey) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$id1", BasicType::TEXT)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::TEXT)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window5.csv");
    const std::string fileNameBuffersRight("window6.csv");
    const std::string fileNameBuffersSink("window_sink4.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 2);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .project(Attribute("test1test2$start"),
                              Attribute("test1test2$end"),
                              Attribute("test1test2$key"),
                              Attribute(timeStampField))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.getBuffer().setWatermark(1000);
    leftBuffer.getBuffer().setOriginId(2);
    leftBuffer.getBuffer().setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.getBuffer().setWatermark(1000);
    rightBuffer.getBuffer().setOriginId(3);
    rightBuffer.getBuffer().setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2);
    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

INSTANTIATE_TEST_CASE_P(testJoinQueries,
                        JoinDeploymentTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<JoinDeploymentTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
}// namespace NES::Runtime::Execution
