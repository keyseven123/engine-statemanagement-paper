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
// clang-format: off
// clang-format: on
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class MapQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test>,
                              public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup QueryCatalogServiceTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler, dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down MapQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MapQueryExecutionTest: Tear down QueryExecutionTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        buf[recordIndex][0].write<int64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(1);
    }
    buf.setNumberOfTuples(10);
}

TEST_P(MapQueryExecutionTest, MapQueryArithmetic) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(schema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).map(Attribute("id") = Attribute("id") * 2).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex * 2);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1LL);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapLogarithmicFunctions) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64)
                            ->addField("test$log10", BasicType::FLOAT64)
                            ->addField("test$log2", BasicType::FLOAT64)
                            ->addField("test$ln", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("log10") = LOG10(Attribute("id")))
                     .map(Attribute("log2") = LOG2(Attribute("id")))
                     .map(Attribute("ln") = LN(Attribute("id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<double>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$log10"].read<double>(), std::log10(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$log2"].read<double>(), std::log2(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$ln"].read<double>(), std::log(recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

INSTANTIATE_TEST_CASE_P(testMapQueries,
                        MapQueryExecutionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<MapQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
