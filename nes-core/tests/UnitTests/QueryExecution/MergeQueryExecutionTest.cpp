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

class MergeQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test>,
                                public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MergeQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup MergeQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down MergeQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down MergeQueryExecutionTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    int numberOfTuples = 10;
    for (int recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
        buf[recordIndex][0].write<int64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(1);
        buf[recordIndex][2].write<int64_t>(42);
    }
    buf.setNumberOfTuples(numberOfTuples);
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
TEST_P(MergeQueryExecutionTest, mergeQuery) {
    auto schema = Schema::create()
                      ->addField("test$id", BasicType::INT64)
                      ->addField("test$one", BasicType::INT64)
                      ->addField("test$value", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto result = Schema::create()->addField("test$id", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(result, /*expected buffers*/ 2);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query1 = TestQuery::from(testSourceDescriptor);
    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5);
    auto mergedQuery = query2.unionWith(query1).project(Attribute("id")).sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(mergedQuery.getQueryPlan());

    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    auto source1 = executionEngine->getDataSource(plan, 0);
    source1->emitBuffer(inputBuffer);
    auto source2 = executionEngine->getDataSource(plan, 1);
    source2->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2u);

    for (uint64_t i = 0; i < testSink->getNumberOfResultBuffers(); i++) {
        auto resultBuffer = testSink->getResultBuffer(i);
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);
        for (uint32_t recordIndex = 0u; recordIndex < resultBuffer.getNumberOfTuples(); ++recordIndex) {
            EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        }
    }

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

INSTANTIATE_TEST_CASE_P(testMergeQueries,
                        MergeQueryExecutionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<MergeQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
