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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceStaging.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class GlobalTimeWindowPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GlobalTimeWindowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup GlobalTimeWindowPipelineTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup GlobalTimeWindowPipelineTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down GlobalTimeWindowPipelineTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down GlobalTimeWindowPipelineTest test class." << std::endl; }
};

/**
 * @brief Test running a pipeline containing a threshold window with a sum aggregation
 */
TEST_P(GlobalTimeWindowPipelineTest, windowWithSum) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    scanSchema->addField("ts", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto aggregationResultFieldName = "test$sum";
    DataTypePtr integerType = DataTypeFactory::createInt64();
    std::vector<Expressions::ExpressionPtr> aggregationFields = {readF2};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType)};
    auto slicePreAggregation =
        std::make_shared<Operators::GlobalSlicePreAggregation>(readTsField, aggregationFields, aggregationFunctions);
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);

    auto sliceMerging = std::make_shared<Operators::GlobalSliceMerging>(aggregationFunctions);
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);
    auto sliceMergingPipeline = std::make_shared<PhysicalOperatorPipeline>();
    sliceMergingPipeline->setRootOperator(sliceMerging);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[0]["ts"].write((int64_t) 1);
    dynamicBuffer[1]["f1"].write((int64_t) 2);
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[1]["ts"].write((int64_t) 1);
    dynamicBuffer[2]["f1"].write((int64_t) 3);
    dynamicBuffer[2]["f2"].write((int64_t) 30);
    dynamicBuffer[2]["ts"].write((int64_t) 2);
    dynamicBuffer[3]["f1"].write((int64_t) 1);
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer[3]["ts"].write((int64_t) 3);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto preAggExecutablePipeline = provider->create(preAggPipeline);
    auto sliceStaging = std::make_shared<Operators::GlobalSliceStaging>();
    std::vector<OriginId> origins = {0};
    auto preAggregationHandler = std::make_shared<Operators::GlobalSlicePreAggregationHandler>(10, 10, origins, sliceStaging);

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);

    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline);
    auto sliceMergingHandler = std::make_shared<Operators::GlobalSliceMergingHandler>(sliceStaging);

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);

    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);

    EXPECT_EQ(pipeline2Context.buffers.size(), 1);
    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 100);

}// namespace NES::Runtime::Execution

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        GlobalTimeWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<GlobalTimeWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
