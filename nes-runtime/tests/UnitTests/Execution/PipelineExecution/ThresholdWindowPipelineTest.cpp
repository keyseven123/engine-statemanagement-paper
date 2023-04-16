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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/ThresholdWindow/GlobalThresholdWindow/GlobaThresholdWindowOperatorHandler.hpp>
#include <Execution/Operators/ThresholdWindow/GlobalThresholdWindow/GlobalThresholdWindow.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class ThresholdWindowPipelineTest : public Testing::NESBaseTest, public AbstractPipelineExecutionTest {
  public:
    std::vector<Expressions::ExpressionPtr> aggFieldAccessExpressionsVector;
    std::vector<Nautilus::Record::RecordFieldIdentifier> resultFieldVector;
    std::vector<Aggregation::AggregationFunctionPtr> aggVector;
    std::vector<std::unique_ptr<Aggregation::AggregationValue>> aggValues;
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ThresholdWindowPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup ThresholdWindowPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ThresholdWindowPipelineTest test class."); }
};

/**
 * @brief Test running a pipeline containing a threshold window with a Avg aggregation
 */
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithSum) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Sum";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerPhysicalType, integerPhysicalType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 50);
}

/**
 * @brief Test running a pipeline containing a threshold window with a Avg aggregation
 */
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithCount) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Count";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);
    auto unsignedIntegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto countAgg = std::make_shared<Aggregation::CountAggregationFunction>(integerPhysicalType, unsignedIntegerType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(countAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Count", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto countAggregationValue = std::make_unique<Aggregation::CountAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(countAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 2);
}

/**
 * @brief Test running a pipeline containing a threshold window with a Min aggregation
 */
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithMin) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Min";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto minAgg = std::make_shared<Aggregation::MinAggregationFunction>(integerPhysicalType, integerPhysicalType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(minAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Min", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto minAggregationValue = std::make_unique<Aggregation::MinAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(minAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 20);
}

/**
 * @brief Test running a pipeline containing a threshold window with a Max aggregation
 */
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithMax) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Max";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto maxAgg = std::make_shared<Aggregation::MaxAggregationFunction>(integerPhysicalType, integerPhysicalType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(maxAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Max", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto maxAggregationValue = std::make_unique<Aggregation::MaxAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(maxAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 30);
}

/**
 * @brief Test running a pipeline containing a threshold window with a Avg aggregation
 */
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithAvg) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Avg";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto avgAgg = std::make_shared<Aggregation::AvgAggregationFunction>(integerPhysicalType, integerPhysicalType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(avgAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Avg", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto avgAggregationValue = std::make_unique<Aggregation::AvgAggregationValue<int8_t>>();
    aggValues.emplace_back(std::move(avgAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 25);
}

// This test ensures that the aggregated field does not have to be an integer, which is the data type of count aggregation.
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithAvgFloat) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::FLOAT32);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Avg";

    DataTypePtr integerType = DataTypeFactory::createFloat();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto avgAgg = std::make_shared<Aggregation::AvgAggregationFunction>(integerPhysicalType, integerPhysicalType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(avgAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Avg", BasicType::FLOAT32);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((float) 10.0);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((float) 20.0);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((float) 30.0);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((float) 40.0);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto avgAggregationValue = std::make_unique<Aggregation::AvgAggregationValue<int8_t>>();
    aggValues.emplace_back(std::move(avgAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<float>(), 25.0);
}

/**
 * @brief Test running a pipeline containing a threshold window with a Avg aggregation
 */
TEST_P(ThresholdWindowPipelineTest, thresholdWindowWithFloatPredicate) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::FLOAT32);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto half = std::make_shared<Expressions::ConstantFloatValueExpression>(0.5);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, half);
    auto aggregationResultFieldName = "test$Sum";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerPhysicalType, integerPhysicalType);
    aggFieldAccessExpressionsVector.emplace_back(readF2);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.emplace_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<Operators::GlobalThresholdWindow>(greaterThanExpression,
                                                                                0,
                                                                                aggFieldAccessExpressionsVector,
                                                                                resultFieldVector,
                                                                                aggVector,
                                                                                0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((float) 0.5);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((float) 2.5);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((float) 3.75);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((float) 0.25);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<Operators::GlobaThresholdWindowOperatorHandler>(std::move(aggValues));

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 50);
}

// TODO #3468: parameterize the aggregation function instead of repeating the similar test
INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ThresholdWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<ThresholdWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
