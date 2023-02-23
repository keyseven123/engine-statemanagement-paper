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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Expressions {
class AggregationFunctionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AddExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TraceTest test class."); }
};

/**
 * Tests the lift, combine, lower and reset functions of the Sum Aggregation
 */
TEST_F(AggregationFunctionTest, scanEmitPipelineSum) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg = Aggregation::SumAggregationFunction(integerType, integerType);
    auto sumValue = Aggregation::SumAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &sumValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 1);
    // test lift
    sumAgg.lift(memref, incomingValue);
    ASSERT_EQ(sumValue.sum, 1);

    // test combine
    sumAgg.combine(memref, memref);
    ASSERT_EQ(sumValue.sum, 2);

    // test lower
    auto aggregationResult = sumAgg.lower(memref);
    ASSERT_EQ(aggregationResult, 2);

    // test reset
    sumAgg.reset(memref);
    EXPECT_EQ(sumValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Count Aggregation
 */
TEST_F(AggregationFunctionTest, scanEmitPipelineCount) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto unsignedIntegerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto countAgg = Aggregation::CountAggregationFunction(integerType, unsignedIntegerType);

    auto countValue = Aggregation::CountAggregationValue<uint64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &countValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 1);
    // test lift
    countAgg.lift(memref, incomingValue);
    ASSERT_EQ(countValue.count, (uint64_t) 1);

    // test combine
    countAgg.combine(memref, memref);
    ASSERT_EQ(countValue.count, (uint64_t) 2);

    // test lower
    auto aggregationResult = countAgg.lower(memref);
    ASSERT_EQ(aggregationResult, (uint64_t) 2);

    // test reset
    countAgg.reset(memref);
    EXPECT_EQ(countValue.count, (uint64_t) 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Average Aggregation
 */
TEST_F(AggregationFunctionTest, scanEmitPipelineAvg) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr doubleType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createDouble());
    auto avgAgg = Aggregation::AvgAggregationFunction(integerType, doubleType);
    auto avgValue = Aggregation::AvgAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &avgValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 2);
    // test lift
    avgAgg.lift(memref, incomingValue);
    EXPECT_EQ(avgValue.count, 1);
    EXPECT_EQ(avgValue.sum, 2);

    // test combine
    avgAgg.combine(memref, memref);
    EXPECT_EQ(avgValue.count, 2);
    EXPECT_EQ(avgValue.sum, 4);

    // test lower
    auto aggregationResult = avgAgg.lower(memref);
    EXPECT_EQ(aggregationResult, 2);

    // test reset
    avgAgg.reset(memref);
    EXPECT_EQ(avgValue.count, 0);
    EXPECT_EQ(avgValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Min Aggregation
 */
TEST_F(AggregationFunctionTest, scanEmitPipelineMin) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto minAgg = Aggregation::MinAggregationFunction(integerType, integerType);
    auto minValue = Aggregation::MinAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &minValue);
    auto incomingValueFive = Nautilus::Value<Nautilus::Int64>((int64_t) 5);
    auto incomingValueTen = Nautilus::Value<Nautilus::Int64>((int64_t) 10);
    auto incomingValueOne = Nautilus::Value<Nautilus::Int64>((int64_t) 1);
    auto incomingValueTwo = Nautilus::Value<Nautilus::Int64>((int64_t) 2);

    // lift value in minAgg with an initial value of 5, thus the current min should be 5
    minAgg.lift(memref, incomingValueFive);
    ASSERT_EQ(minValue.min, incomingValueFive);

    // lift value in minAgg with a higher value of 10, thus the current min should still be 5
    minAgg.lift(memref, incomingValueTen);
    ASSERT_EQ(minValue.min, incomingValueFive);

    // lift value in minAgg with a lower value of 1, thus the current min should change to 1
    minAgg.lift(memref, incomingValueOne);
    ASSERT_EQ(minValue.min, incomingValueOne);

    // lift value in minAgg with a higher value of 2, thus the current min should still be 1
    minAgg.lift(memref, incomingValueTwo);
    ASSERT_EQ(minValue.min, incomingValueOne);

    // combine memrefs in minAgg
    auto anotherMinValue = Aggregation::MinAggregationValue<int64_t>();
    auto anotherMemref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &anotherMinValue);
    minAgg.lift(anotherMemref, incomingValueTen);

    // test if memref1 < memref2
    minAgg.combine(memref, anotherMemref);
    ASSERT_EQ(minValue.min, incomingValueOne);

    // test if memref1 > memref2
    minAgg.combine(anotherMemref, memref);
    ASSERT_EQ(anotherMinValue.min, incomingValueOne);

    // test if memref1 = memref2
    minAgg.lift(anotherMemref, incomingValueOne);
    ASSERT_EQ(anotherMinValue.min, incomingValueOne);

    // lower value in minAgg
    auto aggregationResult = minAgg.lower(memref);
    ASSERT_EQ(aggregationResult, 1);

    // test reset
    minAgg.reset(memref);
    EXPECT_EQ(minValue.min, std::numeric_limits<int64_t>::max());
}

/**
 * Tests the lift, combine, lower and reset functions of the Max Aggregation
 */
TEST_F(AggregationFunctionTest, scanEmitPipelineMax) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto maxAgg = Aggregation::MaxAggregationFunction(integerType, integerType);
    auto maxValue = Aggregation::MaxAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &maxValue);
    auto incomingValueFive = Nautilus::Value<Nautilus::Int64>((int64_t) 5);
    auto incomingValueTen = Nautilus::Value<Nautilus::Int64>((int64_t) 10);
    auto incomingValueOne = Nautilus::Value<Nautilus::Int64>((int64_t) 1);
    auto incomingValueFifteen = Nautilus::Value<Nautilus::Int64>((int64_t) 15);

    // lift value in maxAgg with an initial value of 5, thus the current min should be 5
    maxAgg.lift(memref, incomingValueFive);
    ASSERT_EQ(maxValue.max, incomingValueFive);

    // lift value in maxAgg with a higher value of 10, thus the current min should change to 10
    maxAgg.lift(memref, incomingValueTen);
    ASSERT_EQ(maxValue.max, incomingValueTen);

    // lift value in maxAgg with a lower value of 1, thus the current min should still be 10
    maxAgg.lift(memref, incomingValueOne);
    ASSERT_EQ(maxValue.max, incomingValueTen);

    // lift value in maxAgg with a higher value of 15, thus the current min should change to 15
    maxAgg.lift(memref, incomingValueFifteen);
    ASSERT_EQ(maxValue.max, incomingValueFifteen);

    // combine memrefs in maxAgg
    auto anotherMaxValue = Aggregation::MaxAggregationValue<int64_t>();
    auto anotherMemref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &anotherMaxValue);
    maxAgg.lift(anotherMemref, incomingValueOne);

    // test if memref1 > memref2
    maxAgg.combine(memref, anotherMemref);
    ASSERT_EQ(maxValue.max, incomingValueFifteen);

    // test if memref1 < memref2
    maxAgg.combine(anotherMemref, memref);
    ASSERT_EQ(anotherMaxValue.max, incomingValueFifteen);

    // test if memref1 = memref2
    maxAgg.lift(anotherMemref, incomingValueFifteen);
    maxAgg.combine(memref, anotherMemref);
    ASSERT_EQ(anotherMaxValue.max, incomingValueFifteen);

    // lower value in minAgg
    auto aggregationResult = maxAgg.lower(memref);
    ASSERT_EQ(aggregationResult, incomingValueFifteen);

    // test reset
    maxAgg.reset(memref);
    EXPECT_EQ(maxValue.max, std::numeric_limits<int64_t>::min());
}

}// namespace NES::Runtime::Execution::Expressions
