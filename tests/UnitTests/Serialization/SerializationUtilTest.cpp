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

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/Serialization/QueryReconfigurationPlanSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryReconfigurationPlan.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <iostream>

#include <API/Windowing.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
using namespace NES;

class SerializationUtilTest : public testing::Test {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES_INFO("Setup SerializationUtilTest test class."); }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("SerializationUtilTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SerializationUtilTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup SerializationUtilTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SerializationUtilTest test class."); }
};

TEST_F(SerializationUtilTest, dataTypeSerialization) {

    // serialize and deserialize int8
    auto serializedInt8 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt8(), new SerializableDataType());
    auto deserializedInt8 = DataTypeSerializationUtil::deserializeDataType(serializedInt8);
    EXPECT_TRUE(DataTypeFactory::createInt8()->isEquals(deserializedInt8));

    // serialize and deserialize int16
    auto serializedInt16 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt16(), new SerializableDataType());
    auto deserializedInt16 = DataTypeSerializationUtil::deserializeDataType(serializedInt16);
    EXPECT_TRUE(DataTypeFactory::createInt16()->isEquals(deserializedInt16));

    // serialize and deserialize int32
    auto serializedInt32 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt32(), new SerializableDataType());
    auto deserializedInt32 = DataTypeSerializationUtil::deserializeDataType(serializedInt32);
    EXPECT_TRUE(DataTypeFactory::createInt32()->isEquals(deserializedInt32));

    // serialize and deserialize int64
    auto serializedInt64 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt64(), new SerializableDataType());
    auto deserializedInt64 = DataTypeSerializationUtil::deserializeDataType(serializedInt64);
    EXPECT_TRUE(DataTypeFactory::createInt64()->isEquals(deserializedInt64));

    // serialize and deserialize uint8
    auto serializedUInt8 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt8(), new SerializableDataType());
    auto deserializedUInt8 = DataTypeSerializationUtil::deserializeDataType(serializedUInt8);
    EXPECT_TRUE(DataTypeFactory::createUInt8()->isEquals(deserializedUInt8));

    // serialize and deserialize uint16
    auto serializedUInt16 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt16(), new SerializableDataType());
    auto deserializedUInt16 = DataTypeSerializationUtil::deserializeDataType(serializedUInt16);
    EXPECT_TRUE(DataTypeFactory::createUInt16()->isEquals(deserializedUInt16));

    // serialize and deserialize uint32
    auto serializedUInt32 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt32(), new SerializableDataType());
    auto deserializedUInt32 = DataTypeSerializationUtil::deserializeDataType(serializedUInt32);
    EXPECT_TRUE(DataTypeFactory::createUInt32()->isEquals(deserializedUInt32));

    // serialize and deserialize uint64
    auto serializedUInt64 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt64(), new SerializableDataType());
    auto deserializedUInt64 = DataTypeSerializationUtil::deserializeDataType(serializedUInt64);
    EXPECT_TRUE(DataTypeFactory::createUInt64()->isEquals(deserializedUInt64));

    // serialize and deserialize float32
    auto serializedFloat32 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createFloat(), new SerializableDataType());
    auto deserializedFloat32 = DataTypeSerializationUtil::deserializeDataType(serializedFloat32);
    EXPECT_TRUE(DataTypeFactory::createFloat()->isEquals(deserializedFloat32));

    // serialize and deserialize float64
    auto serializedFloat64 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createDouble(), new SerializableDataType());
    auto deserializedFloat64 = DataTypeSerializationUtil::deserializeDataType(serializedFloat64);
    EXPECT_TRUE(DataTypeFactory::createDouble()->isEquals(deserializedFloat64));

    // serialize and deserialize float64
    auto serializedArray =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createArray(42, DataTypeFactory::createInt8()),
                                                     new SerializableDataType());
    auto deserializedArray = DataTypeSerializationUtil::deserializeDataType(serializedArray);
    EXPECT_TRUE(DataTypeFactory::createArray(42, DataTypeFactory::createInt8())->isEquals(deserializedArray));

    /*
   std::string json_string;
   google::protobuf::util::MessageToJsonString(type, &json_string);
   std::cout << json_string << std::endl;
   */
}

TEST_F(SerializationUtilTest, schemaSerializationTest) {

    auto schema = Schema::create();
    schema->addField("f1", DataTypeFactory::createDouble());
    schema->addField("f2", DataTypeFactory::createInt32());
    schema->addField("f3", DataTypeFactory::createArray(42, DataTypeFactory::createInt8()));

    auto serializedSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    auto deserializedSchema = SchemaSerializationUtil::deserializeSchema(serializedSchema.get());
    EXPECT_TRUE(deserializedSchema->equals(schema));

    std::string json_string;
    auto options = google::protobuf::util::JsonOptions();
    options.add_whitespace = true;
    google::protobuf::util::MessageToJsonString(*serializedSchema, &json_string, options);
    std::cout << json_string << std::endl;
}

TEST_F(SerializationUtilTest, sourceDescriptorSerialization) {
    auto schema = Schema::create();
    schema->addField("f1", INT32);

    {
        auto source = ZmqSourceDescriptor::create(schema, "localhost", 42);
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

#if ENABLE_OPC_BUILD
    {
        UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
        auto source = OPCSourceDescriptor::create(schema, "localhost", nodeId, "", "");
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }
#endif

    {
        auto source = BinarySourceDescriptor::create(schema, "localhost");
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = CsvSourceDescriptor::create(schema, "testStream", "localhost", ",", 0, 10, 10, false);
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
        std::string json_string;
        auto options = google::protobuf::util::JsonOptions();
        options.add_whitespace = true;
        google::protobuf::util::MessageToJsonString(*serializedSourceDescriptor, &json_string, options);
        std::cout << json_string << std::endl;
    }

    {
        auto source = DefaultSourceDescriptor::create(schema, 55, 42);
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = LogicalStreamSourceDescriptor::create("localhost");
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = SenseSourceDescriptor::create(schema, "senseusf");
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        Network::NesPartition nesPartition{1, 22, 33, 44};
        auto source = Network::NetworkSourceDescriptor::create(schema, nesPartition);
        auto serializedSourceDescriptor =
            OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }
}

TEST_F(SerializationUtilTest, sinkDescriptorSerialization) {

    {
        auto sink = ZmqSinkDescriptor::create("localhost", 42);
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

#if ENABLE_OPC_BUILD
    {
        UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
        auto sink = OPCSinkDescriptor::create("localhost", nodeId, "", "");
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }
#endif

    {
        auto sink = PrintSinkDescriptor::create();
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test");
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "TEXT_FORMAT", "APPEND");
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "TEXT_FORMAT", "OVERWRITE");
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "NES_FORMAT", "OVERWRITE");
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "CSV_FORMAT", "OVERWRITE");
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        Network::NodeLocation nodeLocation{1, "localhost", 31337};
        Network::NesPartition nesPartition{1, 22, 33, 44};
        auto sink = Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(1), 5);
        auto serializedSinkDescriptor =
            OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }
}

TEST_F(SerializationUtilTest, expressionSerialization) {

    {
        auto fieldAccess = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f1");
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(fieldAccess, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(fieldAccess->equal(deserializedExpression));
    }
    auto f1 = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f1");
    auto f2 = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f2");

    {
        auto expression = AndExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = OrExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = EqualsExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = LessExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = LessEqualsExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = GreaterExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = GreaterExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = AddExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = MulExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = DivExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = SubExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
}

TEST_F(SerializationUtilTest, operatorSerialization) {

    {
        auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("testStream"));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(source, new SerializableOperator());
        auto sourceOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(source->equal(sourceOperator));
    }

    {
        auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(filter, new SerializableOperator());
        auto filterOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(filter->equal(filterOperator));
    }

    {
        auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(map, new SerializableOperator());
        auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(map->equal(mapOperator));
    }

    {
        auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = POWER(2, Attribute("f3")));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(map, new SerializableOperator());
        auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(map->equal(mapOperator));
    }

    {
        /*
        * Unary operators are available in the C++ API but not in the legacy compiler.
        * We therefore throw a not implemented exception when ABS is used.
        * TODO:  re-enable this test when ABS is fully implemented
        */
        EXPECT_ANY_THROW({
            auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = ABS(Attribute("f3")));
            auto serializedOperator = OperatorSerializationUtil::serializeOperator(map, new SerializableOperator());
            auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
            EXPECT_TRUE(map->equal(mapOperator));
        });
    }

    {
        auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(sink, new SerializableOperator());
        auto sinkOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(sink->equal(sinkOperator));
    }

    {
        auto unionOp = LogicalOperatorFactory::createUnionOperator();
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(unionOp, new SerializableOperator());
        auto unionOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(unionOp->equal(unionOperator));
    }

    {
        auto broadcast = LogicalOperatorFactory::createBroadcastOperator();
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(broadcast, new SerializableOperator());
        auto broadcastOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(broadcast->equal(broadcastOperator));
    }

    {
        Windowing::WindowTriggerPolicyPtr triggerPolicy = Windowing::OnTimeTriggerPolicyDescription::create(1000);
        auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
        auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();
        Join::LogicalJoinDefinitionPtr joinDef = Join::LogicalJoinDefinition::create(
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), API::Milliseconds(10)),
            distrType,
            triggerPolicy,
            triggerAction,
            1,
            1);

        auto join = LogicalOperatorFactory::createJoinOperator(joinDef);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(join, new SerializableOperator());
        auto joinOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(join->equal(joinOperator));
    }
}

TEST_F(SerializationUtilTest, queryPlanSerDeSerialization) {

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink->addChild(map);

    auto queryPlan = QueryPlan::create(1, 1, {sink});

    auto serializedQueryPlan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getQuerySubPlanId() == queryPlan->getQuerySubPlanId());
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));
}

#if ENABLE_OPC_BUILD
TEST_F(SerializationUtilTest, queryPlanWithOPCSerDeSerialization) {

    auto schema = Schema::create();
    schema->addField("f1", INT32);
    UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
    auto source = LogicalOperatorFactory::createSourceOperator(OPCSourceDescriptor::create(schema, "localhost", nodeId, "", ""));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink = LogicalOperatorFactory::createSinkOperator(OPCSinkDescriptor::create("localhost", nodeId, "", ""));
    sink->addChild(map);

    auto queryPlan = QueryPlan::create(1, 1, {sink});

    auto serializedQueryPlan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getQuerySubPlanId() == queryPlan->getQuerySubPlanId());
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));
}
#endif

TEST_F(SerializationUtilTest, queryPlanWithMultipleRootSerDeSerialization) {

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto sink2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink1->addChild(map);
    sink2->addChild(map);

    auto queryPlan = QueryPlan::create(1, 1, {sink1, sink2});

    auto serializedQueryPlan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getQuerySubPlanId() == queryPlan->getQuerySubPlanId());

    std::vector<OperatorNodePtr> actualRootOperators = deserializedQueryPlan->getRootOperators();
    for (auto actualRootOperator : actualRootOperators) {
        bool found = false;
        for (auto queryRoot : queryPlan->getRootOperators()) {
            if (actualRootOperator->equal(queryRoot)) {
                EXPECT_TRUE(actualRootOperator->equalWithAllChildren(queryRoot));
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

TEST_F(SerializationUtilTest, queryPlanWithMultipleSourceSerDeSerialization) {
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("testStream"));
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source1);
    filter->addChild(source2);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto sink2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink1->addChild(map);
    sink2->addChild(map);

    auto queryPlan = QueryPlan::create(1, 1, {sink1, sink2});

    auto serializedQueryPlan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getQuerySubPlanId() == queryPlan->getQuerySubPlanId());

    std::vector<OperatorNodePtr> actualRootOperators = deserializedQueryPlan->getRootOperators();
    for (auto actualRootOperator : actualRootOperators) {
        bool found = false;
        for (auto queryRoot : queryPlan->getRootOperators()) {
            if (actualRootOperator->equal(queryRoot)) {
                EXPECT_TRUE(actualRootOperator->equalWithAllChildren(queryRoot));
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
    EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->as<OperatorNode>()->getId()
                == actualRootOperators[1]->getChildren()[0]->as<OperatorNode>()->getId());
    EXPECT_TRUE(actualRootOperators[0]->getChildren()[0].get() == actualRootOperators[1]->getChildren()[0].get());
    std::vector<NodePtr> sourceOperatorsForRoot1 = actualRootOperators[0]->getAllLeafNodes();
    std::vector<NodePtr> sourceOperatorsForRoot2 = actualRootOperators[1]->getAllLeafNodes();
    EXPECT_TRUE(sourceOperatorsForRoot1.size() == 2);
    EXPECT_TRUE(sourceOperatorsForRoot2.size() == 2);
    for (auto sourceOperatorForRoot1 : sourceOperatorsForRoot1) {
        bool found = false;
        for (auto sourceOperatorForRoot2 : sourceOperatorsForRoot2) {
            if (sourceOperatorForRoot1->equal(sourceOperatorForRoot2)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

TEST_F(SerializationUtilTest, queryReconfigurationPlanSerDe) {
    auto expectedToStart = std::vector<QuerySubPlanId>{111, 222, 333};
    auto expectedToStop = std::vector<QuerySubPlanId>{444, 555, 666};
    auto expectedToReplace = std::unordered_map<QuerySubPlanId, QuerySubPlanId>{{777, 888}, {999, 1111}};
    auto expectedId = 10;
    auto expectedQueryId = 100;

    auto expectedPlan = QueryReconfigurationPlan::create(expectedToStart, expectedToStop, expectedToReplace);
    expectedPlan->setId(expectedId);
    expectedPlan->setQueryId(expectedQueryId);

    auto serializedPlan = QueryReconfigurationPlanSerializationUtil::serializeQueryReconfigurationPlan(expectedPlan);
    auto actualPlan = QueryReconfigurationPlanSerializationUtil::deserializeQueryReconfigurationPlan(serializedPlan);

    ASSERT_EQ(actualPlan->getId(), expectedId);
    ASSERT_EQ(actualPlan->getQueryId(), expectedQueryId);
    ASSERT_TRUE(actualPlan->getQuerySubPlanIdsToStart() == expectedToStart);
    ASSERT_TRUE(actualPlan->getQuerySubPlanIdsToStop() == expectedToStop);
    ASSERT_TRUE(actualPlan->getQuerySubPlanIdsToReplace() == expectedToReplace);
}
