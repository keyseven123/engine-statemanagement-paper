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

#include "gtest/gtest.h"

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>

namespace NES {

class QueryTest : public testing::Test {
  public:
    SourceConfigPtr sourceConfig;

    static void SetUpTestCase() {
        NES::setupLogging("QueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {

        sourceConfig = SourceConfig::create();
        sourceConfig->setSourceConfig("");
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setNumberOfBuffersToProduce(3);
        sourceConfig->setPhysicalStreamName("test2");
        sourceConfig->setLogicalStreamName("test_stream");
    }

    static void TearDownTestCase() { NES_INFO("Tear down QueryTest test class."); }

    void TearDown() {}
};

TEST_F(QueryTest, testQueryFilter) {

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

TEST_F(QueryTest, testQueryProjection) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("id");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").project(Attribute("id"), Attribute("value")).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

TEST_F(QueryTest, testQueryTumblingWindow) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .windowByKey(Attribute("id"), TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
                                   Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

TEST_F(QueryTest, testQuerySlidingWindow) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical")
            .windowByKey(Attribute("id"), SlidingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10), Seconds(2)),
                         Sum(Attribute("value")))
            .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

/**
 * Merge two input stream: one with filter and one without filter.
 */
TEST_F(QueryTest, DISABLED_testQueryMerge) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").unionWith(&subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2);
    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

/**
 * Join two input stream: one with filter and one without filter.
 */
TEST_F(QueryTest, testQueryJoin) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);

    auto query = Query::from("default_logical")
                     .joinWith(subQuery, Attribute("id"), Attribute("id"),
                               TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2);
    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

TEST_F(QueryTest, testQueryExpression) {
    auto andExpression = Attribute("f1") && 10;
    EXPECT_TRUE(andExpression->instanceOf<AndExpressionNode>());

    auto orExpression = Attribute("f1") || 45;
    EXPECT_TRUE(orExpression->instanceOf<OrExpressionNode>());

    auto lessExpression = Attribute("f1") < 45;
    EXPECT_TRUE(lessExpression->instanceOf<LessExpressionNode>());

    auto lessThenExpression = Attribute("f1") <= 45;
    EXPECT_TRUE(lessThenExpression->instanceOf<LessEqualsExpressionNode>());

    auto equalsExpression = Attribute("f1") == 45;
    EXPECT_TRUE(equalsExpression->instanceOf<EqualsExpressionNode>());

    auto greaterExpression = Attribute("f1") > 45;
    EXPECT_TRUE(greaterExpression->instanceOf<GreaterExpressionNode>());

    auto greaterThenExpression = Attribute("f1") >= 45;
    EXPECT_TRUE(greaterThenExpression->instanceOf<GreaterEqualsExpressionNode>());

    auto notEqualExpression = Attribute("f1") != 45;
    EXPECT_TRUE(notEqualExpression->instanceOf<NegateExpressionNode>());
    auto equals = notEqualExpression->as<NegateExpressionNode>()->child();
    EXPECT_TRUE(equals->instanceOf<EqualsExpressionNode>());

    auto assignmentExpression = Attribute("f1") = --Attribute("f1")++ + 10;
    ConsoleDumpHandler::create()->dump(assignmentExpression, std::cout);
    EXPECT_TRUE(assignmentExpression->instanceOf<FieldAssignmentExpressionNode>());
}

}// namespace NES
