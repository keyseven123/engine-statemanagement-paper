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

// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class ProjectBeforeUnionOperatorRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    StreamCatalogPtr streamCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("ProjectBeforeUnionOperatorRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ProjectBeforeUnionOperatorRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() { schema = Schema::create()->addField("a", BasicType::UINT32)->addField("b", BasicType::UINT32); }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup ProjectBeforeUnionOperatorRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ProjectBeforeUnionOperatorRuleTest test class."); }

    void setupSensorNodeAndStreamCatalog(StreamCatalogPtr streamCatalog) {
        NES_INFO("Setup FilterPushDownTest test case.");
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
        std::vector<std::string> logicalStreamName1{"x"};
        streamCatalog->addPhysicalStream(logicalStreamName1, sce);
        std::vector<std::string> logicalStreamName2{"y"};
        streamCatalog->addPhysicalStream(logicalStreamName2, sce);
        streamCatalog->addLogicalStream("x", schema);
        streamCatalog->addLogicalStream("y", schema);
    }
};

TEST_F(ProjectBeforeUnionOperatorRuleTest, testAddingProjectForUnionWithDifferentSchemas) {

    // Prepare
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("x");
    Query query = Query::from("y").unionWith(&subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto projectionOperators = queryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan);

    auto projectBeforeUnionOperatorRule = Optimizer::ProjectBeforeUnionOperatorRule::create();
    auto updatedQueryPlan = projectBeforeUnionOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    projectionOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.size() == 1);
    auto projectOperator = projectionOperators[0];
    SchemaPtr projectOutputSchema = projectOperator->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->hasFieldName("y$a"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("y$b"));
}

TEST_F(ProjectBeforeUnionOperatorRuleTest, testAddingProjectForUnionWithSameSchemas) {

    // Prepare
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("x");
    Query query = Query::from("x").unionWith(&subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto projectionOperators = queryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan);

    auto projectBeforeUnionOperatorRule = Optimizer::ProjectBeforeUnionOperatorRule::create();
    auto updatedQueryPlan = projectBeforeUnionOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    projectionOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.empty());
}
