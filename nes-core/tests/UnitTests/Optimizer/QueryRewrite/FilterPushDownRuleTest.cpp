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

// clang-format off
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
// clang-format on
#include "Compiler/JITCompilerBuilder.hpp"
#include "Compiler/CPPCompiler/CPPCompiler.hpp"
#include "Optimizer/Phases/TypeInferencePhase.hpp"
#include "Optimizer/QueryValidation/SyntacticQueryValidation.hpp"
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class FilterPushDownRuleTest : public Testing::NESBaseTest {

  public:
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("FilterPushDownRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup FilterPushDownRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }
};

void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO2("Setup FilterPushDownTest test case.");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("default_logical", sce1);
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMap) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMapAndBeforeFilter) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFiltersBelowAllMapOperators) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperator1 = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr mapOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFilterBelowMap) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") > 45)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterAlreadyAtBottom) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").filter(Attribute("id") > 45).map(Attribute("value") = 40).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45);

    Query query = Query::from("default_logical")
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ = (*itr);
    ++itr;
    const NodePtr unionOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr mapOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG2("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG2("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersAlreadyBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45);

    Query query = Query::from("default_logical")
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .unionWith(subQuery)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr mapOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG2("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG2("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car");

    Query query = Query::from("default_logical")
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 55)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr unionOperator = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG2("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG2("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterAlreadyBelowAndTwoFiltersBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").map(Attribute("value") = 90).filter(Attribute("id") > 35);

    Query query = Query::from("default_logical")
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 55)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr unionOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr mapOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG2("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG2("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersAlreadyAtBottomAndTwoFiltersBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").filter(Attribute("id") > 35);

    Query query = Query::from("default_logical")
                      .filter(Attribute("id") > 25)
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 55)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ3 = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG2("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG2("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    // The order of the branches of the union operator matters and should stay the same.
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", sinkOperator->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", mapOperatorPQ1->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", mapOperatorPQ2->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", mergeOperator->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ1->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ2->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(filterOperatorPQ3->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ3->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", srcOperatorPQ->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ1->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ2->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorSQ->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    NES_DEBUG2("Expected Plan Node: {}  Actual in updated Query plan: {}", srcOperatorSQ->toString(), (*itr)->toString());
}
TEST_F(FilterPushDownRuleTest, testPushingFilterBetweenTwoMaps) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("val", NES::BasicType::UINT64)
                                ->addField("X", NES::BasicType::UINT64)
                                ->addField("Y", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("example", schema);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    NES_INFO2("Setup FilterPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("example", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("example", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    sourceCatalog->addPhysicalSource("example", sce1);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto query = Query::from("example")
                     .map(Attribute("Y") = Attribute("Y") - 2)
                     .map(Attribute("NEW_id2") = Attribute("Y") / Attribute("Y"))
                     .filter(Attribute("Y") >= 49)
                     .sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG2("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG2("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

/* tests if a filter is correctly pushed below a join if all its attributes belong to source 1. The order of the operators in the
updated query plan is validated, and it is checked that the input and output schema of the filter that is now at a new position
is still correct */
TEST_F(FilterPushDownRuleTest, testPushingFilterBelowJoinToSrc1){
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup sources
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src1", schema);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("src1", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("src1", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("src1", sce1);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("X", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);

    sourceCatalog->addLogicalSource("src2", schema2);
    auto csvSourceType2 = CSVSourceType::create();
    PhysicalSourcePtr physicalSource2 = PhysicalSource::create("src2", "test_stream", csvSourceType2);
    LogicalSourcePtr logicalSource2 = LogicalSource::create("src2", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce2 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    sourceCatalog->addPhysicalSource("src2", sce2);


    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");

    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")),Milliseconds(1000)))
                      .filter(Attribute("A") < 9999 && Attribute("ts") < 9999)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    //auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    //typeInferencePhase->execute(queryPlan);

    /*auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();

    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(QueryParsingService::create(jitCompiler));
    syntacticQueryValidation->validate(query.getQueryPlan()->toString());*/


    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, Catalogs::UDF::UDFCatalog::create());
    NES_DEBUG2("before semantic query validation in filterPushDownRuleTest.cpp");
    semanticQueryValidation->validate(queryPlan);
    NES_DEBUG2("after semantic query validation in filterPushDownRuleTest.cpp");

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());


    auto typeInferencePhase2 = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase2->execute(updatedPlan);

    // Validate
    // The order of the branches of the union operator matters and should stay the same.
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(joinOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    //check if the schema of the filter is still correct
    EXPECT_TRUE(srcOperatorSrc1->as<SourceLogicalOperatorNode>()->getOutputSchema()->equals((*itr)->as<FilterLogicalOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(watermarkOperatorAboveSrc1->as<WatermarkAssignerLogicalOperatorNode>()->getInputSchema()->equals((*itr)->as<FilterLogicalOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc1->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowJoinNotPossible){
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup sources
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("example", schema);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("example", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("example", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("example", sce1);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("X", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);

    sourceCatalog->addLogicalSource("example2", schema2);
    auto csvSourceType2 = CSVSourceType::create();
    PhysicalSourcePtr physicalSource2 = PhysicalSource::create("example2", "test_stream", csvSourceType2);
    LogicalSourcePtr logicalSource2 = LogicalSource::create("example2", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce2 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    sourceCatalog->addPhysicalSource("example2", sce2);


    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("example2");

    Query query = Query::from("example")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")),Milliseconds(1000)))
                      .filter(Attribute("A") < 9999 || Attribute("X") < 9999)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveExp2 = (*itr);
    ++itr;
    const NodePtr srcOperatorExp2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveExp1 = (*itr);
    ++itr;
    const NodePtr srcOperatorExp1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    // The order of the branches of the union operator matters and should stay the same.
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    //check if the schema of the filter is still correct
    //check if the schema of the filter is still correct
    EXPECT_TRUE(joinOperator->as<JoinLogicalOperatorNode>()->getOutputSchema()->equals((*itr)->as<FilterLogicalOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(sinkOperator->as<SinkLogicalOperatorNode>()->getInputSchema()->equals((*itr)->as<FilterLogicalOperatorNode>()->getOutputSchema()));

    ++itr;
    EXPECT_TRUE(joinOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveExp2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorExp2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveExp1->equal((*itr)));
     ++itr;
    EXPECT_TRUE(srcOperatorExp1->equal((*itr)));
}

