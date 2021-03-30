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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/Phases/Z3SignatureInferencePhase.hpp>
#include <iostream>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <z3++.h>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

using namespace NES;

class Z3SignatureBasedCompleteQueryMergerRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    StreamCatalogPtr streamCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("Z3SignatureBasedCompleteQueryMergerRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup Z3SignatureBasedCompleteQueryMergerRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64);
        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream("car", schema);
        streamCatalog->addLogicalStream("bike", schema);
        streamCatalog->addLogicalStream("truck", schema);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup Z3SignatureBasedCompleteQueryMergerRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down Z3SignatureBasedCompleteQueryMergerRuleTest test class."); }
};

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with same queries
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingEqualQueries) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1Children : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2Children : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1Children, sink2Children);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with same queries with multiple same source
 */
//FIXME: This required 1402 and 1378 to be fixed
// We need to do the attribute name resolution just as done in SQL systems to identify or distinguish attribute names
// coming from same streams (in case of a self join)
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, DISABLED_testMergingEqualQueriesWithMultipleSameSources) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto sourceOperator11 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    auto sourceOperator21 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    auto sinkOperator11 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator11->addChild(sourceOperator11);
    sinkOperator11->addChild(sourceOperator21);

    QueryPlanPtr queryPlan1 = QueryPlan::create();
    queryPlan1->addRootOperator(sinkOperator11);
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto sourceOperator12 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    auto sourceOperator22 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    auto sinkOperator12 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator12->addChild(sourceOperator12);
    sinkOperator12->addChild(sourceOperator22);

    QueryPlanPtr queryPlan2 = QueryPlan::create();
    queryPlan2->addRootOperator(sinkOperator12);
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different source
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentSources) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("truck").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with same queries with unionWith operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithUnionOperators) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("truck");
    Query query1 = Query::from("car")
                       .unionWith(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .unionWith(&subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with queries with different order of unionWith operator children
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithUnionOperatorChildrenOrder) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("car");
    Query query1 = Query::from("truck")
                       .unionWith(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .unionWith(&subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with queries with unionWith operators but different children
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithUnionOperatorsWithDifferentChildren) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("bike");
    Query query1 = Query::from("truck")
                       .unionWith(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .unionWith(&subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different filters
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentFilters) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different filters
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentFiltersField) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id1") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different map
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentMapAttribute) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value1") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different map
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentMapValue) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 50).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different window operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentWindowTypes) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));
    auto aggregation1 = Sum(Attribute("value"));

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 40)
                       .windowByKey(Attribute("type"), windowType1, aggregation1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));
    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 40)
                       .windowByKey(Attribute("type"), windowType2, aggregation2)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different window operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentWindowAggregations) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation1 = Sum(Attribute("value"));

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 40)
                       .windowByKey(Attribute("type"), windowType1, aggregation1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Min(Attribute("value"));
    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 40)
                       .windowByKey(Attribute("type"), windowType2, aggregation2)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same window operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithSameWindows) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation1 = Sum(Attribute("value"));

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 40)
                       .windowByKey(Attribute("type"), windowType1, aggregation1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));
    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 40)
                       .windowByKey(Attribute("type"), windowType2, aggregation2)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same window operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithSameWindowsButDifferentOperatorOrder) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation1 = Sum(Attribute("value"));

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .windowByKey(Attribute("type"), windowType1, aggregation1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));
    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .windowByKey(Attribute("type"), windowType2, aggregation2)
                       .filter(Attribute("type") < 40)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same project operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithSameProjectOperator) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("type"), Attribute("value"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("value"), Attribute("type"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same project operator but in different order
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithSameProjectOperatorButDifferentOrder) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("type"), Attribute("value"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .project(Attribute("value"), Attribute("type"))
                       .filter(Attribute("type") < 40)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different project operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentProjectOperatorOrder) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("type"), Attribute("value"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("value"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same watermark operators
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithSameWatermarkOperator) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query1 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("type"), Attribute("value"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("value"), Attribute("type"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different watermark operators.
 * One query with IngestionTimeWatermarkStrategy and other with EventTimeWatermarkStrategy.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentWatermarkOperator) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query1 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("type"), Attribute("value"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .assignWatermark(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                           Attribute("ts"), NES::API::Milliseconds(10), NES::API::Milliseconds()))
                       .map(Attribute("value") = 40)
                       .filter(Attribute("type") < 40)
                       .project(Attribute("value"), Attribute("type"))
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two identical queries with unionWith operators.
 * Each query have two sources and both using IngestionTimeWatermarkStrategy.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithUnionOperatorsAndTwoIdenticalWatermarkAssigner) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("truck").assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
    Query query1 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .unionWith(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck").assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
    Query query2 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .unionWith(&subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with Same queries with unionWith operator.
 * Each query has two sources with different watermark strategy. One source with IngestionTimeWatermarkStrategy and other
 * with EventTimeWatermarkStrategy.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest,
       testMergingEqualQueriesWithUnionOperatorsAndMultipleDistinctWatermarkAssigner) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("truck").assignWatermark(Windowing::EventTimeWatermarkStrategyDescriptor::create(
        Attribute("ts"), NES::API::Milliseconds(10), NES::API::Milliseconds()));
    Query query1 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .unionWith(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck").assignWatermark(Windowing::EventTimeWatermarkStrategyDescriptor::create(
        Attribute("ts"), NES::API::Milliseconds(10), NES::API::Milliseconds()));
    Query query2 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .unionWith(&subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with distinct queries with unionWith operator.
 * Each query has two sources with different watermark strategies. One source with IngestionTimeWatermarkStrategy and other
 * with EventTimeWatermarkStrategy and in the second query the strategies are inverted.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest,
       testMergingDistinctQueriesWithUnionOperatorsAndMultipleDistinctWatermarkAssigner) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("truck").assignWatermark(Windowing::EventTimeWatermarkStrategyDescriptor::create(
        Attribute("ts"), NES::API::Milliseconds(10), NES::API::Milliseconds()));
    Query query1 = Query::from("car")
                       .assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create())
                       .unionWith(&subQuery1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck").assignWatermark(Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
    Query query2 = Query::from("car")
                       .assignWatermark(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                           Attribute("ts"), NES::API::Milliseconds(10), NES::API::Milliseconds()))
                       .unionWith(&subQuery2)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithJoinOperator) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 = Query::from("car")
                       .joinWith(subQuery1)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .joinWith(subQuery2)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType2)
                       .sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size());

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithJoinOperatorWithDifferentStreamOrder) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 = Query::from("car")
                       .joinWith(subQuery1)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("car");
    Query query2 = Query::from("truck")
                       .joinWith(subQuery2)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType2)
                       .sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithJoinOperatorWithDifferentButEqualWindows) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 = Query::from("car")
                       .joinWith(subQuery1)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .joinWith(subQuery2)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType2)
                       .sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(Z3SignatureBasedCompleteQueryMergerRuleTest, testMergingQueriesWithJoinOperatorWithDifferentWindows) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 = Query::from("car")
                       .joinWith(subQuery1)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType1)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .joinWith(subQuery2)
                       .where(Attribute("value"))
                       .equalsTo(Attribute("value"))
                       .window(windowType2)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::Z3SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedCompleteQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}
