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
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/QueryMerger/SignatureBasedEqualQueryMergerRule.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <iostream>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <z3++.h>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

using namespace NES;

class SignatureBasedEqualQueryMergerRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    StreamCatalogPtr streamCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("SignatureBasedEqualQueryMergerRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SignatureBasedEqualQueryMergerRuleTest test case.");
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
    void TearDown() { NES_INFO("Setup EqualQueryMergerRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down EqualQueryMergerRuleTest test class."); }
};

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with same queries
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingEqualQueries) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto gqmToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(gqmToDeploy.size() == 2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with same queries with multiple same source
 */
//FIXME: This required 1402 and 1378 to be fixed
// We need to do the attribute name resolution just as done in SQL systems to identify or distinguish attribute names
// coming from same streams (in case of a self join)
TEST_F(SignatureBasedEqualQueryMergerRuleTest, DISABLED_testMergingEqualQueriesWithMultipleSameSources) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    ASSERT_EQ(sinkGQNs.size(), 2);

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator11->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator12->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    ASSERT_FALSE(sinkOperator1GQN->getChildren().empty());
    ASSERT_FALSE(sinkOperator2GQN->getChildren().empty());

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different source
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentSources) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with same queries with unionWith operators
 */
//TODO: To be fixed by #1502
TEST_F(SignatureBasedEqualQueryMergerRuleTest, DISABLED_testMergingQueriesWithMergeOperators) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with queries with different order of unionWith operator children
 */
//TODO: To be fixed by #1502
TEST_F(SignatureBasedEqualQueryMergerRuleTest, DISABLED_testMergingQueriesWithMergeOperatorChildrenOrder) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with queries with unionWith operators but different children
 */
//TODO: To be fixed by #1502
TEST_F(SignatureBasedEqualQueryMergerRuleTest, DISABLED_testMergingQueriesWithMergeOperatorsWithDifferentChildren) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different filters
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentFilters) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different filters
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentFiltersField) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different map
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentMapAttribute) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different map
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentMapValue) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different window operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentWindowTypes) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different window operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentWindowAggregations) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same window operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithSameWindows) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same window operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithSameWindowsButDifferentOperatorOrder) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same project operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithSameProjectOperator) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same project operator but in different order
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithSameProjectOperatorButDifferentOrder) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different project operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentProjectOperatorOrder) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same watermark operators
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithSameWatermarkOperator) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different watermark operators.
 * One query with IngestionTimeWatermarkStrategy and other with EventTimeWatermarkStrategy.
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithDifferentWatermarkOperator) {

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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two identical queries with unionWith operators.
 * Each query have two sources and both using IngestionTimeWatermarkStrategy.
 */
//TODO: To be fixed by #1502
TEST_F(SignatureBasedEqualQueryMergerRuleTest, DISABLED_testMergingQueriesWithMergeOperatorsAndTwoIdenticalWatermarkAssigner) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with Same queries with unionWith operator.
 * Each query has two sources with different watermark strategy. One source with IngestionTimeWatermarkStrategy and other
 * with EventTimeWatermarkStrategy.
 */
//TODO: To be fixed by #1502
TEST_F(SignatureBasedEqualQueryMergerRuleTest,
       DISABLED_testMergingEqualQueriesWithMergeOperatorsAndMultipleDistinctWatermarkAssigner) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with distinct queries with unionWith operator.
 * Each query has two sources with different watermark strategies. One source with IngestionTimeWatermarkStrategy and other
 * with EventTimeWatermarkStrategy and in the second query the strategies are inverted.
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingDistinctQueriesWithMergeOperatorsAndMultipleDistinctWatermarkAssigner) {
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
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithJoinOperator) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 =
        Query::from("car").joinWith(subQuery1, Attribute("value"), Attribute("value"), windowType1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("truck");
    Query query2 =
        Query::from("car").joinWith(subQuery2, Attribute("value"), Attribute("value"), windowType2).sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
//TODO: To be fixed by #1502
TEST_F(SignatureBasedEqualQueryMergerRuleTest, DISABLED_testMergingQueriesWithJoinOperatorWithDifferentStreamOrder) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 =
        Query::from("car").joinWith(subQuery1, Attribute("value"), Attribute("value"), windowType1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("car");
    Query query2 =
        Query::from("truck").joinWith(subQuery2, Attribute("value"), Attribute("value"), windowType2).sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithJoinOperatorWithDifferentButEqualWindows) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 =
        Query::from("car").joinWith(subQuery1, Attribute("value"), Attribute("value"), windowType1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(4));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("truck");
    Query query2 =
        Query::from("car").joinWith(subQuery2, Attribute("value"), Attribute("value"), windowType2).sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 1);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with same join operators.
 */
TEST_F(SignatureBasedEqualQueryMergerRuleTest, testMergingQueriesWithJoinOperatorWithDifferentWindows) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));

    Query subQuery1 = Query::from("truck");
    Query query1 =
        Query::from("car").joinWith(subQuery1, Attribute("value"), Attribute("value"), windowType1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));
    auto aggregation2 = Sum(Attribute("value"));

    Query subQuery2 = Query::from("truck");
    Query query2 =
        Query::from("car").joinWith(subQuery2, Attribute("value"), Attribute("value"), windowType2).sink(printSinkDescriptor);

    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedGQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_TRUE(updatedGQMToDeploy.size() == 2);
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}
