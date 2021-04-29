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

#include <API/Query.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

namespace NES {

class GlobalQueryPlanUpdatePhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("GlobalQueryPlanUpdatePhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanUpdatePhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() { context = std::make_shared<z3::context>(); }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Tear down GlobalQueryPlanUpdatePhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down GlobalQueryPlanUpdatePhaseTest test class."); }

    z3::ContextPtr context;
};

/**
 * @brief In this test we execute query merger phase on a single invalid query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, DISABLED_executeQueryMergerPhaseForSingleInvalidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query without assigning it a query id.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto queryCatalog = std::make_shared<QueryCatalog>();
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule);
    auto catalogEntry1 = QueryCatalogEntry(INVALID_QUERY_ID, "", "topdown", q1.getQueryPlan(), Scheduling);
    auto request = RunQueryRequest::create(catalogEntry1.getQueryPlan(), catalogEntry1.getQueryPlacementStrategy());
    std::vector<NESRequestPtr> batchOfQueryRequests = {request};
    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForSingleQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    auto queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQuery(queryString, q1.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule);
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto request = RunQueryRequest::create(catalogEntry1->getQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> batchOfQueryRequests = {request};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on same valid query plan twice.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForDuplicateValidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    auto queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQuery(queryString, q1.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto request = RunQueryRequest::create(catalogEntry1->getQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> nesRequests = {request, request};
    //Assert
    EXPECT_THROW(phase->execute(nesRequests), GlobalQueryPlanUpdateException);
}

/**
 * brief In this test we execute query merger phase on multiple valid query plans.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryPlan) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queries.");
    auto queryString1 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryString2 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQuery(queryString1, q1.getQueryPlan(), "TopDown");
    queryCatalog->addNewQuery(queryString2, q2.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    auto request1 = RunQueryRequest::create(catalogEntry1->getQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto request2 = RunQueryRequest::create(catalogEntry2->getQueryPlan(), catalogEntry2->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> requests = {request1, request2};
    auto resultPlan = phase->execute(requests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
*  @brief In this test we execute query merger phase on a valid query plan with invalid status.
*/
TEST_F(GlobalQueryPlanUpdatePhaseTest, DISABLED_executeQueryMergerPhaseForAValidQueryPlanInInvalidState) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    int queryId = 1;
    q1.getQueryPlan()->setQueryId(queryId);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry1 = queryCatalog->addNewQuery("", q1.getQueryPlan(), "topdown");
    //Explicitly fail the query
    queryCatalog->setQueryFailureReason(queryId, "Random reason");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto nesRequest1 = RunQueryRequest::create(catalogEntry1->getQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> batchOfQueryRequests = {nesRequest1};

    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief  In this test we execute query merger phase on multiple query requests with add and removal.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryRequestsWithAddAndRemoval) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queries.");
    auto queryString1 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryString2 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQuery(queryString1, q1.getQueryPlan(), "TopDown");
    queryCatalog->addNewQuery(queryString2, q2.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    queryCatalog->stopQuery(2);
    auto catalogEntry3 = queryCatalog->getQueryCatalogEntry(2);

    auto nesRequest1 = RunQueryRequest::create(catalogEntry1->getQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest2 = RunQueryRequest::create(catalogEntry2->getQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest3 = StopQueryRequest::create(catalogEntry2->getQueryPlan()->getQueryId());

    std::vector<NESRequestPtr> batchOfQueryRequests = {nesRequest1, nesRequest2, nesRequest3};
    auto resultPlan = phase->execute(batchOfQueryRequests);
    resultPlan->removeEmptySharedQueryMetaData();

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_EQ(sharedQueryMetadataToDeploy.size(), 1);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, queryMergerPhaseForSingleQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    auto queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";

    auto queryCatalog = std::make_shared<QueryCatalog>();
    for (int i = 1; i <= 10; i++) {
        NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
        auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
        q1.getQueryPlan()->setQueryId(i);
        queryCatalog->addNewQuery(queryString, q1.getQueryPlan(), "TopDown");
    }

    std::vector<NESRequestPtr> batchOfNesRequests;
    auto allQueries = queryCatalog->getAllQueryCatalogEntries();
    for (auto& [key, value] : allQueries) {
        auto nesRequest = RunQueryRequest::create(value->getQueryPlan(), value->getQueryPlacementStrategy());
        batchOfNesRequests.emplace_back(nesRequest);
    }

    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto resultPlan = phase->execute(batchOfNesRequests);
    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    globalQueryPlan->removeEmptySharedQueryMetaData();
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, queryMergerPhaseForSingleQueryPlan1) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    auto queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";

    auto queryCatalog = std::make_shared<QueryCatalog>();
    for (int i = 1; i <= 1; i++) {
        NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
        auto q1 = Query::from("example")
                      .filter(Attribute("X") <= Attribute("Y"))
                      .map(Attribute("id") = Attribute("id") / 1)
                      .map(Attribute("Y") = Attribute("Y") - 2)
                      .map(Attribute("NEW_id2") = Attribute("Y") / Attribute("Y"))
                      .filter(Attribute("val") < 36)
                      .filter(Attribute("Y") >= 49)
                      .unionWith(&Query::from("example")
                                      .filter(Attribute("X") <= Attribute("Y"))
                                      .map(Attribute("id") = Attribute("id") / 1)
                                      .map(Attribute("Y") = Attribute("Y") - 2)
                                      .map(Attribute("NEW_id2") = Attribute("Y") / Attribute("Y"))
                                      .filter(Attribute("val") < 36)
                                      .filter(Attribute("Y") >= 49))
                      .sink(NullOutputSinkDescriptor::create());

        q1.getQueryPlan()->setQueryId(i);
        queryCatalog->addNewQuery(queryString, q1.getQueryPlan(), "TopDown");
    }

    std::vector<NESRequestPtr> batchOfNesRequests;
    auto allQueries = queryCatalog->getAllQueryCatalogEntries();
    for (auto& [key, value] : allQueries) {
        auto nesRequest = RunQueryRequest::create(value->getQueryPlan(), value->getQueryPlacementStrategy());
        batchOfNesRequests.emplace_back(nesRequest);
    }

    auto streamCatalog = std::make_shared<StreamCatalog>();
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::UINT64)
                                ->addField("val", NES::UINT64)
                                ->addField("X", NES::UINT64)
                                ->addField("Y", NES::UINT64);

    streamCatalog->addLogicalStream("example", schema);

    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, context, true,
                                                    Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);

    auto resultPlan = phase->execute(batchOfNesRequests);
    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    globalQueryPlan->removeEmptySharedQueryMetaData();
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}
}// namespace NES
