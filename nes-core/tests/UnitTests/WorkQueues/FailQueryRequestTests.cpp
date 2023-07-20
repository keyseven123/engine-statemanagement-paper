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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/Identifiers.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/RPCQueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <NesBaseTest.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/PlacementStrategy.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace std;

namespace NES {

using namespace Configurations;

class FailQueryRequestTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG); }
    static void TearDownTestCase() {
        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        remove(inputSequence.c_str());
    }

    void createDataStructures() {
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
        topology = Topology::create();
        globalQueryPlan = GlobalQueryPlan::create();
        globalExecutionPlan = GlobalExecutionPlan::create();
        std::map<const Compiler::Language, std::shared_ptr<const Compiler::LanguageCompiler>> compilerMap = {
            {Compiler::Language::CPP, Compiler::CPPCompiler::create()}};
        auto jitCompiler = std::make_shared<Compiler::JITCompiler>(compilerMap, false);
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        auto z3context = std::make_shared<z3::context>();
        const auto coordinatorConfig = CoordinatorConfiguration::createDefault();
        udfCatalog = std::shared_ptr<Catalogs::UDF::UDFCatalog>(Catalogs::UDF::UDFCatalog::create().release());
        globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                   queryCatalogService,
                                                                                   sourceCatalog,
                                                                                   globalQueryPlan,
                                                                                   z3context,
                                                                                   coordinatorConfig,
                                                                                   udfCatalog,
                                                                                   globalExecutionPlan);
        syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
        sourceCatalogService = std::make_shared<SourceCatalogService>(sourceCatalog);
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    }

    void populateTopology() {
        TopologyNodeId id = 1;
        std::string address = "localhost";
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;
        uint64_t resources = 4;
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        rootNode = TopologyNode::create(id, address, grpcPort, dataPort, resources, properties);
        topology->setAsRoot(rootNode);
        ++id;
        ++grpcPort;
        ++dataPort;
        worker2 = TopologyNode::create(id, address, grpcPort, dataPort, resources, properties);
        topology->addNewTopologyNodeAsChild(rootNode, worker2);
    }

    void deployQuery() {
        //register source
        sourceCatalog->addLogicalSource("test", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");
        sourceCatalogService->registerPhysicalSource(worker2, "physical_test", "test");

        std::string outputFilePath = getTestResourceFolder() / "failQueryRequestTest.out";
        string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
            + R"(", "CSV_FORMAT", "APPEND"));)";
        const auto placementStrategy = Optimizer::PlacementStrategy::BottomUp;
        const auto placementStratedyName = "BottomUp";
        queryId = 1;
        const auto lineage = LineageType::IN_MEMORY;
        auto queryPlan = syntacticQueryValidation->validate(query);
        queryPlan->setQueryId(queryId);
        queryPlan->setLineageType(lineage);
        queryCatalogService->createNewEntry(query, queryPlan, placementStratedyName);
        const auto runRequest = AddQueryRequest::create(queryPlan, placementStrategy);

        globalQueryPlanUpdatePhase->execute({runRequest});

        auto sharedQueryPlanToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
        ASSERT_EQ(sharedQueryPlanToDeploy.size(), 1);

        auto sharedQueryPlan = sharedQueryPlanToDeploy.front();

        //Fetch the shared query plan id
        sharedQueryId = sharedQueryPlan->getId();
        NES_DEBUG("Updating Query Plan with global query id : {}", sharedQueryId);

        //Check If the shared query plan is newly created
        ASSERT_EQ(SharedQueryPlanStatus::Created, sharedQueryPlan->getStatus());

        //Perform placement of new shared query plan
        NES_DEBUG("QueryProcessingService: Performing Operator placement for shared query plan");
        bool placementSuccessful = queryPlacementPhase->execute(sharedQueryPlan);
        ASSERT_TRUE(placementSuccessful);

        //skip deployment

        std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
        ASSERT_FALSE(executionNodes.empty());

        //Remove the old mapping of the shared query plan
        if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {
            queryCatalogService->removeSharedQueryPlanMapping(sharedQueryId);
        }

        //Reset all sub query plan metadata in the catalog
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalogService->resetSubQueryMetaData(queryId);
            queryCatalogService->mapSharedQueryPlanId(sharedQueryId, queryId);
        }

        //Add sub query plan metadata in the catalog
        for (auto& executionNode : executionNodes) {
            auto workerId = executionNode->getId();
            auto subQueryPlans = executionNode->getQuerySubPlans(sharedQueryId);
            for (auto& subQueryPlan : subQueryPlans) {
                QueryId querySubPlanId = subQueryPlan->getQuerySubPlanId();
                for (auto& queryId : sharedQueryPlan->getQueryIds()) {
                    queryCatalogService->addSubQueryMetaData(queryId, querySubPlanId, workerId);
                }
            }
        }

        //Mark queries as deployed
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalogService->updateQueryStatus(queryId, QueryStatus::DEPLOYED, "");
        }

        //Mark queries as running
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalogService->updateQueryStatus(queryId, QueryStatus::RUNNING, "");
        }

        //Update the shared query plan as deployed
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::Deployed);

        ASSERT_EQ(queryCatalogService->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::RUNNING);
        ASSERT_NE(globalQueryPlan->getSharedQueryId(queryId), INVALID_SHARED_QUERY_ID);
        ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
        ASSERT_EQ(globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Deployed);
    }
    std::shared_ptr<Catalogs::Query::QueryCatalog> queryCatalog;
    QueryCatalogServicePtr queryCatalogService;
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    SourceCatalogServicePtr sourceCatalogService;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;
    Optimizer::GlobalQueryPlanUpdatePhasePtr globalQueryPlanUpdatePhase;
    Optimizer::QueryPlacementPhasePtr queryPlacementPhase;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    QueryId queryId;
    SharedQueryId sharedQueryId;

    TopologyNodePtr rootNode;
    TopologyNodePtr worker2;
};

//test successful execution of fail query request for a single query until the undeployment step, which cannot be done in a unit test
TEST_F(FailQueryRequestTest, testValidFailRequestNoSubPlanSpecified) {
    constexpr RequestId requestId = 1;
    constexpr uint8_t maxRetries = 1;
    createDataStructures();
    populateTopology();

    deployQuery();

    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    std::promise<Experimental::FailQueryResponse> promise;
    auto future = promise.get_future();
    auto failQueryRequest = Experimental::FailQueryRequest::create(requestId,
                                                                   queryId,
                                                                   INVALID_QUERY_SUB_PLAN_ID,
                                                                   maxRetries,
                                                                   workerRpcClient,
                                                                   std::move(promise));
    TwoPhaseLockingStorageHandler storageHandler(globalExecutionPlan,
                                                 topology,
                                                 queryCatalogService,
                                                 globalQueryPlan,
                                                 sourceCatalog,
                                                 udfCatalog);
    auto thread = std::make_shared<std::thread>([&failQueryRequest, &storageHandler, this]() {
        try {
            failQueryRequest->execute(storageHandler);
        } catch (Exceptions::RPCQueryUndeploymentException& e) {
            ASSERT_EQ(e.getMode(), RpcClientModes::Stop);
            const auto failedCallNodeIds = e.getFailedExecutionNodeIds();
            ASSERT_EQ(failedCallNodeIds.size(), 2);
            ASSERT_NE(std::find(failedCallNodeIds.cbegin(), failedCallNodeIds.cend(), rootNode->getId()),
                      failedCallNodeIds.cend());
            ASSERT_NE(std::find(failedCallNodeIds.cbegin(), failedCallNodeIds.cend(), worker2->getId()),
                      failedCallNodeIds.cend());

            //expect the query to be marked for failure and not failed, because the deployment did not succeed
            EXPECT_EQ(queryCatalogService->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::MARKED_FOR_FAILURE);
            auto entry = queryCatalogService->getAllQueryCatalogEntries()[queryId];
            EXPECT_EQ(entry->getQueryStatus(), QueryStatus::MARKED_FOR_FAILURE);
            for (const auto& subQueryPlanMetaData : entry->getAllSubQueryPlanMetaData()) {
                EXPECT_EQ(subQueryPlanMetaData->getSubQueryStatus(), QueryStatus::MARKED_FOR_FAILURE);
            }

            EXPECT_EQ(globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Failed);
        }
    });
    thread->join();
    ASSERT_EQ(future.get().sharedQueryId, sharedQueryId);
}

//test error handling if a fail query request is executed but no query with the supplied id exists
TEST_F(FailQueryRequestTest, testInvalidQueryId) {
    constexpr RequestId requestId = 1;
    constexpr uint8_t maxRetries = 1;
    createDataStructures();
    populateTopology();

    deployQuery();

    const auto nonExistentId = queryId + 1;
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    std::promise<Experimental::FailQueryResponse> promise;
    auto failQueryRequest = Experimental::FailQueryRequest::create(requestId,
                                                                   nonExistentId,
                                                                   INVALID_QUERY_SUB_PLAN_ID,
                                                                   maxRetries,
                                                                   workerRpcClient,
                                                                   std::move(promise));
    TwoPhaseLockingStorageHandler storageHandler(globalExecutionPlan,
                                                 topology,
                                                 queryCatalogService,
                                                 globalQueryPlan,
                                                 sourceCatalog,
                                                 udfCatalog);
    EXPECT_THROW(failQueryRequest->execute(storageHandler), Exceptions::QueryNotFoundException);
}

//test error handling when trying to let a query fail after it has already been set to the status STOPPED
TEST_F(FailQueryRequestTest, testWrongQueryStatus) {
    constexpr RequestId requestId = 1;
    constexpr uint8_t maxRetries = 1;
    createDataStructures();
    populateTopology();

    deployQuery();

    //set the query status to stopped, it should not be possible to fail a stopped query
    queryCatalogService->getEntryForQuery(queryId)->setQueryStatus(QueryStatus::STOPPED);

    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    std::promise<Experimental::FailQueryResponse> promise;
    auto failQueryRequest = Experimental::FailQueryRequest::create(requestId,
                                                                   queryId,
                                                                   INVALID_QUERY_SUB_PLAN_ID,
                                                                   maxRetries,
                                                                   workerRpcClient,
                                                                   std::move(promise));

    TwoPhaseLockingStorageHandler storageHandler(globalExecutionPlan,
                                                 topology,
                                                 queryCatalogService,
                                                 globalQueryPlan,
                                                 sourceCatalog,
                                                 udfCatalog);
    EXPECT_THROW(failQueryRequest->execute(storageHandler), Exceptions::InvalidQueryStatusException);
}
}// namespace NES