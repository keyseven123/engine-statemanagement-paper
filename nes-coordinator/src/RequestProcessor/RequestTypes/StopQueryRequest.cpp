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

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <string>
#include <utility>

namespace NES::RequestProcessor::Experimental {

StopQueryRequest::StopQueryRequest(QueryId queryId, uint8_t maxRetries)
    : AbstractRequest({ResourceType::QueryCatalogService,
                       ResourceType::GlobalExecutionPlan,
                       ResourceType::Topology,
                       ResourceType::GlobalQueryPlan,
                       ResourceType::UdfCatalog,
                       ResourceType::SourceCatalog,
                       ResourceType::CoordinatorConfiguration},
                      maxRetries),
      queryId(queryId) {}

StopQueryRequestPtr StopQueryRequest::create(QueryId queryId, uint8_t maxRetries) {
    return std::make_shared<StopQueryRequest>(queryId, maxRetries);
}

std::vector<AbstractRequestPtr> StopQueryRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
    NES_TRACE("Start Stop Request logic.");
    std::vector<AbstractRequestPtr> failureRequests = {};
    try {
        NES_TRACE("Acquire Resources.");
        globalExecutionPlan = storageHandler->getGlobalExecutionPlanHandle(requestId);
        topology = storageHandler->getTopologyHandle(requestId);
        queryCatalogService = storageHandler->getQueryCatalogServiceHandle(requestId);
        globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);
        udfCatalog = storageHandler->getUDFCatalogHandle(requestId);
        sourceCatalog = storageHandler->getSourceCatalogHandle(requestId);
        NES_TRACE("Locks acquired. Create Phases");
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
        queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan);
        NES_TRACE("Phases created. Stop request initialized.");

        if (queryId == INVALID_SHARED_QUERY_ID) {
            throw Exceptions::QueryNotFoundException("Cannot stop query with invalid query id " + std::to_string(queryId)
                                                     + ". Please enter a valid query id.");
        }
        //mark single query for hard stop
        if (!queryCatalogService->checkAndMarkForHardStop(queryId)) {
            throw Exceptions::InvalidQueryStateException({QueryState::OPTIMIZING,
                                                          QueryState::REGISTERED,
                                                          QueryState::DEPLOYED,
                                                          QueryState::RUNNING,
                                                          QueryState::RESTARTING},
                                                         queryCatalogService->getEntryForQuery(queryId)->getQueryState());
        }
        auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            throw Exceptions::QueryNotFoundException("Could not find a a valid shared query plan for query with id "
                                                     + std::to_string(queryId) + " in the global query plan");
        }
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        if (!sharedQueryPlan) {
            throw Exceptions::QueryNotFoundException("Could not find a a valid shared query plan for query with id "
                                                     + std::to_string(queryId) + " in the global query plan");
        }
        //undeploy SQP
        queryUndeploymentPhase->execute(sharedQueryId, sharedQueryPlan->getStatus());
        //remove single query from global query plan
        globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
        if (SharedQueryPlanStatus::Stopped == sharedQueryPlan->getStatus()) {
            //Mark all contained queryIdAndCatalogEntryMapping as stopped
            for (auto& involvedQueryIds : sharedQueryPlan->getQueryIds()) {
                queryCatalogService->updateQueryStatus(involvedQueryIds, QueryState::STOPPED, "Hard Stopped");
            }
            globalQueryPlan->removeSharedQueryPlan(sharedQueryId);
        } else if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {
            //Perform placement of updated shared query plan
            NES_DEBUG("QueryProcessingService: Performing Operator placement for shared query plan");
            bool placementSuccessful = queryPlacementPhase->execute(sharedQueryPlan);
            if (!placementSuccessful) {
                throw Exceptions::QueryPlacementException(sharedQueryId,
                                                          "QueryProcessingService: Failed to perform query placement for "
                                                          "query plan with shared query id: "
                                                              + std::to_string(sharedQueryId));
            }

            //Perform deployment of re-placed shared query plan
            queryDeploymentPhase->execute(sharedQueryPlan);

            //Update the shared query plan as deployed
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::Deployed);
        }

        //todo: #3742 FIXME: This is a work-around for an edge case. To reproduce this:
        // 1. The query merging feature is enabled.
        // 2. A query from a shared query plan was removed but over all shared query plan is still serving other queryIdAndCatalogEntryMapping (Case 3.1).
        // Expected Result:
        //  - Query status of the removed query is marked as stopped.
        // Actual Result:
        //  - Query status of the removed query will not be set to stopped and the query will remain in MarkedForHardStop.
        queryCatalogService->updateQueryStatus(queryId, QueryState::STOPPED, "Hard Stopped");

    } catch (RequestExecutionException& e) {
        auto requests = handleError(e, storageHandler);
        failureRequests.insert(failureRequests.end(), requests.begin(), requests.end());
    }
    return failureRequests;
}

void StopQueryRequest::postExecution([[maybe_unused]] const StorageHandlerPtr& storageHandler) { NES_TRACE("Release locks."); }

std::string StopQueryRequest::toString() { return "StopQueryRequest { QueryId: " + std::to_string(queryId) + "}"; }

void StopQueryRequest::preRollbackHandle(const RequestExecutionException& ex,
                                         [[maybe_unused]] const StorageHandlerPtr& storageHandle) {
    NES_TRACE("Error: {}", ex.what());
}

void StopQueryRequest::postRollbackHandle(const RequestExecutionException& ex,
                                          [[maybe_unused]] const StorageHandlerPtr& storageHandle) {
    NES_TRACE("Error: {}", ex.what());
}

std::vector<AbstractRequestPtr> StopQueryRequest::rollBack(RequestExecutionException& ex,
                                                           const StorageHandlerPtr& storageHandler) {
    std::vector<AbstractRequestPtr> failRequest;
    try {
        NES_TRACE("Error: {}", ex.what());
        if (ex.instanceOf<Exceptions::QueryPlacementException>()) {
            NES_ERROR("{}", ex.what());
            std::promise<Experimental::FailQueryResponse> failPromise;
            failRequest.push_back(FailQueryRequest::create(ex.getQueryId(), INVALID_QUERY_SUB_PLAN_ID, MAX_RETRIES_FOR_FAILURE));
        } else if (ex.instanceOf<QueryDeploymentException>() || ex.instanceOf<InvalidQueryException>()) {
            //Happens if:
            //1. InvalidQueryException: inside QueryDeploymentPhase, if the query sub-plan metadata already exists in the query catalog --> non-recoverable
            //todo: #3821 change to more specific exceptions, remove QueryDeploymentException
            //2. QueryDeploymentException The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF
            //3. QueryDeploymentException: Error in call to Elegant acceleration service with code
            //4. QueryDeploymentException: QueryDeploymentPhase : unable to find query sub plan with id
            std::promise<Experimental::FailQueryResponse> failPromise;
            failRequest.push_back(FailQueryRequest::create(ex.getQueryId(), INVALID_QUERY_SUB_PLAN_ID, MAX_RETRIES_FOR_FAILURE));
        } else if (ex.instanceOf<TypeInferenceException>() || ex.instanceOf<Exceptions::QueryUndeploymentException>()) {
            // In general, failures in QueryUndeploymentPhase are concerned with the current sqp id and a failure with a topology node
            // Therefore, for QueryUndeploymentException, we assume that the sqp is not running on any node, and we can set the sqp's status to stopped
            // we do this as long as there are retries present, otherwise, we fail the query
            NES_ERROR("{}", ex.what());
            queryCatalogService->updateQueryStatus(ex.getQueryId(), QueryState::FAILED, ex.what());
        } else if (ex.instanceOf<Exceptions::QueryNotFoundException>()
                   || ex.instanceOf<Exceptions::ExecutionNodeNotFoundException>()
                   || ex.instanceOf<Exceptions::InvalidQueryStateException>()) {
            //Happens if:
            //1. could not obtain execution nodes by shared query id --> non-recoverable
            //2. if check and mark for hard stop failed, means that stop is already in process, hence, we don't do anything
            //3. Could not find topology node to release resources
            //4. Could not find sqp in global query plan
            //5. Could not remove query sub plan from execution node
            //--> SQP is not running on any nodes:
            //log the error to let the user know
            //no other action necessary
            NES_ERROR("{}", ex.what());
        } else if (ex.instanceOf<Exceptions::RuntimeException>()) {
            //todo: #3821 change to more specific exceptions
            //1. Called from QueryUndeploymentPhase: GlobalQueryPlan: Unable to remove all child operators of the identified sink operator in the shared query plan
            //2. Called from PlacementStrategyPhase: PlacementStrategyFactory: Unknown placement strategy
            NES_ERROR("RuntimeException: {}", ex.what());
        } else {
            //todo: #3821 retry for these errors, add specific rpcCallException and retry failed part, differentiate between deployment and undeployment phase
            //RPC call errors:
            //1. asynchronous call to worker to stop shared query plan failed --> currently invokes NES_THROW_RUNTIME_ERROR;
            //2. asynchronous call to worker to unregister shared query plan failed --> currently invokes NES_THROW_RUNTIME_ERROR:
            NES_ERROR("Unknown exception: {}", ex.what());
        }
    } catch (RequestExecutionException& e) {
        if (retry()) {
            handleError(e, storageHandler);
        } else {
            NES_ERROR("StopQueryRequest: Final failure to rollback. No retries left. Error: {}", e.what());
        }
    }
    return failRequest;
}
}// namespace NES::RequestProcessor::Experimental
 // namespace NES