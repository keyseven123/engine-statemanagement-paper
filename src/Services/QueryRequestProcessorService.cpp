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

#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryReconfigurationException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryPlacementRefinementPhase.hpp>
#include <Phases/QueryReconfigurationPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <exception>

namespace NES {

QueryRequestProcessorService::QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                           QueryCatalogPtr queryCatalog, GlobalQueryPlanPtr globalQueryPlan,
                                                           StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient,
                                                           QueryRequestQueuePtr queryRequestQueue, bool enableQueryMerging,
                                                           bool enableQueryReconfiguration)
    : queryProcessorStatusLock(), queryProcessorRunning(true), queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue),
      globalQueryPlan(globalQueryPlan), enableQueryMerging(enableQueryMerging),
      enableQueryReconfiguration(enableQueryReconfiguration) {

    NES_DEBUG("QueryRequestProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create(streamCatalog);
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient);
    queryReconfigurationPhase = QueryReconfigurationPhase::create(topology, globalExecutionPlan, workerRpcClient, streamCatalog);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
    queryPlacementRefinementPhase = QueryPlacementRefinementPhase::create(globalExecutionPlan);
    queryMergerPhase = QueryMergerPhase::create();
}

QueryRequestProcessorService::~QueryRequestProcessorService() { NES_DEBUG("~QueryRequestProcessorService()"); }

void QueryRequestProcessorService::start() {

    try {
        while (isQueryProcessorRunning()) {
            NES_INFO("QueryRequestProcessorService: Waiting for new query request trigger");
            const std::vector<QueryCatalogEntry> queryCatalogEntryBatch = queryRequestQueue->getNextBatch();
            NES_INFO("QueryProcessingService: Found " << queryCatalogEntryBatch.size() << " query requests to process");
            //process the queries using query-at-a-time model
            for (auto queryCatalogEntry : queryCatalogEntryBatch) {
                QueryId queryId = queryCatalogEntry.getQueryId();
                try {
                    if (queryCatalogEntry.getQueryStatus() == QueryStatus::MarkedForStop) {
                        NES_INFO("QueryProcessingService: Request received for stopping the query " << queryId);
                        globalQueryPlan->removeQuery(queryId);
                    } else if (queryCatalogEntry.getQueryStatus() == QueryStatus::Registered) {
                        auto queryPlan = queryCatalogEntry.getQueryPlan();
                        NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query "
                                 << queryId << " status=" << queryCatalogEntry.getQueryStatusAsString());
                        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);

                        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                        queryPlan = typeInferencePhase->execute(queryPlan);

                        NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " << queryId);
                        queryPlan = queryRewritePhase->execute(queryPlan);

                        if (!queryPlan) {
                            throw Exception("QueryProcessingService: Failed during query rewrite phase for query: "
                                            + std::to_string(queryId));
                        }

                        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                        queryPlan = typeInferencePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw Exception("QueryProcessingService: Failed during Type inference phase for query: "
                                            + std::to_string(queryId));
                        }

                        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                        globalQueryPlan->addQueryPlan(queryPlan);
                    } else {
                        NES_ERROR("QueryProcessingService: Request received for query with status "
                                  << queryCatalogEntry.getQueryStatus() << " ");
                        throw InvalidQueryStatusException({QueryStatus::MarkedForStop, QueryStatus::Scheduling},
                                                          queryCatalogEntry.getQueryStatus());
                    }

                    if (enableQueryMerging) {
                        NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
                        queryMergerPhase->execute(globalQueryPlan);
                    }
                    auto sharedQueryMetaDataToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
                    for (auto sharedQueryMetaData : sharedQueryMetaDataToDeploy) {
                        SharedQueryId sharedQueryId = sharedQueryMetaData->getSharedQueryId();
                        NES_DEBUG("QueryProcessingService: Updating Query Plan with global query id : " << sharedQueryId);

                        if ((!enableQueryReconfiguration || sharedQueryMetaData->isEmpty()) && !sharedQueryMetaData->isNew()) {
                            NES_DEBUG("QueryProcessingService: Undeploying Query Plan with global query id : " << sharedQueryId);
                            bool successful = queryUndeploymentPhase->execute(sharedQueryId);
                            if (!successful) {
                                throw QueryUndeploymentException("Unable to stop Global QueryId "
                                                                 + std::to_string(sharedQueryId));
                            }
                        }
                        if ((sharedQueryMetaData->isNew() || !enableQueryReconfiguration) && !sharedQueryMetaData->isEmpty()) {

                            auto queryPlan = sharedQueryMetaData->getQueryPlan();

                            NES_DEBUG(
                                "QueryProcessingService: Performing Query Operator placement for query with shared query id : "
                                << sharedQueryId);
                            std::string placementStrategy = queryCatalogEntry.getQueryPlacementStrategy();
                            bool placementSuccessful = queryPlacementPhase->execute(placementStrategy, queryPlan);
                            if (!placementSuccessful) {
                                throw QueryPlacementException("QueryProcessingService: Failed to perform query placement for "
                                                              "query plan with shared query id: "
                                                              + std::to_string(sharedQueryId));
                            }
                            bool successful = queryDeploymentPhase->execute(sharedQueryId);
                            if (!successful) {
                                throw QueryDeploymentException(
                                    "QueryRequestProcessingService: Failed to deploy query with global query Id "
                                    + std::to_string(sharedQueryId));
                            }
                        }
                        if (enableQueryReconfiguration && !sharedQueryMetaData->isEmpty() && !sharedQueryMetaData->isNew()) {
                            auto queryPlan = sharedQueryMetaData->getQueryPlan();
                            bool successful = queryReconfigurationPhase->execute(queryPlan);
                            if (!successful) {
                                throw QueryReconfigurationException(
                                    "QueryRequestProcessingService: Failed to reconfigure query with global query Id "
                                    + std::to_string(sharedQueryId) + " when attempting for query Id " + std::to_string(queryId));
                            }
                        }
                        //Mark the meta data as deployed
                        sharedQueryMetaData->markAsDeployed();
                    }

                    if (queryCatalogEntry.getQueryStatus() == QueryStatus::Registered) {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                    } else {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
                    }
                } catch (QueryPlacementException& ex) {
                    NES_ERROR("QueryRequestProcessingService QueryPlacementException: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryDeploymentException& ex) {
                    NES_ERROR("QueryRequestProcessingService QueryDeploymentException: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryReconfigurationException& ex) {
                    NES_ERROR("QueryRequestProcessingService QueryReconfigurationException: " << ex.what());
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (TypeInferenceException& ex) {
                    NES_ERROR("QueryRequestProcessingService TypeInferenceException: " << ex.what());
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (InvalidQueryStatusException& ex) {
                    NES_ERROR("QueryRequestProcessingService InvalidQueryStatusException: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryNotFoundException& ex) {
                    NES_ERROR("QueryRequestProcessingService QueryNotFoundException: " << ex.what());
                } catch (QueryUndeploymentException& ex) {
                    NES_ERROR("QueryRequestProcessingService QueryUndeploymentException: " << ex.what());
                } catch (Exception& ex) {
                    NES_ERROR("QueryRequestProcessingService Exception: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            }
        }
        NES_WARNING("QueryProcessingService: Terminated");
    } catch (std::exception& ex) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queries: " << ex.what());
        shutDown();
    }
}

bool QueryRequestProcessorService::isQueryProcessorRunning() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    return queryProcessorRunning;
}

void QueryRequestProcessorService::shutDown() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    this->queryProcessorRunning = false;
    queryRequestQueue->insertPoisonPill();
}

}// namespace NES