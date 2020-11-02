#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryPlacementRefinementPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>

namespace NES {

QueryRequestProcessorService::QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, QueryCatalogPtr queryCatalog, GlobalQueryPlanPtr globalQueryPlan,
                                                           StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient, QueryRequestQueuePtr queryRequestQueue, bool enableQueryMerging)
    : queryProcessorStatusLock(), queryProcessorRunning(true), queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue), enableQueryMerging(enableQueryMerging), globalQueryPlan(globalQueryPlan) {

    NES_DEBUG("QueryRequestProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create(streamCatalog);
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
    queryPlacementRefinementPhase = QueryPlacementRefinementPhase::create(globalExecutionPlan);
    queryMergerPhase = QueryMergerPhase::create();
}

QueryRequestProcessorService::~QueryRequestProcessorService() {
    NES_DEBUG("~QueryRequestProcessorService()");
}

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
                        NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query " << queryId << " status=" << queryCatalogEntry.getQueryStatusAsString());
                        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);

                        NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " << queryId);
                        queryPlan = queryRewritePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw Exception("QueryProcessingService: Failed during query rewrite phase for query: " + queryId);
                        }

                        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                        queryPlan = typeInferencePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw Exception("QueryProcessingService: Failed during Type inference phase for query: " + queryId);
                        }

                        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                        globalQueryPlan->addQueryPlan(queryPlan);
                    } else {
                        NES_ERROR("QueryProcessingService: Request received for query with status " << queryCatalogEntry.getQueryStatus() << " ");
                        throw InvalidQueryStatusException({QueryStatus::MarkedForStop, QueryStatus::Scheduling}, queryCatalogEntry.getQueryStatus());
                    }

                    if (enableQueryMerging) {
                        NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
                        queryMergerPhase->execute(globalQueryPlan);
                    }

                    if (!globalQueryPlan->updateGlobalQueryMetaDataMap()) {
                        NES_ERROR("QueryProcessingService: Failed to update Global Query Metadata.");
                        throw Exception("QueryProcessingService: Failed to update Global Query Metadata.");
                    }

                    std::vector<GlobalQueryMetaDataPtr> listOfGlobalQueryMetaData = globalQueryPlan->getGlobalQueryMetaDataToDeploy();
                    for (auto globalQueryMetaData : listOfGlobalQueryMetaData) {
                        GlobalQueryId globalQueryId = globalQueryMetaData->getGlobalQueryId();
                        NES_DEBUG("QueryProcessingService: Updating Query Plan with global query id : " << globalQueryId);

                        if (!globalQueryMetaData->isNew()) {
                            NES_DEBUG("QueryProcessingService: Undeploying Query Plan with global query id : " << globalQueryId);
                            bool successful = queryUndeploymentPhase->execute(globalQueryId);
                            if (!successful) {
                                throw QueryUndeploymentException("Unable to stop Global QueryId " + globalQueryId);
                            }
                        }

                        if (!globalQueryMetaData->isEmpty()) {

                            auto queryPlan = globalQueryMetaData->getQueryPlan();

                            NES_DEBUG("QueryProcessingService: Performing Query Operator placement for query with global query id : " << globalQueryId);
                            std::string placementStrategy = queryCatalogEntry.getQueryPlacementStrategy();
                            bool placementSuccessful = queryPlacementPhase->execute(placementStrategy, queryPlan);
                            if (!placementSuccessful) {
                                throw QueryPlacementException("QueryProcessingService: Failed to perform query placement for query plan with global query id: " + globalQueryId);
                            }

                            bool successful = queryDeploymentPhase->execute(globalQueryId);
                            if (!successful) {
                                throw QueryDeploymentException("QueryRequestProcessingService: Failed to deploy query with global query Id " + globalQueryId);
                            }
                        }
                        //Mark the meta data as deployed
                        globalQueryMetaData->markAsDeployed();
                        globalQueryMetaData->setAsOld();
                    }

                    if (queryCatalogEntry.getQueryStatus() == QueryStatus::Registered) {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                    } else {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
                    }

                } catch (QueryPlacementException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryDeploymentException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (InvalidQueryStatusException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryNotFoundException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                } catch (QueryUndeploymentException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                } catch (Exception& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            }
        }
        NES_WARNING("QueryProcessingService: Terminated");
    } catch (...) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queries.");
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