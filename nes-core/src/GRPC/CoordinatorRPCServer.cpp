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

#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Services/LocationService.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Services/ReplicationService.hpp>
#include <Services/SourceCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectPoint.hpp>
#include <Util/Experimental/SpatialTypeUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <utility>

using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer(QueryServicePtr queryService,
                                           TopologyManagerServicePtr topologyManagerService,
                                           SourceCatalogServicePtr sourceCatalogService,
                                           QueryCatalogServicePtr queryCatalogService,
                                           Monitoring::MonitoringManagerPtr monitoringManager,
                                           ReplicationServicePtr replicationService,
                                           LocationServicePtr locationService)
    : queryService(std::move(queryService)), topologyManagerService(std::move(topologyManagerService)),
      sourceCatalogService(std::move(sourceCatalogService)), queryCatalogService(std::move(queryCatalogService)),
      monitoringManager(std::move(monitoringManager)), replicationService(std::move(replicationService)),
      locationService(std::move(locationService)){};

Status CoordinatorRPCServer::RegisterWorker(ServerContext*,
                                            const RegisterWorkerRequest* registrationRequest,
                                            RegisterWorkerReply* reply) {

    NES_DEBUG("Received worker registration request " << registrationRequest->DebugString());
    auto address = registrationRequest->address();
    auto grpcPort = registrationRequest->grpcport();
    auto dataPort = registrationRequest->dataport();
    auto slots = registrationRequest->numberofslots();
    //construct worker property from the request
    std::map<std::string, std::any> workerProperties;
    workerProperties[NES::Worker::Properties::MAINTENANCE] =
        false;//During registration, we assume the node is not under maintenance
    workerProperties[NES::Worker::Configuration::TENSORFLOW_SUPPORT] = registrationRequest->tfsupported();
    workerProperties[NES::Worker::Configuration::JAVA_UDF_SUPPORT] = registrationRequest->javaudfsupported();
    workerProperties[NES::Worker::Configuration::SPATIAL_SUPPORT] =
        NES::Spatial::Util::SpatialTypeUtility::protobufEnumToNodeType(registrationRequest->spatialtype());

    NES_DEBUG("TopologyManagerService::RegisterNode: request =" << registrationRequest);
    uint64_t workerId = topologyManagerService->registerWorker(address, grpcPort, dataPort, slots, workerProperties);

    NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation(registrationRequest->waypoint().geolocation().lat(),
                                                                   registrationRequest->waypoint().geolocation().lng());

    if (!topologyManagerService->addGeoLocation(workerId, std::move(geoLocation))) {
        NES_ERROR("Unable to update geo location of the topology");
        reply->set_workerid(0);
        return Status::CANCELLED;
    }

    auto registrationMetrics =
        std::make_shared<Monitoring::Metric>(Monitoring::RegistrationMetrics(registrationRequest->registrationmetrics()),
                                             Monitoring::MetricType::RegistrationMetric);
    registrationMetrics->getValue<Monitoring::RegistrationMetrics>().nodeId = workerId;
    monitoringManager->addMonitoringData(workerId, registrationMetrics);

    if (workerId != 0) {
        NES_DEBUG("CoordinatorRPCServer::RegisterNode: success id=" << workerId);
        reply->set_workerid(workerId);
        return Status::OK;
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterNode: failed");
    reply->set_workerid(0);
    return Status::CANCELLED;
}

Status
CoordinatorRPCServer::UnregisterWorker(ServerContext*, const UnregisterWorkerRequest* request, UnregisterWorkerReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: request =" << request);

    bool success = topologyManagerService->removeGeoLocation(request->workerid());
    if (success) {
        if (!topologyManagerService->unregisterNode(request->workerid())) {
            NES_ERROR("CoordinatorRPCServer::UnregisterNode: Worker was not removed");
            reply->set_success(false);
            return Status::CANCELLED;
        }
        monitoringManager->removeMonitoringNode(request->workerid());
        NES_DEBUG("CoordinatorRPCServer::UnregisterNode: Worker successfully removed");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterNode: Worker was not removed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterMonitoringPlan(ServerContext*, const RegisterMonitoringPlanRequest* request, RegisterMonitoringPlanReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterMonitoringPlan: request =" << request);

    std::map <Monitoring::MetricType, std::pair<SchemaPtr, uint64_t>> monitoringPlanMap;
    std::pair<SchemaPtr, uint64_t> pairTemp;
    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->id());
    SchemaPtr schema;
    Monitoring::MetricType metricType;
    std::list<uint64_t> coreList;
    for (const auto& monitoringPlanPart : request->monitoringplanpart()) {
        metricType = Monitoring::parse(monitoringPlanPart.metrictype());
        schema = Schema::parse(monitoringPlanPart.schema());
        pairTemp = std::make_pair(schema, monitoringPlanPart.samplerate());
        monitoringPlanMap[metricType] = pairTemp;
    }
    for (const auto& coreNum : request->coreslist()) {
        coreList.push_back(coreNum.corenum());
    }
    Monitoring::MonitoringPlanPtr monitoringPlan = Monitoring::MonitoringPlan::create(monitoringPlanMap, coreList);
    bool success = monitoringManager->registerMonitoringPlans(request->id(), monitoringPlan);
    if (!success) {
        NES_ERROR("CoordinatorRPCServer::RegisterMonitoringPlan failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterMonitoringPlan Succeed");
    reply->set_success(true);
    return Status::OK;
}

Status CoordinatorRPCServer::RegisterLogicalSourceName(ServerContext*, const RegisterLogicalSourceNameRequest* request, RegisterLogicalSourceNameReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSourceName: request =" << request);

    bool success = monitoringManager->insertLogicalSource(request->logicalsourcename());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSourceName: LogicalSource successfully inserted");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RegisterLogicalSourceName: LogicalSource already in set");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::LogicalSourceLookUp(ServerContext*, const LogicalSourceLookUpRequest* request, LogicalSourceLookUpReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::LogicalSourceLookUp: request =" << request);

    bool success = sourceCatalogService->logicalSourceLookUp(request->logicalsourcename());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::LogicalSourceLookUp: LogicalSource does already exist");
        reply->set_success(true);
        return Status::OK;
    }
    NES_INFO("CoordinatorRPCServer::LogicalSourceLookUp: LogicalSource does not exist yet");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterLogicalSourceV2(ServerContext*, const RegisterLogicalSourceV2Request* request
                                                      , RegisterLogicalSourceV2Reply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSourceNEW: request =" << request);

    SchemaPtr schema = Schema::parse(request->logicalsourceschema());
    bool success = sourceCatalogService->registerLogicalSource(request->logicalsourcename(), schema);
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSourceNEW: LogicalSource successfully registered");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RegisterLogicalSourceNEW: LogicalSource NOT successfully registered");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterPhysicalSource(ServerContext*,
                                                    const RegisterPhysicalSourcesRequest* request,
                                                    RegisterPhysicalSourcesReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource: request =" << request);
    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->workerid());
    for (const auto& physicalSourceDefinition : request->physicalsources()) {
        bool success = sourceCatalogService->registerPhysicalSource(physicalNode,
                                                                    physicalSourceDefinition.physicalsourcename(),
                                                                    physicalSourceDefinition.logicalsourcename());
        if (!success) {
            NES_ERROR("CoordinatorRPCServer::RegisterPhysicalSource failed");
            reply->set_success(false);
            return Status::CANCELLED;
        }
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource Succeed");
    reply->set_success(true);
    return Status::OK;
}

Status CoordinatorRPCServer::UnregisterPhysicalSource(ServerContext*,
                                                      const UnregisterPhysicalSourceRequest* request,
                                                      UnregisterPhysicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalSource: request =" << request);

    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->workerid());
    bool success =
        sourceCatalogService->unregisterPhysicalSource(physicalNode, request->physicalsourcename(), request->logicalsourcename());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalSource success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterPhysicalSource failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterLogicalSource(ServerContext*,
                                                   const RegisterLogicalSourceRequest* request,
                                                   RegisterLogicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSource: request =" << request);

    bool success = sourceCatalogService->registerLogicalSource(request->logicalsourcename(), request->sourceschema());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSource success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RegisterLogicalSource failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::UnregisterLogicalSource(ServerContext*,
                                                     const UnregisterLogicalSourceRequest* request,
                                                     UnregisterLogicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalSource: request =" << request);

    bool success = sourceCatalogService->unregisterLogicalSource(request->logicalsourcename());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalSource success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterLogicalSource failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::AddParent(ServerContext*, const AddParentRequest* request, AddParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::AddParent: request =" << request);

    bool success = topologyManagerService->addParent(request->childid(), request->parentid());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::AddParent success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::AddParent failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::ReplaceParent(ServerContext*, const ReplaceParentRequest* request, ReplaceParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::ReplaceParent: request =" << request);

    bool success = topologyManagerService->removeParent(request->childid(), request->oldparent());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::ReplaceParent success removeParent");
        bool success2 = topologyManagerService->addParent(request->childid(), request->newparent());
        if (success2) {
            NES_DEBUG("CoordinatorRPCServer::ReplaceParent success addParent topo=");
            reply->set_success(true);
            return Status::OK;
        }
        NES_ERROR("CoordinatorRPCServer::ReplaceParent failed in addParent");
        reply->set_success(false);
        return Status::CANCELLED;

    } else {
        NES_ERROR("CoordinatorRPCServer::ReplaceParent failed in remove parent");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RemoveParent(ServerContext*, const RemoveParentRequest* request, RemoveParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RemoveParent: request =" << request);

    bool success = topologyManagerService->removeParent(request->childid(), request->parentid());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RemoveParent success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RemoveParent failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::NotifyQueryFailure(ServerContext*,
                                                const QueryFailureNotification* request,
                                                QueryFailureNotificationReply* reply) {
    try {
        NES_ERROR("CoordinatorRPCServer::notifyQueryFailure: failure message received. id of failed query: "
                  << request->queryid() << " subplan: " << request->subqueryid() << " Id of worker: " << request->workerid()
                  << " Reason for failure: " << request->errormsg());

        NES_ASSERT2_FMT(!request->errormsg().empty(),
                        "Cannot fail query without error message " << request->queryid() << " subplan: " << request->subqueryid()
                                                                   << " from worker: " << request->workerid());

        auto sharedQueryId = request->queryid();
        auto subQueryId = request->subqueryid();
        if (!queryCatalogService->checkAndMarkForFailure(sharedQueryId, subQueryId)) {
            NES_ERROR("Unable to mark queries for failure :: subQueryId=" << subQueryId);
            return Status::CANCELLED;
        }

        //Send one failure request for the shared query plan
        if (!queryService->validateAndQueueFailQueryRequest(sharedQueryId, request->errormsg())) {
            NES_ERROR("Failed to create Query Failure request for shared query plan " << sharedQueryId);
            return Status::CANCELLED;
        }

        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::NotifyEpochTermination(ServerContext*,
                                                    const EpochBarrierPropagationNotification* request,
                                                    EpochBarrierPropagationReply* reply) {
    try {
        NES_INFO("CoordinatorRPCServer::propagatePunctuation: received punctuation with timestamp "
                 << request->timestamp() << "and querySubPlanId " << request->queryid());
        this->replicationService->notifyEpochTermination(request->timestamp(), request->queryid());
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken punctuation message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::GetNodesInRange(ServerContext*, const GetNodesInRangeRequest* request, GetNodesInRangeReply* reply) {

    std::vector<std::pair<uint64_t, NES::Spatial::DataTypes::Experimental::GeoLocation>> inRange =
        topologyManagerService->getNodesIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation(request->geolocation()),
                                                   request->radius());

    for (auto elem : inRange) {
        NES::Spatial::Protobuf::WorkerLocationInfo* workerInfo = reply->add_nodes();
        auto geoLocation = elem.second;
        workerInfo->set_id(elem.first);
        auto protoGeoLocation = workerInfo->mutable_geolocation();
        protoGeoLocation->set_lat(geoLocation.getLatitude());
        protoGeoLocation->set_lng(geoLocation.getLongitude());
    }
    return Status::OK;
}

Status CoordinatorRPCServer::SendErrors(ServerContext*, const SendErrorsMessage* request, ErrorReply* reply) {
    try {
        NES_ERROR("CoordinatorRPCServer::sendErrors: failure message received."
                  << "Id of worker: " << request->workerid() << " Reason for failure: " << request->errormsg());
        // TODO implement here what happens with received Error Messages
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RequestSoftStop(::grpc::ServerContext*,
                                             const ::RequestSoftStopMessage* request,
                                             ::StopRequestReply* response) {
    auto sharedQueryId = request->queryid();
    auto subQueryPlanId = request->subqueryid();
    NES_WARNING("CoordinatorRPCServer: received request for soft stopping the shared query plan id: " << sharedQueryId)

    //Check with query catalog service if the request possible
    auto sourceId = request->sourceid();
    auto softStopPossible = queryCatalogService->checkAndMarkForSoftStop(sharedQueryId, subQueryPlanId, sourceId);

    //Send response
    response->set_success(softStopPossible);
    return Status::OK;
}

Status CoordinatorRPCServer::notifySourceStopTriggered(::grpc::ServerContext*,
                                                       const ::SoftStopTriggeredMessage* request,
                                                       ::SoftStopTriggeredReply* response) {
    auto sharedQueryId = request->queryid();
    auto querySubPlanId = request->querysubplanid();
    NES_INFO("CoordinatorRPCServer: received request for soft stopping the sub pan : "
             << querySubPlanId << " shared query plan id: " << sharedQueryId)

    //inform catalog service
    bool success = queryCatalogService->updateQuerySubPlanStatus(sharedQueryId, querySubPlanId, QueryStatus::SoftStopTriggered);

    //update response
    response->set_success(success);
    return Status::OK;
}

Status CoordinatorRPCServer::NotifySoftStopCompleted(::grpc::ServerContext*,
                                                     const ::SoftStopCompletionMessage* request,
                                                     ::SoftStopCompletionReply* response) {
    //Fetch the request
    auto queryId = request->queryid();
    auto querySubPlanId = request->querysubplanid();

    //inform catalog service
    bool success = queryCatalogService->updateQuerySubPlanStatus(queryId, querySubPlanId, QueryStatus::SoftStopCompleted);

    //update response
    response->set_success(success);
    return Status::OK;
}

Status CoordinatorRPCServer::SendScheduledReconnect(ServerContext*,
                                                    const SendScheduledReconnectRequest* request,
                                                    SendScheduledReconnectReply* reply) {
    auto reconnectsToAddMessage = request->addreconnects();
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> addedReconnects;
    for (const auto& toAdd : reconnectsToAddMessage) {
        NES::Spatial::DataTypes::Experimental::GeoLocation location(toAdd.geolocation());
        addedReconnects.emplace_back(NES::Spatial::Mobility::Experimental::ReconnectPoint{location, toAdd.id(), toAdd.time()});
    }
    auto reconnectsToRemoveMessage = request->removereconnects();
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> removedReconnects;

    for (const auto& toRemove : reconnectsToRemoveMessage) {
        NES::Spatial::DataTypes::Experimental::GeoLocation location(toRemove.geolocation());
        removedReconnects.emplace_back(
            NES::Spatial::Mobility::Experimental::ReconnectPoint{location, toRemove.id(), toRemove.time()});
    }
    bool success = locationService->updatePredictedReconnect(addedReconnects, removedReconnects);
    reply->set_success(success);
    if (success) {
        return Status::OK;
    }
    return Status::CANCELLED;
}

Status
CoordinatorRPCServer::SendLocationUpdate(ServerContext*, const LocationUpdateRequest* request, LocationUpdateReply* reply) {
    auto coordinates = request->waypoint().geolocation();
    auto timestamp = request->waypoint().timestamp();
    NES_DEBUG("Coordinator received location update from node with id "
              << request->workerid() << " which reports [" << coordinates.lat() << ", " << coordinates.lng() << "] at TS "
              << timestamp);
    //todo #2862: update coordinator trajectory prediction
    auto geoLocation = NES::Spatial::DataTypes::Experimental::GeoLocation(coordinates);
    if (!topologyManagerService->updateGeoLocation(request->workerid(), std::move(geoLocation))) {
        reply->set_success(true);
        return Status::OK;
    }
    reply->set_success(false);
    return Status::CANCELLED;
}
