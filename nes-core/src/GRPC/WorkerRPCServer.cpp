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

#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProvider.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {

WorkerRPCServer::WorkerRPCServer(Runtime::NodeEnginePtr nodeEngine,
                                 Monitoring::MonitoringAgentPtr monitoringAgent,
                                 NES::Spatial::Mobility::Experimental::LocationProviderPtr locationProvider,
                                 NES::Spatial::Mobility::Experimental::TrajectoryPredictorPtr trajectoryPredictor)
    : nodeEngine(std::move(nodeEngine)), monitoringAgent(std::move(monitoringAgent)),
      locationProvider(std::move(locationProvider)), trajectoryPredictor(std::move(trajectoryPredictor)) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer()");
}

Status WorkerRPCServer::RegisterQuery(ServerContext*, const RegisterQueryRequest* request, RegisterQueryReply* reply) {
    auto queryPlan = QueryPlanSerializationUtil::deserializeQueryPlan((SerializableQueryPlan*) &request->queryplan());
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for queryId: " << queryPlan->getQueryId()
                                                                          << " plan=" << queryPlan->toString());
    bool success = 0;
    try {
        success = nodeEngine->registerQueryInNodeEngine(queryPlan);
    } catch (std::exception& error) {
        NES_ERROR("Register query crashed: " << error.what());
        success = false;
    }
    if (success) {
        NES_DEBUG("WorkerRPCServer::RegisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::RegisterQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::UnregisterQuery(ServerContext*, const UnregisterQueryRequest* request, UnregisterQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::UnregisterQuery: got request for " << request->queryid());
    bool success = nodeEngine->unregisterQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::UnregisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::UnregisterQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::StartQuery(ServerContext*, const StartQueryRequest* request, StartQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StartQuery: got request for " << request->queryid());
    bool success = nodeEngine->startQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::StartQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StartQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::StopQuery(ServerContext*, const StopQueryRequest* request, StopQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StopQuery: got request for " << request->queryid());
    auto terminationType = Runtime::QueryTerminationType(request->queryterminationtype());
    NES_ASSERT2_FMT(terminationType != Runtime::QueryTerminationType::Graceful
                        && terminationType != Runtime::QueryTerminationType::Invalid,
                    "Invalid termination type requested");
    bool success = nodeEngine->stopQuery(request->queryid(), terminationType);
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StopQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

//Status WorkerRPCServer::RegisterMonitoringPlan(ServerContext*,
//                                               const MonitoringRegistrationRequest* request,
//                                               MonitoringRegistrationReply*) {
//    try {
//        NES_DEBUG("WorkerRPCServer::RegisterMonitoringPlan: Got request");
//        std::set<Monitoring::MetricType> types;
//        for (auto type : request->metrictypes()) {
//            types.insert((Monitoring::MetricType) type);
//        }
//        Monitoring::MonitoringPlanPtr plan = Monitoring::MonitoringPlan::create(types);
//        monitoringAgent->setMonitoringPlan(plan);
//        return Status::OK;
//    } catch (std::exception& ex) {
//        NES_ERROR("WorkerRPCServer: Registering monitoring plan failed: " << ex.what());
//    }
//    return Status::CANCELLED;
//}

Status WorkerRPCServer::GetMonitoringData(ServerContext*, const MonitoringDataRequest*, MonitoringDataReply* reply) {
    try {
        NES_DEBUG("WorkerRPCServer: GetMonitoringData request received");
        auto metrics = monitoringAgent->getMetricsAsJson().dump();
        NES_DEBUG("WorkerRPCServer: Transmitting monitoring data: " << metrics);
        reply->set_metricsasjson(metrics);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Requesting monitoring data failed: " << ex.what());
    }
    return Status::CANCELLED;
}

Status WorkerRPCServer::InjectEpochBarrier(ServerContext*, const EpochBarrierNotification* request, EpochBarrierReply* reply) {
    try {
        NES_ERROR("WorkerRPCServer::propagatePunctuation received a punctuation with the timestamp "
                  << request->timestamp() << " and a queryId " << request->queryid());
        reply->set_success(true);
        nodeEngine->injectEpochBarrier(request->timestamp(), request->queryid());
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: received a broken punctuation message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::BeginBuffer(ServerContext*, const BufferRequest* request, BufferReply* reply) {
    NES_DEBUG("WorkerRPCServer::BeginBuffer request received");

    uint64_t querySubPlanId = request->querysubplanid();
    uint64_t uniqueNetworkSinkDescriptorId = request->uniquenetworksinkdescriptorid();
    bool success = nodeEngine->bufferData(querySubPlanId, uniqueNetworkSinkDescriptorId);
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::StopQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}
Status
WorkerRPCServer::UpdateNetworkSink(ServerContext*, const UpdateNetworkSinkRequest* request, UpdateNetworkSinkReply* reply) {
    NES_DEBUG("WorkerRPCServer::Sink Reconfiguration request received");
    uint64_t querySubPlanId = request->querysubplanid();
    uint64_t uniqueNetworkSinkDescriptorId = request->uniquenetworksinkdescriptorid();
    uint64_t newNodeId = request->newnodeid();
    std::string newHostname = request->newhostname();
    uint32_t newPort = request->newport();

    bool success = nodeEngine->updateNetworkSink(newNodeId, newHostname, newPort, querySubPlanId, uniqueNetworkSinkDescriptorId);
    if (success) {
        NES_DEBUG("WorkerRPCServer::UpdateNetworkSinks: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::UpdateNetworkSinks: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::GetLocation(ServerContext*, const GetLocationRequest* request, GetLocationReply* reply) {
    (void) request;
    NES_DEBUG("WorkerRPCServer received location request")
    if (!locationProvider) {
        NES_DEBUG("WorkerRPCServer: locationProvider not set, node doesn't have known location")
        //return an empty reply
        return Status::OK;
    }
    auto waypoint = locationProvider->getCurrentWaypoint();
    auto loc = waypoint.getLocation();
    auto protoWaypoint = reply->mutable_waypoint();
    if (loc.isValid()) {
        auto coord = protoWaypoint->mutable_geolocation();
        coord->set_lat(loc.getLatitude());
        coord->set_lng(loc.getLongitude());
    }
    if (waypoint.getTimestamp()) {
        protoWaypoint->set_timestamp(waypoint.getTimestamp().value());
    }
    return Status::OK;
}
}// namespace NES