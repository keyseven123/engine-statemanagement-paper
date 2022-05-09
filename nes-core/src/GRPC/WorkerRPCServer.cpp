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
#include <cpprest/json.h>
#include <utility>

namespace NES {

WorkerRPCServer::WorkerRPCServer(Runtime::NodeEnginePtr nodeEngine, MonitoringAgentPtr monitoringAgent)
    : nodeEngine(std::move(nodeEngine)), monitoringAgent(std::move(monitoringAgent)) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer()");
}

Status WorkerRPCServer::RegisterQuery(ServerContext*, const RegisterQueryRequest* request, RegisterQueryReply* reply) {
    auto queryPlan = QueryPlanSerializationUtil::deserializeQueryPlan((SerializableQueryPlan*) &request->queryplan());
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for queryId: " << queryPlan->getQueryId()
                                                                          << " plan=" << queryPlan->toString());

    auto success = nodeEngine->registerQuery(queryPlan);

    if (success.isSuccessful()) {
        NES_DEBUG("WorkerRPCServer::RegisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::RegisterQuery: failed");
    reply->set_success(false);
    reply->mutable_error()->set_message(success.getMessage());
    reply->mutable_error()->set_stacktrace(success.getStacktrace());
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
    bool success = nodeEngine->stopQuery(request->queryid(), Runtime::QueryTerminationType(request->queryterminationtype()));
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StopQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::RegisterMonitoringPlan(ServerContext*,
                                               const MonitoringRegistrationRequest* request,
                                               MonitoringRegistrationReply*) {
    try {
        NES_DEBUG("WorkerRPCServer::RegisterMonitoringPlan: Got request");
        std::set<MetricType> types;
        for (auto type : request->metrictypes()) {
            types.insert((MetricType) type);
        }
        MonitoringPlanPtr plan = MonitoringPlan::create(types);
        monitoringAgent->setMonitoringPlan(plan);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Registering monitoring plan failed: " << ex.what());
    }
    return Status::CANCELLED;
}

Status WorkerRPCServer::GetMonitoringData(ServerContext*, const MonitoringDataRequest*, MonitoringDataReply* reply) {
    try {
        NES_DEBUG("WorkerRPCServer: GetMonitoringData request received");
        auto metrics = monitoringAgent->getMetricsAsJson().serialize();
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
}// namespace NES