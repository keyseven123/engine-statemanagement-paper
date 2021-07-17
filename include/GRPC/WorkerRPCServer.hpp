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

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

class MonitoringAgent;
typedef std::shared_ptr<MonitoringAgent> MonitoringAgentPtr;

class WorkerRPCServer final : public WorkerRPCService::Service {
  public:
    WorkerRPCServer(NodeEngine::NodeEnginePtr nodeEngine, MonitoringAgentPtr monitoringAgent);

    Status RegisterQuery(ServerContext* context, const RegisterQueryRequest* request, RegisterQueryReply* reply) override;

    Status UnregisterQuery(ServerContext* context, const UnregisterQueryRequest* request, UnregisterQueryReply* reply) override;

    Status StartQuery(ServerContext* context, const StartQueryRequest* request, StartQueryReply* reply) override;

    Status StopQuery(ServerContext* context, const StopQueryRequest* request, StopQueryReply* reply) override;

    Status RegisterMonitoring(ServerContext* context,
                              const MonitoringRegistrationRequest* request,
                              MonitoringRegistrationReply* reply) override;

    Status GetMonitoringData(ServerContext* context, const MonitoringDataRequest* request, MonitoringDataReply* reply) override;

    Status SetSourceConfig(ServerContext* context, const SetSourceConfigRequest* request, SetSourceConfigReply* reply) override;

    Status GetSourceConfig(ServerContext* context, const GetSourceConfigRequest* request, GetSourceConfigReply* reply) override;

  private:
    NodeEngine::NodeEnginePtr nodeEngine;
    MonitoringAgentPtr monitoringAgent;
};

}// namespace NES