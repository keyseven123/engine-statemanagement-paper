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
//
// Created by balint on 20.04.21.
//
#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>
#include <WorkQueues/RequestTypes/RestartQueryRequest.hpp>

namespace NES {


MigrateQueryRequestPtr MigrateQueryRequest::create(QueryId queryId, TopologyNodeId nodeId,bool withBuffer) {
    return std::make_shared<MigrateQueryRequest>(MigrateQueryRequest(queryId,nodeId,withBuffer));

}

MigrateQueryRequest::MigrateQueryRequest(QueryId queryId, TopologyNodeId nodeId, bool withBuffer) :
                              NESRequest(queryId),withBuffer(withBuffer),nodeId(nodeId){};


bool MigrateQueryRequest::isWithBuffer() { return withBuffer; }

std::string MigrateQueryRequest::toString() {
    return "MigrateQueryRequest { QueryId: " + std::to_string(getQueryId()) + ", Topology Node: " + std::to_string(nodeId)
           + ", withBuffer: " + std::to_string(withBuffer) + "}";
}
TopologyNodeId MigrateQueryRequest::getTopologyNode() { return nodeId; }
}// namespace NES