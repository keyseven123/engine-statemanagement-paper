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

#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>
#include <string>

namespace NES::Experimental {

MigrateQueryRequestPtr MigrateQueryRequest::create( TopologyNodeId nodeId, MigrationType::Value migrationType) {
    return std::make_shared<MigrateQueryRequest>(MigrateQueryRequest(nodeId, migrationType));
}

MigrateQueryRequest::MigrateQueryRequest(TopologyNodeId nodeId, MigrationType::Value migrationType)
    : Request(0), nodeId(nodeId), migrationType(migrationType){};

MigrationType::Value MigrateQueryRequest::getMigrationType() { return migrationType; }

std::string MigrateQueryRequest::toString() {
    return "MigrateQueryRequest {Topology Node: " + std::to_string(nodeId)
        + ", withBuffer: " + std::to_string(migrationType) + "}";
}
TopologyNodeId MigrateQueryRequest::getTopologyNodeId() { return nodeId; }
}// namespace NES::Experimental