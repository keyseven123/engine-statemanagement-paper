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

#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyNodeRequest.hpp>
#include <string>

namespace NES::Experimental {

RemoveTopologyNodeRequest::RemoveTopologyNodeRequest(WorkerId topologyNodeId) : topologyNodeId(topologyNodeId) {}

RemoveTopologyNodeRequestPtr RemoveTopologyNodeRequest::create(WorkerId topologyNodeId) {
    return std::make_shared<RemoveTopologyNodeRequest>(RemoveTopologyNodeRequest(topologyNodeId));
}

WorkerId RemoveTopologyNodeRequest::getWorkerId() const { return topologyNodeId; }

std::string RemoveTopologyNodeRequest::toString() {
    return "RemoveTopologyNodeRequest { WorkerId: " + std::to_string(topologyNodeId) + "}";
}

RequestType RemoveTopologyNodeRequest::getRequestType() { return RequestType::RemoveTopologyNode; }
}// namespace NES::Experimental
