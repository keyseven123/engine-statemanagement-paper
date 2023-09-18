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
#include <Topology/Prediction/Edge.hpp>
#include <sstream>
namespace NES::Experimental::TopologyPrediction {
std::string Edge::toString() const {
    std::stringstream ss;
    ss << upstreamTopologyNode << "->" << downstreamTopologyNode;
    return ss.str();
}

bool Edge::operator==(const Edge& other) const {
    return this->downstreamTopologyNode == other.downstreamTopologyNode
        && this->upstreamTopologyNode == other.upstreamTopologyNode;
}
}// namespace NES::Experimental::TopologyPrediction
