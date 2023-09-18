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
#ifndef TOPOLOGYPREDICTION_DELTA_HPP_
#define TOPOLOGYPREDICTION_DELTA_HPP_
#include <cstdint>
#include <memory>
#include <vector>

namespace NES::Experimental::TopologyPrediction {
class Edge;
/**
 * @brief this class represents a set of changes to be applied to the topology changelog for a certain point in time. It consists
 * of prediction from one or more nodes.
 */
class TopologyDelta {
  public:
    /**
   * @brief constructor
   * @param added a list of edges to be added to the topology. It is the callers responsibility to ensure that this list does not
   * contain any duplicates
   * @param removed a list of edges to be removed from the topology. It is the callers responsibility to ensure that this list does not
   * contain any duplicates
   */
    TopologyDelta(std::vector<Edge> added, std::vector<Edge> removed);

    /**
     * @brief this function returns a string visualizing the content of the changelog
     * @return a string in the format "added: {CHILD->PARENT, CHILD->PARENT, ...} removed: {CHILD->PARENT, CHILD->PARENT, ...}"
     */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief return the list of added edges contained in this delta
     * @return a const reference to a vector of edges
     */
    [[nodiscard]] const std::vector<Edge>& getAdded() const;

    /**
     * @brief return the list of removed edges contained in this delta
     * @return a const reference to a vector of edges
     */
    [[nodiscard]] const std::vector<Edge>& getRemoved() const;

  private:
    /**
     * @brief obtain a string representation of a vector of edges
     * @param edges a vector of edges
     * @return a string in the format "CHILD->PARENT, CHILD->PARENT, ..."
     */
    static std::string edgeListToString(const std::vector<Edge>& edges);

    std::vector<Edge> added;
    std::vector<Edge> removed;
};
}// namespace NES::Experimental::TopologyPrediction
#endif//TOPOLOGYPREDICTION_DELTA_HPP_
