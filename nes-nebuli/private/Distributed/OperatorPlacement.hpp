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
#pragma once

#include <cstddef>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <NebuLI.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <PlanningContext.hpp>

namespace NES
{

using Capacities = std::unordered_map<TopologyGraph::NodeId, size_t>;

std::optional<TopologyGraph::NodeId> getPlacementFor(const LogicalOperator& op);

/// Placement that traverses the topology from bottom-to-top based on a topological sort and the given capacities of the nodes.
class OperatorPlacer
{
    LogicalPlan plan;
    PlanningContext ctx;

    explicit OperatorPlacer(CLI::LegacyOptimizer::OptimizedLogicalPlan&& inputPlan);

public:
    static OperatorPlacer from(CLI::LegacyOptimizer::OptimizedLogicalPlan&& inputPlan);
    OperatorPlacer() = delete;
    ~OperatorPlacer() = default;

    OperatorPlacer(const OperatorPlacer&) = delete;
    OperatorPlacer(OperatorPlacer&&) = delete;
    OperatorPlacer& operator=(const OperatorPlacer&) = delete;
    OperatorPlacer& operator=(OperatorPlacer&&) = delete;

    struct PlacedLogicalPlan
    {
        LogicalPlan plan;
        PlanningContext ctx;
    };
    PlacedLogicalPlan place() &&;

private:
    void pinSources();
    void pinSinks();
    LogicalOperator placeOperatorRecursive(const LogicalOperator& op);
    LogicalOperator placeUnaryOperator(const LogicalOperator& op, const LogicalOperator& child);
    LogicalOperator placeBinaryOperator(const LogicalOperator& op, const LogicalOperator& leftChild, const LogicalOperator& rightChild);

    [[nodiscard]] std::optional<TopologyGraph::NodeId>
    findFirstNodeWithCapacity(const TopologyGraph::NodeId& start, const TopologyGraph::NodeId& nextPlacedOperatorNode) const;

    [[nodiscard]] std::optional<TopologyGraph::NodeId> findNodeWithCapacity(
        const std::vector<std::tuple<std::optional<TopologyGraph::NodeId>, TopologyGraph::Path, TopologyGraph::Path>>& candidates) const;
};
}
