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

#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

#include <Catalogs/StreamCatalogEntry.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class NESExecutionPlan;
using NESExecutionPlanPtr = std::shared_ptr<NESExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class NESTopologyEntry;
using NESTopologyEntryPtr = std::shared_ptr<NESTopologyEntry>;

class NESTopologyGraph;
using NESTopologyGraphPtr = std::shared_ptr<NESTopologyGraph>;

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;
}// namespace NES

namespace NES::Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    static constexpr auto NSINK_RETRIES = 3;
    static constexpr auto NSINK_RETRY_WAIT = std::chrono::seconds(5);

  public:
    explicit BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topologyPtr,
                                   TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog);

    virtual ~BasePlacementStrategy() = default;

    /**
     * @brief Returns an execution graph based on the input query and nes topology.
     * @param queryPlan: the query plan
     * @return true if successful
     */
    virtual bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) = 0;

    virtual bool partiallyUpdateGlobalExecutionPlan(const QueryPlanPtr& queryPlan) = 0;

  protected:
    /**
     * @brief Map the logical source name to the physical source nodes in the topology used for placing the operators
     * @param queryId : the id of the query.
     * @param sourceOperators: the source operators in the query
     */
    void mapPinnedOperatorToTopologyNodes(QueryId queryId, const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators);

    void pinSinkOperator(QueryId queryId,
                         const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators,
                         std::map<std::string, std::vector<TopologyNodePtr>>& mapOfSourceToTopologyNodes,
                         std::vector<TopologyNodePtr>& mergedGraphSourceNodes);

    /**
     * @brief Get Execution node for the input topology node
     * @param candidateTopologyNode: topology node
     * @return Execution Node pointer
     */
    ExecutionNodePtr getExecutionNode(const TopologyNodePtr& candidateTopologyNode);

    /**
     * @brief Get the physical node for the logical source
     * @param operatorId: the id of the operator
     * @return Topology node ptr or nullptr
     */
    TopologyNodePtr getTopologyNodeForPinnedOperator(uint64_t operatorId);

    /**
     * @brief Add network source and sinks between query sub plans allocated on different execution nodes
     * @param queryPlan: the original query plan
     */
    void addNetworkSourceAndSinkOperators(const QueryPlanPtr& queryPlan);

    /**
     * @brief Run the type inference phase for all the query sub plans for the input query id
     * @param queryId: the input query id
     * @return true if successful else false
     */
    bool runTypeInferencePhase(QueryId queryId);

    /**
     * @brief assign binary matrix representation of placement mapping to the topology
     * @param topologyPtr the topology to place the operators
     * @param queryPlan query plan to place
     * @param mapping binary mapping determining whether to place an operator to a node
     */
    bool assignMappingToTopology(const NES::TopologyPtr topologyPtr,
                                 const NES::QueryPlanPtr queryPlan,
                                 const std::vector<std::vector<bool>> mapping);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
    std::map<uint64_t, TopologyNodePtr> pinnedOperatorLocationMap;
    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;

  private:
    /**
     * @brief create a new network sink operator
     * @param queryId : the query id to which the sink belongs to
     * @param sourceOperatorId : the operator id of the corresponding source operator
     * @param sourceTopologyNode : the topology node to which sink operator will send the data
     * @return the instance of network sink operator
     */
    static OperatorNodePtr
    createNetworkSinkOperator(QueryId queryId, uint64_t sourceOperatorId, const TopologyNodePtr& sourceTopologyNode);

    /**
     * @brief create a new network source operator
     * @param queryId : the query id to which the source belongs to
     * @param inputSchema : the schema for input event stream
     * @param operatorId : the operator id of the source network operator
     * @return the instance of network source operator
     */
    static OperatorNodePtr createNetworkSourceOperator(QueryId queryId, SchemaPtr inputSchema, uint64_t operatorId);

    /**
     * @brief Attach network source or sink operator to the given operator
     * @param queryId : the id of the query
     * @param operatorNode : the logical operator to which source or sink operator need to be attached
     */
    void placeNetworkOperator(QueryId queryId, const OperatorNodePtr& operatorNode);

    /**
     * @brief Add an execution node as root
     * @param executionNode
     */
    void addExecutionNodeAsRoot(ExecutionNodePtr& executionNode);
};
}// namespace NES::Optimizer
#endif//NESPLACEMENTOPTIMIZER_HPP
