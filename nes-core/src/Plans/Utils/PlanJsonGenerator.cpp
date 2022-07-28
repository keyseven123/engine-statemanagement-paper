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

#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameSourceOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpprest/json.h>

namespace NES {

std::string PlanJsonGenerator::getOperatorType(const OperatorNodePtr& operatorNode) {
    NES_INFO("Util: getting the type of the operator");

    std::string operatorType;
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        if (operatorNode->as<SourceLogicalOperatorNode>()
                ->getSourceDescriptor()
                ->instanceOf<Network::NetworkSourceDescriptor>()) {
            operatorType = "SOURCE_SYS";
        } else {
            operatorType = "SOURCE";
        }
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        operatorType = "FILTER";
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        operatorType = "MAP";
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        operatorType = "WINDOW AGGREGATION";
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        operatorType = "JOIN";
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        operatorType = "PROJECTION";
    } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        operatorType = "UNION";
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        operatorType = "BROADCAST";
    } else if (operatorNode->instanceOf<RenameSourceOperatorNode>()) {
        operatorType = "RENAME";
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        operatorType = "WATERMARK";
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        if (operatorNode->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
            operatorType = "SINK_SYS";
        } else {
            operatorType = "SINK";
        }
    } else {
        operatorType = "UNDEFINED";
    }
    NES_DEBUG("UtilityFunctions: operatorType = " << operatorType);
    return operatorType;
}

web::json::value PlanJsonGenerator::getExecutionPlanAsJson(const GlobalExecutionPlanPtr& globalExecutionPlan, QueryId queryId) {

    NES_INFO("UtilityFunctions: getting execution plan as JSON");

    web::json::value executionPlanJson{};
    std::vector<web::json::value> nodes = {};

    std::vector<ExecutionNodePtr> executionNodes;
    if (queryId != INVALID_QUERY_ID) {
        executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    } else {
        executionNodes = globalExecutionPlan->getAllExecutionNodes();
    }

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        web::json::value currentExecutionNodeJsonValue{};

        currentExecutionNodeJsonValue["executionNodeId"] = web::json::value::number(executionNode->getId());
        currentExecutionNodeJsonValue["topologyId"] = web::json::value::number(executionNode->getTopologyNode()->getId());
        currentExecutionNodeJsonValue["topologyNodeIpAddress"] =
            web::json::value::string(executionNode->getTopologyNode()->getIpAddress());

        std::map<QueryId, std::vector<QueryPlanPtr>> queryToQuerySubPlansMap;
        std::vector<QueryPlanPtr> querySubPlans;
        if (queryId == INVALID_QUERY_ID) {
            queryToQuerySubPlansMap = executionNode->getAllQuerySubPlans();
        } else {
            querySubPlans = executionNode->getQuerySubPlans(queryId);
            if (!querySubPlans.empty()) {
                queryToQuerySubPlansMap[queryId] = querySubPlans;
            }
        }

        std::vector<web::json::value> scheduledQueries = {};

        for (auto& [queryId, querySubPlans] : queryToQuerySubPlansMap) {

            std::vector<web::json::value> scheduledSubQueries;
            web::json::value queryToQuerySubPlans{};
            queryToQuerySubPlans["queryId"] = web::json::value::number(queryId);

            // loop over all query sub plans inside the current executionNode
            for (const QueryPlanPtr& querySubPlan : querySubPlans) {
                // prepare json object to hold information on current query sub plan
                web::json::value currentQuerySubPlan{};

                // id of current query sub plan
                currentQuerySubPlan["querySubPlanId"] = querySubPlan->getQuerySubPlanId();

                // add the string containing operator to the json object of current query sub plan
                currentQuerySubPlan["operator"] = web::json::value::string(querySubPlan->toString());

                // TODO: Add source and target
                scheduledSubQueries.push_back(currentQuerySubPlan);
            }
            queryToQuerySubPlans["querySubPlans"] = web::json::value::array(scheduledSubQueries);
            scheduledQueries.push_back(queryToQuerySubPlans);
        }

        currentExecutionNodeJsonValue["ScheduledQueries"] = web::json::value::array(scheduledQueries);
        nodes.push_back(currentExecutionNodeJsonValue);
    }

    // add `executionNodes` JSON array to the final JSON result
    executionPlanJson["executionNodes"] = web::json::value::array(nodes);

    return executionPlanJson;
}

web::json::value PlanJsonGenerator::getQueryPlanAsJson(const QueryPlanPtr& queryPlan) {

    NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");

    web::json::value result{};
    std::vector<web::json::value> nodes{};
    std::vector<web::json::value> edges{};

    OperatorNodePtr root = queryPlan->getRootOperators()[0];

    if (!root) {
        NES_DEBUG("UtilityFunctions::getQueryPlanAsJson : root operator is empty");
        auto node = web::json::value::object();
        node["id"] = web::json::value::string("NONE");
        node["name"] = web::json::value::string("NONE");
        nodes.push_back(node);
    } else {
        NES_DEBUG("UtilityFunctions::getQueryPlanAsJson : root operator is not empty");
        std::string rootOperatorType = getOperatorType(root);

        // Create a node JSON object for the root operator
        auto node = web::json::value::object();

        // use the id of the root operator to fill the id field
        node["id"] = web::json::value::string(std::to_string(root->getId()));

        // use concatenation of <operator type>(OP-<operator id>) to fill name field
        node["name"] = web::json::value::string(rootOperatorType + +"(OP-" + std::to_string(root->getId()) + ")");

        node["nodeType"] = web::json::value::string(rootOperatorType);

        nodes.push_back(node);

        // traverse to the children of the current operator
        getChildren(root, nodes, edges);
    }

    // add `nodes` and `edges` JSON array to the final JSON result
    result["nodes"] = web::json::value::array(nodes);
    result["edges"] = web::json::value::array(edges);

    return result;
}

void PlanJsonGenerator::getChildren(OperatorNodePtr const& root,
                                    std::vector<web::json::value>& nodes,
                                    std::vector<web::json::value>& edges) {

    std::vector<web::json::value> childrenNode;

    std::vector<NodePtr> children = root->getChildren();
    if (children.empty()) {
        NES_DEBUG("UtilityFunctions::children : children is empty()");
        return;
    }

    NES_DEBUG("UtilityFunctions::children : children size = " << children.size());
    for (const NodePtr& child : children) {
        // Create a node JSON object for the current operator
        auto node = web::json::value::object();
        auto childLogicalOperatorNode = child->as<LogicalOperatorNode>();
        std::string childOPeratorType = getOperatorType(childLogicalOperatorNode);

        // use the id of the current operator to fill the id field
        node["id"] = web::json::value::string(std::to_string(childLogicalOperatorNode->getId()));

        if (childOPeratorType == "WINDOW AGGREGATION") {
            // window operator node needs more information, therefore we added information about window type and aggregation
            node["name"] = web::json::value::string(childLogicalOperatorNode->as<WindowLogicalOperatorNode>()->toString());
            std::cout << childLogicalOperatorNode->as<WindowLogicalOperatorNode>()->toString() << std::endl;
        } else {
            // use concatenation of <operator type>(OP-<operator id>) to fill name field
            // e.g. FILTER(OP-1)
            node["name"] =
                web::json::value::string(childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");
        }
        node["nodeType"] = web::json::value::string(childOPeratorType);

        // store current node JSON object to the `nodes` JSON array
        nodes.push_back(node);

        // Create an edge JSON object for current operator
        auto edge = web::json::value::object();

        if (childOPeratorType == "WINDOW AGGREGATION") {
            // window operator node needs more information, therefore we added information about window type and aggregation
            edge["source"] = web::json::value::string(childLogicalOperatorNode->as<WindowLogicalOperatorNode>()->toString());
        } else {
            edge["source"] =
                web::json::value::string(childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");
        }

        if (getOperatorType(root) == "WINDOW AGGREGATION") {
            edge["target"] = web::json::value::string(root->as<WindowLogicalOperatorNode>()->toString());
        } else {
            edge["target"] = web::json::value::string(getOperatorType(root) + "(OP-" + std::to_string(root->getId()) + ")");
        }
        // store current edge JSON object to `edges` JSON array
        edges.push_back(edge);

        // traverse to the children of current operator
        getChildren(childLogicalOperatorNode, nodes, edges);
    }
}

}// namespace NES
