#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() : freeGlobalQueryNodeId(0) {
    root = GlobalQueryNode::createEmpty(getNextFreeId());
}

GlobalQueryPlanPtr GlobalQueryPlan::create() {
    return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan());
}

void GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {
    NES_INFO("GlobalQueryPlan: Adding new query plan to the Global query plan");
    QueryId queryId = queryPlan->getQueryId();
    if (queryId == INVALID_QUERY_ID) {
        NES_ERROR("GlobalQueryPlan: Found query plan without query id");
        throw Exception("GlobalQueryPlan: Found query plan without query id");
    }

    if (queryIdToGlobalQueryNodeMap.find(queryId) != queryIdToGlobalQueryNodeMap.end()) {
        NES_ERROR("GlobalQueryPlan: Found existing entry for the query Id " << queryId);
        throw Exception("GlobalQueryPlan: Entry for the queryId " + std::to_string(queryId) + " already present. Can't add same query multiple time.");
    }

    NES_INFO("GlobalQueryPlan: adding the query plan for query: " << queryId << " to the global query plan.");
    const auto rootOperators = queryPlan->getRootOperators();
    NES_DEBUG("GlobalQueryPlan: adding the root nodes of the query plan for query: " << queryId << " as children to the root node of the global query plan.");
    for (const auto& rootOperator : rootOperators) {
        addNewGlobalQueryNode(root, queryId, rootOperator);
    }
}

void GlobalQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("Removing query information from the meta data");
    GlobalQueryId globalQueryId = queryIdToGlobalQueryIdMap[queryId];
    GlobalQueryMetaDataPtr globalQueryMetaData = globalQueryIdToMetaDataMap[globalQueryId];
    globalQueryMetaData->removeQueryId(queryId);
    queryIdToGlobalQueryIdMap.erase(queryId);

    NES_INFO("GlobalQueryPlan: Remove the query plan for query " << queryId);
    const std::vector<GlobalQueryNodePtr>& globalQueryNodes = getGQNListForQueryId(queryId);
    for (GlobalQueryNodePtr globalQueryNode : globalQueryNodes) {
        globalQueryNode->removeQuery(queryId);
        //If global query node is empty then remove its parent child nodes as well
        if (globalQueryNode->isEmpty()) {
            globalQueryNode->removeChildren();
            globalQueryNode->removeAllParent();
        }
    }
    queryIdToGlobalQueryNodeMap.erase(queryId);
}

void GlobalQueryPlan::addNewGlobalQueryNode(const GlobalQueryNodePtr& parentNode, const QueryId queryId, const OperatorNodePtr& operatorNode) {

    NES_DEBUG("GlobalQueryPlan: Creating a new global query node for operator of query " << queryId << " and adding it as child to global query node with id " << parentNode->getId());
    GlobalQueryNodePtr newGlobalQueryNode = GlobalQueryNode::create(getNextFreeId(), queryId, operatorNode->copy());
    addGlobalQueryNodeToQuery(queryId, newGlobalQueryNode);
    parentNode->addChild(newGlobalQueryNode);
    NES_DEBUG("GlobalQueryPlan: Creating new global query node for the children of query operator of query " << queryId);
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (const auto& child : children) {
        addNewGlobalQueryNode(newGlobalQueryNode, queryId, child->as<OperatorNode>());
    }
}

std::vector<GlobalQueryNodePtr> GlobalQueryPlan::getGQNListForQueryId(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
    if (queryIdToGlobalQueryNodeMap.find(queryId) == queryIdToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId);
        return std::vector<GlobalQueryNodePtr>();
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId);
        return queryIdToGlobalQueryNodeMap[queryId];
    }
}

bool GlobalQueryPlan::addGlobalQueryNodeToQuery(QueryId queryId, GlobalQueryNodePtr globalQueryNode) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
    if (queryIdToGlobalQueryNodeMap.find(queryId) == queryIdToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId << " . Creating a new entry.");
        queryIdToGlobalQueryNodeMap[queryId] = {globalQueryNode};
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId << ". Adding the new global query node to the list.");
        std::vector<GlobalQueryNodePtr> globalQueryNodes = getGQNListForQueryId(queryId);
        globalQueryNodes.push_back(globalQueryNode);
        updateGQNListForQueryId(queryId, globalQueryNodes);
    }
    return true;
}

uint64_t GlobalQueryPlan::getNextFreeId() {
    return freeGlobalQueryNodeId++;
}

bool GlobalQueryPlan::updateGQNListForQueryId(QueryId queryId, std::vector<GlobalQueryNodePtr> globalQueryNodes) {
    if (queryIdToGlobalQueryNodeMap.find(queryId) == queryIdToGlobalQueryNodeMap.end()) {
        NES_WARNING("GlobalQueryPlan: unable to find query with id " << queryId << " in the global query plan.");
        return false;
    }
    NES_DEBUG("GlobalQueryPlan: Successfully updated the GQN List for query with id " << queryId);
    queryIdToGlobalQueryNodeMap[queryId] = globalQueryNodes;
    return true;
}

bool GlobalQueryPlan::updateGlobalQueryMetaDataMap() {

    if (!checkMetaDataValidity()) {
        NES_WARNING("GlobalQueryPlan: Failed to validate meta data.");
        return false;
    }

    NES_DEBUG("GlobalQueryPlan: Update and create new Global Query MetaData by grouping together GQNs with sink operators having overlapping leaf GQNs");
    //Comparator to compare two Global Query Nodes based on their id and used in the set as comparator
    auto cmp = [](NodePtr a, NodePtr b) {
        return a->as<GlobalQueryNode>()->getId() != b->as<GlobalQueryNode>()->getId();
    };

    std::vector<std::set<NodePtr>> vectorOfGroupedSinkGQNSets;
    //Iterate over all GQNs with sink operators and group them together if they have partially overlapping leaf nodes.
    std::vector<GlobalQueryNodePtr> sinkGQNs = getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    for (auto sinkGQN : sinkGQNs) {
        if (queryIdToGlobalQueryIdMap.find(sinkGQN->getQueryIds()[0]) != queryIdToGlobalQueryIdMap.end()) {
            NES_TRACE("GlobalQueryPlan: Skipping as Global Query Node is already part of an existing Global Query Meta Data.");
            continue;
        }
        NES_TRACE("GlobalQueryPlan: Checking with already grouped Global Query Nodes if there are overlapping leaf GQNs.");
        std::vector<NodePtr> targetLeafNodes = sinkGQN->getAllLeafNodes();
        std::set<NodePtr> groupedSinkGQNSet{sinkGQN};
        for (auto itr = vectorOfGroupedSinkGQNSets.begin(); itr != vectorOfGroupedSinkGQNSets.end(); itr++) {

            NES_TRACE("GlobalQueryPlan: Preparing the set of leaf GQNs for the grouped Sink GQNs");
            std::set<NodePtr> hostLeafNodes;
            for (auto hostSinkNode : *itr) {
                const std::vector<NodePtr>& leafNodes = hostSinkNode->getAllLeafNodes();
                hostLeafNodes.insert(leafNodes.begin(), leafNodes.end());
            }

            NES_TRACE("GlobalQueryPlan: compute the intersection between the leaf of the target GQN and host grouped GQNs");
            std::set<NodePtr> intersectionSet;
            std::set_intersection(hostLeafNodes.begin(), hostLeafNodes.end(), targetLeafNodes.begin(), targetLeafNodes.end(),
                                  std::inserter(intersectionSet, intersectionSet.begin()), cmp);

            if (!intersectionSet.empty()) {
                NES_TRACE("GlobalQueryPlan: Found overlap in leaf GQNs. Adding the target GQNs to the host group.");
                for (auto hostSinkNode : *itr) {
                    groupedSinkGQNSet.insert(hostSinkNode);
                }
                vectorOfGroupedSinkGQNSets.erase(itr--);
            }
        }
        vectorOfGroupedSinkGQNSets.push_back(groupedSinkGQNSet);
    }

    NES_TRACE("GlobalQueryPlan: Iterating over all groups of GQNs and will either add to existing Global Query MetaData or will create a new Global Query Metadata.");
    for (auto groupedSinkGQNs : vectorOfGroupedSinkGQNSets) {
        std::set<QueryId> queryIds;
        std::set<GlobalQueryNodePtr> targetSinkGQNs;
        std::set<NodePtr> targetLeafNodes;

        NES_TRACE("GlobalQueryPlan: Iterate over grouped GQNs and compute set of leaf GQNs, query Ids, and GQNs with sink operator.");
        for (auto sinkGQN : groupedSinkGQNs) {
            targetSinkGQNs.insert(sinkGQN->as<GlobalQueryNode>());
            queryIds.insert(sinkGQN->as<GlobalQueryNode>()->getQueryIds()[0]);
            std::vector<NodePtr> leafNodes = sinkGQN->getAllLeafNodes();
            targetLeafNodes.insert(leafNodes.begin(), leafNodes.end());
        }

        NES_TRACE("GlobalQueryPlan: Iterate over all exisiting Global Query Metadata and trying to find if one of them has overlapping leaf GQNs with the current group of traget GQNs.");
        GlobalQueryMetaDataPtr hostGlobalQueryMetaData;
        for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
            std::set<GlobalQueryNodePtr> hostSinkNodes = globalQueryMetaData->getSinkGlobalQueryNodes();
            std::set<NodePtr> hostLeafNodes;
            for (auto hostSinkNode : hostSinkNodes) {
                const std::vector<NodePtr>& leafNodes = hostSinkNode->getAllLeafNodes();
                hostLeafNodes.insert(leafNodes.begin(), leafNodes.end());
            }
            std::set<NodePtr> intersectionSet;
            std::set_intersection(targetLeafNodes.begin(), targetLeafNodes.end(), hostLeafNodes.begin(), hostLeafNodes.end(), std::inserter(intersectionSet, intersectionSet.begin()), cmp);

            if (!intersectionSet.empty()) {
                NES_TRACE("GlobalQueryPlan: Found overlap in leaf GQNs. Found an existing Global Query Metadata for inserting the grouped together target GQNs.");
                hostGlobalQueryMetaData = globalQueryMetaData;
                break;
            }
        }

        if (hostGlobalQueryMetaData) {
            NES_TRACE("GlobalQueryPlan: Adding target group of GQNs to the Global Query Metadata with overlapping leaf GQNs");
            hostGlobalQueryMetaData->addNewSinkGlobalQueryNodes(targetSinkGQNs);
        } else {
            NES_TRACE("GlobalQueryPlan: Creating new Global Query Metadata");
            hostGlobalQueryMetaData = GlobalQueryMetaData::create(queryIds, targetSinkGQNs);
        }

        GlobalQueryId globalQueryId = hostGlobalQueryMetaData->getGlobalQueryId();
        NES_TRACE("GlobalQueryPlan: Updating the Global Query Id to Metadata map");
        globalQueryIdToMetaDataMap[globalQueryId] = hostGlobalQueryMetaData;

        NES_TRACE("GlobalQueryPlan: Updating the Query Id to Global Query Id map");
        for (auto queryId : queryIds) {
            queryIdToGlobalQueryIdMap[queryId] = globalQueryId;
        }
    }
    return true;
}

bool GlobalQueryPlan::checkMetaDataValidity() {
    NES_DEBUG("GlobalQueryPlan: check if all Global Query MetaData are still valid");
    //Comparator to compare two Global Query Nodes based on their id and used in the set as comparator
    auto cmp = [](NodePtr a, NodePtr b) {
        return a->as<GlobalQueryNode>()->getId() != b->as<GlobalQueryNode>()->getId();
    };

    NES_TRACE("GlobalQueryPlan: Iterate over the map of Global Query Metadata to inspect the validity.");
    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {

        if (globalQueryMetaData->isEmpty()) {
            NES_TRACE("GlobalQueryPlan: Found an empty Global Query Metadata.");
            if (globalQueryMetaData->isDeployed()) {
                NES_TRACE("GlobalQueryPlan: Removing the Global Query Metadata that has been deployed and is empty.");
                globalQueryIdToMetaDataMap.erase(globalQueryId);
            }
            continue;
        }

        //Iterate over all Global Query nodes with sink operators and try to identify if there exists a global query node
        // that do not have any overlapping leaf Global Query Nodes
        std::set<GlobalQueryNodePtr> sinkGQNs = globalQueryMetaData->getSinkGlobalQueryNodes();
        for (auto itrOuter = sinkGQNs.begin(); itrOuter != sinkGQNs.end(); itrOuter++) {

            std::set<GlobalQueryNodePtr> sinkGQNsWithMatchedSourceGQNs{*itrOuter};
            std::vector<NodePtr> outerLeafNodes = (*itrOuter)->getAllLeafNodes();
            // Iterate over remaining GQNs and try to find the GQNs with overlapping leaf GQNs
            for (auto itrInner = (itrOuter++); itrInner != sinkGQNs.end(); itrInner++) {

                std::vector<NodePtr> innerLeafNodes = (*itrInner)->getAllLeafNodes();
                std::set<NodePtr> intersectionSet;
                std::set_intersection(outerLeafNodes.begin(), outerLeafNodes.end(), innerLeafNodes.begin(), innerLeafNodes.end(),
                                      std::inserter(intersectionSet, intersectionSet.begin()), cmp);
                if (!intersectionSet.empty()) {
                    NES_TRACE("GlobalQueryPlan: found overlapping leaf  global query nodes. Adding the Global query "
                              "node with sink operator to the set of GQNs with overlapping GQNs");
                    sinkGQNsWithMatchedSourceGQNs.insert(*itrInner);
                }
            }

            if (sinkGQNsWithMatchedSourceGQNs.size() == sinkGQNs.size()) {
                NES_TRACE("GlobalQueryPlan: Found no changes to the MetaData information.");
                NES_TRACE("GlobalQueryPlan: Found " << sinkGQNsWithMatchedSourceGQNs.size() << " of expected " << sinkGQNs.size() << " sink nodes.");
                break;
            } else {
                NES_DEBUG("GlobalQueryPlan: Found MetaData information with non-merged query plans. Clearing MetaData for re-computation.");
                //Get the query ids from the metadata
                std::set<QueryId> queryIds = globalQueryMetaData->getQueryIds();
                for (auto queryId : queryIds) {
                    queryIdToGlobalQueryIdMap.erase(queryId);
                }
                globalQueryMetaData->clear();
            }
        }
    }
    return true;
}

std::vector<GlobalQueryMetaDataPtr> GlobalQueryPlan::getGlobalQueryMetaDataToDeploy() {
    NES_DEBUG("GlobalQueryPlan: Get the Global MetaData to be deployed.");
    std::vector<GlobalQueryMetaDataPtr> globalQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map with global query metadata.");
    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
        if (globalQueryMetaData->isDeployed()) {
            NES_TRACE("GlobalQueryPlan: Skipping! found already deployed query meta data.");
            continue;
        }

        if (globalQueryMetaData->isEmpty()) {
            NES_DEBUG("GlobalQueryPlan: Removing! found an empty query meta data.");
            globalQueryIdToMetaDataMap.erase(globalQueryMetaData->getGlobalQueryId());
        }
        globalQueryMetaDataToDeploy.push_back(globalQueryMetaData);
    }
    NES_DEBUG("GlobalQueryPlan: Found " << globalQueryMetaDataToDeploy.size() << "  Global MetaData to be deployed.");
    return globalQueryMetaDataToDeploy;
}

GlobalQueryId GlobalQueryPlan::getGlobalQueryIdForQuery(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Get the Global Query Id for the query " << queryId);
    if (queryIdToGlobalQueryIdMap.find(queryId) != queryIdToGlobalQueryIdMap.end()) {
        return queryIdToGlobalQueryIdMap[queryId];
    }
    NES_WARNING("GlobalQueryPlan: Unable to find Global Query Id for the query " << queryId);
    return INVALID_GLOBAL_QUERY_ID;
}
}// namespace NES