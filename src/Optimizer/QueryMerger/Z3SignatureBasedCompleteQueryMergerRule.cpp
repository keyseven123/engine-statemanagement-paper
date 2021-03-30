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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES::Optimizer {

Z3SignatureBasedCompleteQueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule(z3::ContextPtr context) {
    signatureEqualityUtil = SignatureEqualityUtil::create(context);
}

Z3SignatureBasedCompleteQueryMergerRule::~Z3SignatureBasedCompleteQueryMergerRule() {
    NES_DEBUG("~Z3SignatureBasedCompleteQueryMergerRule()");
}

Z3SignatureBasedCompleteQueryMergerRulePtr Z3SignatureBasedCompleteQueryMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedCompleteQueryMergerRule>(Z3SignatureBasedCompleteQueryMergerRule(context));
}

bool Z3SignatureBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {
    NES_INFO(
        "Z3SignatureBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<SharedQueryMetaDataPtr> allNewSharedQueryMetaData = globalQueryPlan->getAllNewSharedQueryMetaData();
    if (allNewSharedQueryMetaData.empty()) {
        NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    std::vector<SharedQueryMetaDataPtr> allOldSharedQueryMetaData = globalQueryPlan->getAllOldSharedQueryMetaData();
    NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (auto& targetSharedQueryMetaData : allNewSharedQueryMetaData) {
        bool merged = false;
        for (auto& hostSharedQueryMetaData : allOldSharedQueryMetaData) {
            if (targetSharedQueryMetaData->getSharedQueryId() == hostSharedQueryMetaData->getSharedQueryId()) {
                continue;
            }

            auto hostQueryPlan = hostSharedQueryMetaData->getQueryPlan();
            auto targetQueryPlan = targetSharedQueryMetaData->getQueryPlan();

            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            auto targetSinks = targetQueryPlan->getSinkOperators();
            bool foundMatch = false;
            for (auto& targetSink : targetSinks) {
                auto hostSinks = hostQueryPlan->getSinkOperators();
                for (auto& hostSink : hostSinks) {
                    //Check if the address and target sink operator signatures match each other
                    if (signatureEqualityUtil->checkEquality(hostSink->getSignature(), targetSink->getSignature())) {
                        targetToHostSinkOperatorMap[targetSink] = hostSink;
                        foundMatch = true;
                        break;
                    }
                }
                if (!foundMatch) {
                    NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: There are no matching host sink for target sink "
                                << targetSink->toString());
                    break;
                }
            }

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (!foundMatch) {
                NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Target and Host Shared Query MetaData are not equal");
                continue;
            }

            NES_TRACE("Z3SignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");

            //Iterate over all matched pairs of sink operators and merge the query plan
            for (auto& [targetSinkOperator, hostSinkOperator] : targetToHostSinkOperatorMap) {
                //Get children of target and host sink operators
                auto targetSinkChildren = targetSinkOperator->getChildren();
                auto hostSinkChildren = hostSinkOperator->getChildren();
                //Iterate over target children operators and migrate their parents to the host children operators.
                // Once done, remove the target parent from the target children.
                for (auto& childToMerge : targetSinkChildren) {
                    for (auto& hostChild : hostSinkChildren) {
                        bool addedNewParent = hostChild->addParent(targetSinkOperator);
                        if (!addedNewParent) {
                            NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Failed to add new parent");
                        }
                    }
                    childToMerge->removeParent(targetSinkOperator);
                }
                //Add target sink operator as root to the host query plan.
                hostQueryPlan->addRootOperator(targetSinkOperator);
            }

            hostSharedQueryMetaData->addSharedQueryMetaData(targetSharedQueryMetaData);
            //Clear the target shared query metadata
            targetSharedQueryMetaData->clear();
            //Update the shared query meta data
            globalQueryPlan->updateSharedQueryMetadata(hostSharedQueryMetaData);
            // exit the for loop as we found a matching address shared query meta data
            merged = true;
            break;
        }
        if (!merged) {
            allOldSharedQueryMetaData.emplace_back(targetSharedQueryMetaData);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeEmptySharedQueryMetaData();
    return true;
}

}// namespace NES::Optimizer
