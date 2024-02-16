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
#ifndef NES_TOPOLOGYNODERELOCATIONREQUEST_HPP
#define NES_TOPOLOGYNODERELOCATIONREQUEST_HPP
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <Util/TopologyLinkInformation.hpp>


namespace NES {
using Timestamp = uint64_t;
class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
}// namespace Optimizer

namespace RequestProcessor::Experimental {
class TopologyNodeRelocationRequest;

struct TopologyNodeRelocationRequestResponse : AbstractRequestResponse {
    explicit TopologyNodeRelocationRequestResponse(bool success) : success(success){};
    bool success;
};
using TopologyNodeRelocationRequestPtr = std::shared_ptr<TopologyNodeRelocationRequest>;

class TopologyNodeRelocationRequest : public AbstractUniRequest {
  public:
    /**
     * @brief Constructor
     * @param removedLinks a list of links to be removed represented as pairs in the format {upstreamId, downstreamId}
     * @param addedLinks a list of links to be added represented as pairs in the format {upstreamId, downstreamId}
     * @param maxRetries the maximum amount of times this request should be retried in case of failure
     */
    TopologyNodeRelocationRequest(const std::vector<TopologyLinkInformation>& removedLinks,
                                  const std::vector<TopologyLinkInformation>& addedLinks,
                                  const std::vector<TopologyLinkInformation>& expectedRemovedLinks,
                                  const std::vector<TopologyLinkInformation>& expectedAddedLinks,
                                  Timestamp expectedTime,
                                  uint8_t maxRetries);

    /**
     * @brief Create a new topology change request
     * @param removedLinks a list of links to be removed represented as pairs in the format {upstreamId, downstreamId}
     * @param addedLinks a list of links to be added represented as pairs in the format {upstreamId, downstreamId}
     * @param maxRetries the maximum amount of times this request should be retried in case of failure
     * @return a pointer the the newly created request
     */
    static TopologyNodeRelocationRequestPtr create(const std::vector<TopologyLinkInformation>& removedLinks,
                                                   const std::vector<TopologyLinkInformation>& addedLinks,
                                                   const std::vector<TopologyLinkInformation>& expectedRemovedLinks,
                                                   const std::vector<TopologyLinkInformation>& expectedAddedLinks,
                                                   Timestamp expectedTime,
                                                   uint8_t maxRetries);

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief identify the operators affected by a topology change, run an incremental placement and deploy the changes
     * @param upstreamNodeId the id of the upstream node of removed link
     * @param downstreamNodeId the id of the downstream node of the removed link
     */
    void processRemoveTopologyLinkRequest(OperatorId upstreamNodeId, OperatorId downstreamNodeId);

    /**
     * @brief identify the operators of the specified shared query plan that are affected by a topology change,
     * run an incremental placement and deploy the changes
     * @param sharedQueryPlanId the id of the shared query plan for which the placemetn is to be updated
     * @param upstreamNodeId the id of the upstream node of removed link
     * @param downstreamNodeId the id of the downstream node of the removed link
     */
    void markOperatorsForReOperatorPlacement(SharedQueryId sharedQueryPlanId,
                                             const std::shared_ptr<Optimizer::ExecutionNode>& upstreamExecutionNode,
                                             const std::shared_ptr<Optimizer::ExecutionNode>& downstreamExecutionNode);

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    void markOperatorsForProactivePlacement(
        SharedQueryId sharedQueryPlanId,
        const Optimizer::ExecutionNodePtr& upstreamExecutionNode,
        const Optimizer::ExecutionNodePtr& downstreamExecutionNode);

  private:
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    std::vector<TopologyLinkInformation> removedLinks;
    std::vector<TopologyLinkInformation> addedLinks;
    std::vector<TopologyLinkInformation> expectedRemovedLinks;
    std::vector<TopologyLinkInformation> expectedAddedLinks;
    Timestamp expectedTime;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    QueryCatalogServicePtr queryCatalogService;
    std::vector<SharedQueryPlanPtr> proactiveDeployment(WorkerId upstreamNodeId, WorkerId downstreamNodeId);
};
}// namespace RequestProcessor::Experimental
}// namespace NES
#endif//NES_TOPOLOGYNODERELOCATIONREQUEST_HPP
