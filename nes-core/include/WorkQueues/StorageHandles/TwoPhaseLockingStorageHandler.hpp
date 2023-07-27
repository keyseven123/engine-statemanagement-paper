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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_TWOPHASELOCKINGSTORAGEHANDLER_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_TWOPHASELOCKINGSTORAGEHANDLER_HPP_

#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <atomic>
#include <vector>

namespace NES {
/**
 * @brief Resource handles created by this class ensure that the resource has been locked in the growing phase and stay locked
 * until the handle goes out of scope.
 */
class TwoPhaseLockingStorageHandler : public StorageHandler {
  public:
    //todo #3956: implement one data structure which wraps these pointers
    explicit TwoPhaseLockingStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                                           TopologyPtr topology,
                                           QueryCatalogServicePtr queryCatalogService,
                                           GlobalQueryPlanPtr globalQueryPlan,
                                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                           Catalogs::UDF::UDFCatalogPtr udfCatalog);

    /**
     * @brief factory to create a two phase locking storage manager object
     * @return shared pointer to the two phase locking storage manager
     */
    static std::shared_ptr<TwoPhaseLockingStorageHandler> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                 TopologyPtr topology,
                                                                 QueryCatalogServicePtr queryCatalogService,
                                                                 GlobalQueryPlanPtr globalQueryPlan,
                                                                 Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                                 Catalogs::UDF::UDFCatalogPtr udfCatalog);

    /**
     * @brief Locks the specified resources ordered after the corresponding enum variants in ResourceType beginning
     * with the first variant. Locking the resources in a specified order will prevent deadlocks.
     * This function can only be executed once during the lifetime of the storage handle. Will throw an
     * exception on second execution attempt. Resources which are not locked using this function can not be locked later on.
     * @param requestId the id of the request calling this function
     * @param requiredResources the types of the resources to be locked
     */
    void acquireResources(RequestId requestId, const std::vector<ResourceType>& requiredResources) override;

    /**
     * Releases all the locks a request holds on any data structure
     * @param requestId the id of the lock holding request
     */
    void releaseResources(RequestId requestId) override;

    /**
     * @brief Obtain a mutable global execution plan handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @param requestId the id of the request calling this function
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable topology handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @param requestId the id of the request calling this function
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable query catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @param requestId the id of the request calling this function
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogServiceHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable global query plan handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @param requestId the id of the request calling this function
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable source catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @param requestId the id of the request calling this function
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable udf catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @param requestId the id of the request calling this function
     * @return a handle to the udf catalog.
     */
    UDFCatalogHandle getUDFCatalogHandle(RequestId requestId) override;

  private:
    /**
     * @brief Locks the resource for the calling request and maintains the lock until resources are released on request of the request
     * @param requestId: The id of the request instance which is trying to lock the resource
     * @param resourceType: The type of resource to be locked
     */
    void lockResource(ResourceType resourceType, RequestId requestId);

    /**
     * @brief get a mutable reference to the member variable which stores the id of the request which currently holds the lock
     * on a resource
     * @param resourceType type of the resource for which the lock info is requested
     * @return an atomic containing the id of the lock holding request or INVALID_REQUEST_ID if the resource is not currently locked
     */
    std::atomic<RequestId>& getHolder(ResourceType resourceType);

    /**
     * @brief indicates if a request holds a lock on any resource
     * @param requestId the id of the request in question
     * @return true if the request holds a lock to at least one resource
     */
    bool isLockHolder(RequestId requestId);

    //resource pointers
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;

    std::atomic<RequestId> globalExecutionPlanHolder;
    std::atomic<RequestId> topologyHolder;
    std::atomic<RequestId> queryCatalogServiceHolder;
    std::atomic<RequestId> globalQueryPlanHolder;
    std::atomic<RequestId> sourceCatalogHolder;
    std::atomic<RequestId> udfCatalogHolder;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_TWOPHASELOCKINGSTORAGEHANDLER_HPP_
