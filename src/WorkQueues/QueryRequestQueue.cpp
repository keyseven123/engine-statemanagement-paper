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

#include <Catalogs/QueryCatalogEntry.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <algorithm>

namespace NES {

QueryRequestQueue::QueryRequestQueue(uint32_t batchSize) : newRequestAvailable(false), batchSize(batchSize) {
    NES_DEBUG("QueryRequestQueue()");
}

QueryRequestQueue::~QueryRequestQueue() { NES_DEBUG("~QueryRequestQueue()"); }

bool QueryRequestQueue::add(QueryCatalogEntryPtr queryCatalogEntry) {
    std::unique_lock<std::mutex> lock(queryRequest);
    QueryId queryId = queryCatalogEntry->getQueryId();
    NES_INFO("QueryRequestQueue: Adding a new query request for query: " << queryId);
    auto itr = std::find_if(schedulingQueue.begin(), schedulingQueue.end(), [&](auto queryRequest) {
        return queryRequest.getQueryId() == queryId;
    });

    if (itr != schedulingQueue.end()) {
        NES_INFO("QueryRequestQueue: Found query with same id already present in the query request queue for processing.");
        NES_INFO("QueryRequestQueue: Changing the status of already present entry in the request queue to:"
                 << queryCatalogEntry->getQueryStatus());
        itr->setQueryStatus(queryCatalogEntry->getQueryStatus());
    } else {
        NES_INFO("QueryRequestQueue: Adding query with id " << queryId << " to the scheduling queue");
        //Save a copy of the catalog entry to prevent it from changes happening in the catalog
        schedulingQueue.push_back(queryCatalogEntry->copy());
    }
    NES_INFO("QueryCatalog: Marking that new request is available to be scheduled");
    setNewRequestAvailable(true);
    availabilityTrigger.notify_one();
    return true;
}

std::vector<QueryCatalogEntry> QueryRequestQueue::getNextBatch() {
    std::unique_lock<std::mutex> lock(queryRequest);
    //We are using conditional variable to prevent Lost Wakeup and Spurious Wakeup
    //ref: https://www.modernescpp.com/index.php/c-core-guidelines-be-aware-of-the-traps-of-condition-variables
    availabilityTrigger.wait(lock, [&] {
        return isNewRequestAvailable();
    });
    NES_INFO("QueryRequestQueue: Fetching Queries to Schedule");
    std::vector<QueryCatalogEntry> queriesToSchedule;
    if (!schedulingQueue.empty()) {
        uint64_t currentBatchSize = 1;
        uint64_t totalQueriesToSchedule = schedulingQueue.size();
        //Prepare a batch of queries to schedule
        while (currentBatchSize <= batchSize || currentBatchSize == totalQueriesToSchedule) {
            queriesToSchedule.push_back(schedulingQueue.front());
            schedulingQueue.pop_front();
            currentBatchSize++;
        }
        NES_INFO("QueryRequestQueue: Scheduling " << queriesToSchedule.size() << " queries.");
        setNewRequestAvailable(!schedulingQueue.empty());
        return queriesToSchedule;
    }
    NES_INFO("QueryRequestQueue: Nothing to schedule.");
    setNewRequestAvailable(!schedulingQueue.empty());
    return queriesToSchedule;
}

void QueryRequestQueue::insertPoisonPill() {
    std::unique_lock<std::mutex> lock(queryRequest);
    NES_INFO("QueryRequestQueue: Shutdown is called. Inserting Poison pill in the query request queue.");
    setNewRequestAvailable(true);
    availabilityTrigger.notify_one();
}

bool QueryRequestQueue::isNewRequestAvailable() const { return newRequestAvailable; }

void QueryRequestQueue::setNewRequestAvailable(bool newRequestAvailable) { this->newRequestAvailable = newRequestAvailable; }

}// namespace NES