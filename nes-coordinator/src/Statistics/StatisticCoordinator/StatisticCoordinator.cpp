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

#include <algorithm>

#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticStorageSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptorFactory.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Statistics/Requests/StatisticCreateRequest.hpp>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/StatisticCoordinator/StatisticCoordinator.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Experimental::Statistics {

StatisticCoordinator::StatisticCoordinator(QueryServicePtr queryService,
                                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                           StatisticsMode statisticsMode)
    : queryService(std::move(queryService)), sourceCatalog(std::move(sourceCatalog)), workerClient(WorkerRPCClient::create()),
      statisticsMode(statisticsMode) {}

QueryId StatisticCoordinator::createStatistic(StatisticCreateRequest& createRequest) {

    auto fieldName = createRequest.getFieldName();
    if (createRequest.getFieldName().find(createRequest.getLogicalSourceName()) == std::string::npos) {
        fieldName = createRequest.getLogicalSourceName() + "$" + createRequest.getFieldName();
    }

    auto statisticQueryIdentifier =
        StatisticQueryIdentifier(createRequest.getLogicalSourceName(), fieldName, createRequest.getStatisticCollectorType());
    auto statisticQueryIdIt = trackedStatistics.find(statisticQueryIdentifier);

    QueryId queryId;
    if (statisticQueryIdIt == trackedStatistics.end()) {

        NES_DEBUG("Statistic does not yet exist");
        // ToDo: add logic for different queries in later issue
        const auto& sourceName = statisticQueryIdentifier.getLogicalSourceName();

        WindowStatisticDescriptorPtr windowStatisticDesc = nullptr;
        auto statisticSinkDesc = StatisticStorageSinkDescriptor::create(createRequest.getStatisticCollectorType(),
                                                                        createRequest.getLogicalSourceName());

        switch (createRequest.getStatisticCollectorType()) {
            case StatisticCollectorType::COUNT_MIN:
                windowStatisticDesc =
                    WindowStatisticDescriptorFactory::createCountMinDescriptor(createRequest.getLogicalSourceName(),
                                                                               fieldName,
                                                                               createRequest.getTimestampField(),
                                                                               createRequest.getDepth(),
                                                                               createRequest.getWindowSize(),
                                                                               createRequest.getWindowSlide(),
                                                                               createRequest.getWidth());
                break;
            case StatisticCollectorType::DDSKETCH: NES_ERROR("StatisticType not implemented!"); break;
            case StatisticCollectorType::HYPER_LOG_LOG: NES_ERROR("StatisticType not implemented!"); break;
            case StatisticCollectorType::RESERVOIR:
                windowStatisticDesc =
                    WindowStatisticDescriptorFactory::createReservoirSampleDescriptor(createRequest.getLogicalSourceName(),
                                                                                      fieldName,
                                                                                      createRequest.getTimestampField(),
                                                                                      createRequest.getWindowSize(),
                                                                                      createRequest.getWindowSlide(),
                                                                                      createRequest.getDepth());
                break;
            default: NES_ERROR("StatisticType is unknown!");
        }

        auto query = Query::from(sourceName).buildStatistic(windowStatisticDesc).sink(statisticSinkDesc);

        NES::Optimizer::PlacementStrategy placementStrategy;
        switch (statisticsMode) {
            case NES::Experimental::Statistics::StatisticsMode::CENTRALIZED_MODE:
                placementStrategy = Optimizer::PlacementStrategy::BottomUp;
                break;
            case NES::Experimental::Statistics::StatisticsMode::HYBRID_MODE:
                placementStrategy = Optimizer::PlacementStrategy::BottomUp;
                break;
            case NES::Experimental::Statistics::StatisticsMode::DECENTRALIZED_MODE:
                placementStrategy = Optimizer::PlacementStrategy::BottomUp;
                break;
        }

        if (placementStrategy == Optimizer::PlacementStrategy::Manual) {
            // create placement strategy
            // NES::GlobalExecutionPlanPtr globalExecutionPlan,
            // NES::TopologyPtr topology,
            // NES::Optimizer::TypeInferencePhasePtr typeInferencePhase

            // Optimizer::TypeInferencePhasePtr::create(Catalogs::Source::SourceCatalogPtr sourceCatalog,
            //                                          Catalogs::UDF::UDFCatalogPtr udfCatalog)

        }

        queryId = queryService->validateAndQueueAddQueryRequest("", query.getQueryPlan(), placementStrategy);
        NES_DEBUG("Adding statistic to the unordered_map of tracked statistics");
        trackedStatistics[statisticQueryIdentifier] = queryId;

        return queryId;

    } else {

        NES_DEBUG("Statistic already exists. QueryID of the generating query is returned")
        queryId = statisticQueryIdIt->second;
        return queryId;
    }
}

std::vector<double> StatisticCoordinator::probeStatistic(StatisticProbeRequest& probeRequest) {

    std::vector<double> statistics;
    std::vector<double> erroneousResult{-1.0};

    auto fieldName = probeRequest.getFieldName();
    if (probeRequest.getFieldName().find(probeRequest.getLogicalSourceName()) == std::string::npos) {
        fieldName = probeRequest.getLogicalSourceName() + "$" + probeRequest.getFieldName();
    }

    auto queriedPhysicalSources = probeRequest.getPhysicalSourceNames();
    probeRequest.clearPhysicalSourceNames();

    // get all physicalSources and sort them. Sorting them according to the node allows us to combine requests to nodes that
    // have multiple physicalSource we want to query
    auto allPhysicalSources =
        sourceCatalog->getSubsetOfPhysicalSources(probeRequest.getLogicalSourceName(), queriedPhysicalSources);
    std::sort(allPhysicalSources.begin(),
              allPhysicalSources.end(),
              [](const Catalogs::Source::SourceCatalogEntryPtr& entry1, const Catalogs::Source::SourceCatalogEntryPtr& entry2) {
                  return entry1->getNode()->getId() < entry2->getNode()->getId();
              });

    if (allPhysicalSources[0] == nullptr) {
        /**
         * if a single statistic cannot be found, then we return a vector with the singular error value -1
         * as we potentially otherwise run into undefined behavior about how to combine only the available
         * statistics
         */
        return erroneousResult;
    }

    // all the desired physicalSource exist
    auto statisticQueryIdentifier =
        StatisticQueryIdentifier(probeRequest.getLogicalSourceName(), fieldName, probeRequest.getStatisticCollectorType());

    auto statisticQueryPairIt = trackedStatistics.find(statisticQueryIdentifier);

    if (statisticQueryPairIt == trackedStatistics.end()) {
        NES_DEBUG("Statistic cannot be queried, as it is not being generated.");
        return erroneousResult;
    } else {
        NES_DEBUG("Statistic is being generated. Proceeding with probe operation.");
        if (statisticsMode == Experimental::Statistics::StatisticsMode::DECENTRALIZED_MODE) {

            for (uint64_t index = 0; index < allPhysicalSources.size(); index++) {
                auto sourceCatalogEntry = allPhysicalSources.at(index);
                auto physicalSourceName = sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName();
                probeRequest.addPhysicalSourceName(physicalSourceName);
                auto node = sourceCatalogEntry->getNode();
                std::string destAddress = node->getIpAddress() + ":" + std::to_string(node->getGrpcPort());
                for (uint64_t followUpIndexes = index + 1; followUpIndexes < allPhysicalSources.size(); followUpIndexes++) {
                    sourceCatalogEntry = allPhysicalSources.at(followUpIndexes);
                    auto secondNode = sourceCatalogEntry->getNode();
                    if (node == secondNode) {
                        physicalSourceName = sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName();
                        probeRequest.addPhysicalSourceName(physicalSourceName);
                    } else {
                        break;
                    }
                }

                auto localStatistics = workerClient->probeStatistic(destAddress, probeRequest);
                statistics.insert(statistics.end(), localStatistics.begin(), localStatistics.end());
            }
        } else {
            statistics = statisticManager->probeStatistic(probeRequest.getLogicalSourceName(),
                                                          fieldName,
                                                          probeRequest.getStatisticCollectorType(),
                                                          probeRequest.getExpression(),
                                                          queriedPhysicalSources,
                                                          probeRequest.getStartTime(),
                                                          probeRequest.getEndTime());
        }
    }
    if (probeRequest.getMerge()) {
        //ToDo: In future potentially combine fine grained statistics to coarse grained ones
    }

    return statistics;
}

bool StatisticCoordinator::deleteStatistic(StatisticDeleteRequest& deleteRequest) {

    auto fieldName = deleteRequest.getFieldName();
    if (deleteRequest.getFieldName().find(deleteRequest.getLogicalSourceName()) == std::string::npos) {
        fieldName = deleteRequest.getLogicalSourceName() + "$" + deleteRequest.getFieldName();
    }

    // check if statistic(s) even exists
    auto statisticQueryIdentifier =
        StatisticQueryIdentifier(deleteRequest.getLogicalSourceName(), fieldName, deleteRequest.getStatisticCollectorType());

    auto statisticQueryIdIt = trackedStatistics.find(statisticQueryIdentifier);
    if (statisticQueryIdIt == trackedStatistics.end()) {
        // statistic is not being generated, return with error value
        return -1;
    } else {
        auto allAddresses = addressesOfLogicalStream(deleteRequest.getLogicalSourceName());

        // Success equal to one for the moment
        int32_t success = 1;
        for (const std::string& destAddress : allAddresses) {
            success = workerClient->deleteStatistic(destAddress, deleteRequest);
            if (success == -1) {
                return success;
            }
        }
        NES_DEBUG("Trying to stop query!");
        auto queryStopped = queryService->validateAndQueueStopQueryRequest(statisticQueryIdIt->second);
        if (!queryStopped) {
            return -1;
        }

        trackedStatistics.erase(statisticQueryIdentifier);
        NES_DEBUG("StatisticCollectorFormats successfully deleted!");
        NES_DEBUG("Statistic queries successfully stopped!");
        return success;
    }
}

std::vector<std::string> StatisticCoordinator::addressesOfLogicalStream(const std::string& logicalSourceName) {

    std::vector<std::string> addresses;
    auto physicalSources = sourceCatalog->getPhysicalSources(logicalSourceName);
    for (const auto& physicalSource : physicalSources) {
        TopologyNodePtr node = physicalSource->getNode();
        addresses.push_back(node->getIpAddress() + ":" + std::to_string(node->getGrpcPort()));
    }
    return addresses;
}

StatisticsMode StatisticCoordinator::getStatisticsMode() const { return statisticsMode; }

void StatisticCoordinator::setStatisticManager(const StatisticManagerPtr& statisticManager) {
    StatisticCoordinator::statisticManager = statisticManager;
}
}// namespace NES::Experimental::Statistics
