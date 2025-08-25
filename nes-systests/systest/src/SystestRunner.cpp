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

#include <SystestRunner.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected> /// NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <queue>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <QueryManager/EmbeddedWorkerQueryManager.hpp>
#include <fmt/base.h>
#include <fmt/color.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <folly/MPMCQueue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <nlohmann/json.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
namespace
{
template <typename ErrorCallable>
void reportResult(
    std::shared_ptr<RunningQuery>& runningQuery,
    std::size_t& finishedCount,
    const std::size_t total,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    ErrorCallable&& errorBuilder,
    SuccessCallback&& successCallback)
{
    const std::string errorMessage = errorBuilder();
    runningQuery->passed = errorMessage.empty();
    std::string queryPerformanceMessage = "";
    if (not errorMessage.empty())
    {
        failed.push_back(runningQuery);
    }
    else
    {
        queryPerformanceMessage = successCallback(*runningQuery);
    }
    printQueryResultToStdOut(*runningQuery, errorMessage, finishedCount++, total, queryPerformanceMessage);
}

bool passes(const std::shared_ptr<RunningQuery>& runningQuery)
{
    return runningQuery->passed;
}

void processQueryWithError(
    std::shared_ptr<RunningQuery> runningQuery,
    std::size_t& finished,
    const size_t numQueries,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    const std::optional<Exception>& exception)
{
    runningQuery->exception = exception;
    reportResult(
        runningQuery,
        finished,
        numQueries,
        failed,
        [&]
        {
            if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError)
                and std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code
                    == runningQuery->exception->code())
            {
                return std::string{};
            }
            return fmt::format("unexpected parsing error: {}", *runningQuery->exception);
        },
        [](auto) { return ""; });
}

}

/// NOLINTBEGIN(readability-function-cognitive-complexity)
std::vector<RunningQuery> runQueries(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    QuerySubmitter& querySubmitter,
    SuccessCallback&& successCallback)
{
    std::queue<SystestQuery> pending;
    for (auto it = queries.rbegin(); it != queries.rend(); ++it)
    {
        pending.push(*it);
    }

    std::unordered_map<QueryId, std::shared_ptr<RunningQuery>> active;
    std::vector<std::shared_ptr<RunningQuery>> failed;
    std::size_t finished = 0;

    const auto startMoreQueries = [&] -> bool
    {
        bool hasOneMoreQueryToStart = false;
        while (active.size() < numConcurrentQueries and not pending.empty())
        {
            SystestQuery nextQuery = std::move(pending.front());
            pending.pop();

            if (nextQuery.planInfoOrException.has_value())
            {
                /// Registration
                if (auto reg = querySubmitter.registerQuery(nextQuery.planInfoOrException.value().queryPlan))
                {
                    hasOneMoreQueryToStart = true;
                    querySubmitter.startQuery(*reg);
                    active.emplace(*reg, std::make_shared<RunningQuery>(nextQuery, *reg));
                }
                else
                {
                    processQueryWithError(std::make_shared<RunningQuery>(nextQuery), finished, queries.size(), failed, {reg.error()});
                }
            }
            else
            {
                /// There was an error during query parsing, report the result and don't register the query
                processQueryWithError(
                    std::make_shared<RunningQuery>(nextQuery), finished, queries.size(), failed, {nextQuery.planInfoOrException.error()});
            }
        }
        return hasOneMoreQueryToStart;
    };

    while (startMoreQueries() or not(active.empty() and pending.empty()))
    {
        for (const auto& summary : querySubmitter.finishedQueries())
        {
            auto it = active.find(summary.queryId);
            if (it == active.end())
            {
                throw TestException("received unregistered queryId: {}", summary.queryId);
            }

            auto& runningQuery = it->second;

            if (summary.currentStatus == QueryStatus::Failed)
            {
                INVARIANT(summary.runs.back().error, "A query that failed must have a corresponding error.");
                processQueryWithError(it->second, finished, queries.size(), failed, summary.runs.back().error);
            }
            else
            {
                reportResult(
                    runningQuery,
                    finished,
                    queries.size(),
                    failed,
                    [&]
                    {
                        if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError))
                        {
                            return fmt::format(
                                "expected error {} but query succeeded",
                                std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code);
                        }
                        runningQuery->querySummary = summary;
                        if (auto err = checkResult(*runningQuery))
                        {
                            return *err;
                        }
                        return std::string{};
                    },
                    std::move(successCallback));
            }
            active.erase(it);
        }
    }

    auto failedViews = failed | std::views::filter(std::not_fn(passes)) | std::views::transform([](auto& p) { return *p; });
    return {failedViews.begin(), failedViews.end()};
}

/// NOLINTEND(readability-function-cognitive-complexity)

namespace
{
std::vector<RunningQuery> serializeExecutionResults(const std::vector<RunningQuery>& queries, nlohmann::json& resultJson)
{
    std::vector<RunningQuery> failedQueries;
    for (const auto& queryRan : queries)
    {
        if (!queryRan.passed)
        {
            failedQueries.emplace_back(queryRan);
        }
        const auto executionTimeInSeconds = queryRan.getElapsedTime().count();
        resultJson.push_back({
            {"query_name", fmt::format("{}_{}", queryRan.systestQuery.testName, queryRan.systestQuery.queryIdInFile)},
            {"time", executionTimeInSeconds},
            {"bytesPerSecond",
             queryRan.bytesProcessed.has_value() ? static_cast<double>(queryRan.bytesProcessed.value()) / executionTimeInSeconds
                                                 : std::numeric_limits<double>::quiet_NaN()},
            {"tuplesPerSecond",
             queryRan.tuplesProcessed.has_value() ? static_cast<double>(queryRan.tuplesProcessed.value()) / executionTimeInSeconds
                                                  : std::numeric_limits<double>::quiet_NaN()},
        });
    }
    return failedQueries;
}
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<SystestQuery>& queries, const SingleNodeWorkerConfiguration& configuration, nlohmann::json& resultJson)
{
    auto worker = std::make_unique<EmbeddedWorkerQueryManager>(configuration);
    QuerySubmitter submitter(std::move(worker));
    constexpr auto numConcurrentQueries = 1;

    auto successCallback = [](RunningQuery& runningQuery)
    {
        /// Getting the size and no. tuples of all input files to pass this information to currentRunningQuery.bytesProcessed
        size_t bytesProcessed = 0;
        size_t tuplesProcessed = 0;
        for (const auto& [sourcePath, sourceOccurrencesInQuery] :
             runningQuery.systestQuery.planInfoOrException.value().sourcesToFilePathsAndCounts | std::views::values)
        {
            if (not(std::filesystem::exists(sourcePath.getRawValue()) and sourcePath.getRawValue().has_filename()))
            {
                NES_ERROR("Source path is empty or does not exist.");
                bytesProcessed = 0;
                tuplesProcessed = 0;
                break;
            }

            bytesProcessed += (std::filesystem::file_size(sourcePath.getRawValue()) * sourceOccurrencesInQuery);

            /// Counting the lines, i.e., \n in the sourcePath
            std::ifstream inFile(sourcePath.getRawValue());
            tuplesProcessed
                += std::count(std::istreambuf_iterator(inFile), std::istreambuf_iterator<char>(), '\n') * sourceOccurrencesInQuery;
        }
        runningQuery.bytesProcessed = bytesProcessed;
        runningQuery.tuplesProcessed = tuplesProcessed;
        return fmt::format(" in {} ({})", runningQuery.getElapsedTime(), runningQuery.getThroughput());
    };

    const auto ranQueries = runQueries(queries, numConcurrentQueries, submitter, successCallback);
    return serializeExecutionResults(ranQueries, resultJson);
}

void printQueryResultToStdOut(
    const RunningQuery& runningQuery,
    const std::string& errorMessage,
    const size_t queryCounter,
    const size_t totalQueries,
    const std::string_view queryPerformanceMessage)
{
    const auto queryNameLength = runningQuery.systestQuery.testName.size();
    const auto queryNumberAsString = runningQuery.systestQuery.queryIdInFile.toString();
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << runningQuery.systestQuery.testName << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0')
              << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (runningQuery.passed)
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", queryPerformanceMessage);
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.systestQuery.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", errorMessage);
        std::cout << "===================================================================" << '\n';
    }
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries, const uint64_t numConcurrentQueries, const SingleNodeWorkerConfiguration& configuration)
{
    auto embeddedQueryManager = std::make_unique<EmbeddedWorkerQueryManager>(configuration);
    QuerySubmitter submitter(std::move(embeddedQueryManager));
    return runQueries(queries, numConcurrentQueries, submitter);
}

std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<SystestQuery>& queries, const uint64_t numConcurrentQueries, const std::string& serverURI)
{
    auto remoteQueryManager = std::make_unique<GRPCQueryManager>(CreateChannel(serverURI, grpc::InsecureChannelCredentials()));
    QuerySubmitter submitter(std::move(remoteQueryManager));
    return runQueries(queries, numConcurrentQueries, submitter);
}

}
