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

#include <SingleNodeWorker.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <utility>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticPrinter.hpp>
#include <ThroughputListener.hpp>

namespace NES
{
SingleNodeWorker::~SingleNodeWorker()
{
    for (const auto& listener : queryEngineStatisticsListener)
    {
        listener->onNodeShutdown();
    }
};
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : compiler(std::make_unique<QueryCompilation::QueryCompiler>())
    , bufferSize(configuration.workerConfiguration.bufferSizeInBytes.getValue())
    , optimizer(std::make_unique<QueryOptimizer>(configuration.workerConfiguration.queryOptimizer))
{
    if (configuration.workerConfiguration.bufferSizeInBytes.getValue()
        < configuration.workerConfiguration.queryOptimizer.operatorBufferSize.getValue())
    {
        throw InvalidConfigParameter(
            "Currently, we require the bufferSizeInBytes {} to be at least the operatorBufferSize {}",
            configuration.workerConfiguration.bufferSizeInBytes.getValue(),
            configuration.workerConfiguration.queryOptimizer.operatorBufferSize.getValue());
    }

    /// Writing the current throughput to the log
    auto callback = [](const ThroughputListener::CallBackParams& callBackParams)
    {
        /// Helper function to format throughput in SI units
        auto formatThroughput = [](double throughput, const std::string_view suffix)
        {
            constexpr std::array<const char*, 5> units = {"", "k", "M", "G", "T"};
            int unitIndex = 0;

            while (throughput >= 1000 && unitIndex < 4)
            {
                throughput /= 1000;
                unitIndex++;
            }

            return fmt::format("{:.3f} {}{}/s", throughput, units[unitIndex], suffix);
        };

        const auto bytesPerSecondMessage = formatThroughput(callBackParams.throughputInBytesPerSec, "B");
        const auto tuplesPerSecondMessage = formatThroughput(callBackParams.throughputInTuplesPerSec, "Tup");
        std::cout << fmt::format(
            "Throughput for queryId {} in window {}-{} is {} / {}\n",
            callBackParams.queryId,
            callBackParams.windowStart,
            callBackParams.windowEnd,
            bytesPerSecondMessage,
            tuplesPerSecondMessage);
    };
    constexpr auto timeIntervalInMilliSeconds = 500;
    const auto throughputListener = std::make_shared<ThroughputListener>(timeIntervalInMilliSeconds, callback);

    const auto printStatisticListener = std::make_shared<PrintingStatisticListener>(
        fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid()));
    queryEngineStatisticsListener = {printStatisticListener, throughputListener};
    systemEventListener = printStatisticListener;
    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, systemEventListener, queryEngineStatisticsListener).build();
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) const
{
    try
    {
        plan.setQueryId(QueryId(queryIdCounter++));
        auto queryPlan = optimizer->optimize(plan);
        systemEventListener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        auto result = compiler->compileQuery(std::move(request));
        return nodeEngine->registerCompiledQueryPlan(std::move(result));
    }
    catch (Exception& e)
    {
        tryLogCurrentException();
        return std::unexpected(e);
    }
}

void SingleNodeWorker::startQuery(QueryId queryId)
{
    nodeEngine->startQuery(queryId);
}

void SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type)
{
    nodeEngine->stopQuery(queryId, type);
}

void SingleNodeWorker::unregisterQuery(QueryId queryId)
{
    nodeEngine->unregisterQuery(queryId);
}

std::optional<QuerySummary> SingleNodeWorker::getQuerySummary(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getQuerySummary(queryId);
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

}
