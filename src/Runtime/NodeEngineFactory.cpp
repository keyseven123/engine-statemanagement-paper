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

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <memory>
namespace NES::Runtime {

extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
extern void removeGlobalErrorListener(std::shared_ptr<ErrorListener> const&);

NodeEnginePtr
NodeEngineFactory::createDefaultNodeEngine(const std::string& hostname, uint16_t port, PhysicalStreamConfigPtr config) {
    return createNodeEngine(hostname,
                            port,
                            std::move(config),
                            1,
                            4096,
                            1024,
                            128,
                            12,
                            NumaAwarenessFlag::DISABLED,
                            "",
                            "DEBUG",
                            "ALL");
}

NodeEnginePtr NodeEngineFactory::createNodeEngine(const std::string& hostname,
                                                  const uint16_t port,
                                                  const PhysicalStreamConfigPtr config,
                                                  const uint16_t numThreads,
                                                  const uint64_t bufferSize,
                                                  const uint64_t numberOfBuffersInGlobalBufferManager,
                                                  const uint64_t numberOfBuffersInSourceLocalBufferPool,
                                                  const uint64_t numberOfBuffersPerWorker,
                                                  NumaAwarenessFlag enableNumaAwareness,
                                                  const std::string& workerToCodeMapping,
                                                  const std::string& queryCompilerExecutionMode,
                                                  const std::string& queryCompilerOutputBufferOptimizationLevel) {

    try {
        enableNumaAwareness = NES::Runtime::NumaAwarenessFlag::DISABLED;
        auto nodeEngineId = Util::getNextNodeEngineId();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        auto hardwareManager = std::make_shared<Runtime::HardwareManager>();
        std::vector<BufferManagerPtr> bufferManagers;
#ifdef NES_ENABLE_NUMA_SUPPORT
        if (enableNumaAwareness == NumaAwarenessFlag::ENABLED) {
            auto numberOfBufferPerNumaNode = numberOfBuffersInGlobalBufferManager / hardwareManager->getNumberOfNumaRegions();
            NES_ASSERT2_FMT(numberOfBuffersInSourceLocalBufferPool < numberOfBufferPerNumaNode,
                            "The number of buffer for each numa node: " << numberOfBufferPerNumaNode
                                                                        << " is lower than the fixed size pool: "
                                                                        << numberOfBuffersInSourceLocalBufferPool);
            NES_ASSERT2_FMT(numberOfBuffersPerWorker < numberOfBufferPerNumaNode,
                            "The number of buffer for each numa node: "
                                << numberOfBufferPerNumaNode << " is lower than the pipeline pool: " << numberOfBuffersPerWorker);
            for (auto i = 0u; i < hardwareManager->getNumberOfNumaRegions(); ++i) {
                bufferManagers.push_back(
                    std::make_shared<BufferManager>(bufferSize, numberOfBufferPerNumaNode, hardwareManager->getNumaAllactor(i)));
            }
        } else {
            bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,
                                                                     numberOfBuffersInGlobalBufferManager,
                                                                     hardwareManager->getGlobalAllocator()));
        }
#else
        NES_WARNING("Numa flags " << int(enableNumaAwareness) << " are ignored");
        bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,
                                                                 numberOfBuffersInGlobalBufferManager,
                                                                 hardwareManager->getGlobalAllocator()));
#endif
        if (bufferManagers.empty()) {
            NES_ERROR("Runtime: error while creating buffer manager");
            throw Exception("Error while creating buffer manager");
        }

        QueryManagerPtr queryManager;
        if (workerToCodeMapping != "") {
            std::vector<uint64_t> workerToCoreMapping = Util::splitWithStringDelimiter<uint64_t>(workerToCodeMapping, ",");
            queryManager = std::make_shared<QueryManager>(bufferManagers, nodeEngineId, numThreads, workerToCoreMapping);
        } else {
            queryManager = std::make_shared<QueryManager>(bufferManagers, nodeEngineId, numThreads);
        }

        auto stateManager = std::make_shared<StateManager>(nodeEngineId);
        if (!partitionManager) {
            NES_ERROR("Runtime: error while creating partition manager");
            throw Exception("Error while creating partition manager");
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while creating queryManager");
            throw Exception("Error while creating queryManager");
        }
        if (!stateManager) {
            NES_ERROR("Runtime: error while creating stateManager");
            throw Exception("Error while creating stateManager");
        }
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto queryCompilationOptions =
            createQueryCompilationOptions(queryCompilerExecutionMode, queryCompilerOutputBufferOptimizationLevel);
        auto compiler = QueryCompilation::DefaultQueryCompiler::create(queryCompilationOptions, phaseFactory, jitCompiler);
        if (!compiler) {
            NES_ERROR("Runtime: error while creating compiler");
            throw Exception("Error while creating compiler");
        }
        auto engine = std::make_shared<NodeEngine>(
            config,
            std::move(hardwareManager),
            std::move(bufferManagers),
            std::move(queryManager),
            [hostname, port, numThreads](const std::shared_ptr<NodeEngine>& engine) {
                return Network::NetworkManager::create(hostname,
                                                       port,
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(),
                                                       numThreads);
            },
            std::move(partitionManager),
            std::move(compiler),
            std::move(stateManager),
            nodeEngineId,
            numberOfBuffersInGlobalBufferManager,
            numberOfBuffersInSourceLocalBufferPool,
            numberOfBuffersPerWorker);
        installGlobalErrorListener(engine);
        return engine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
    return nullptr;
}
QueryCompilation::QueryCompilerOptionsPtr
NodeEngineFactory::createQueryCompilationOptions(std::string, std::string queryCompilerOutputBufferOptimizationLevel) {
    auto queryCompilationOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    // set output buffer optimization level
    if (queryCompilerOutputBufferOptimizationLevel == "ALL") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(QueryCompilation::QueryCompilerOptions::ALL);
    } else if (queryCompilerOutputBufferOptimizationLevel == "NO") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(QueryCompilation::QueryCompilerOptions::NO);
    } else if (queryCompilerOutputBufferOptimizationLevel == "REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(
            QueryCompilation::QueryCompilerOptions::REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK);
    } else if (queryCompilerOutputBufferOptimizationLevel == "ONLY_INPLACE_OPERATIONS_NO_FALLBACK") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(
            QueryCompilation::QueryCompilerOptions::ONLY_INPLACE_OPERATIONS_NO_FALLBACK);
    } else if (queryCompilerOutputBufferOptimizationLevel == "REUSE_INPUT_BUFFER_NO_FALLBACK") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(
            QueryCompilation::QueryCompilerOptions::REUSE_INPUT_BUFFER_NO_FALLBACK);
    } else if (queryCompilerOutputBufferOptimizationLevel == "OMIT_OVERFLOW_CHECK_NO_FALLBACK") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(
            QueryCompilation::QueryCompilerOptions::OMIT_OVERFLOW_CHECK_NO_FALLBACK);
    } else if (queryCompilerOutputBufferOptimizationLevel == "NO") {
        queryCompilationOptions->setOutputBufferOptimizationLevel(QueryCompilation::QueryCompilerOptions::NO);
    } else {
        NES_FATAL_ERROR("queryCompilerOutputBufferOptimizationLevel " << queryCompilerOutputBufferOptimizationLevel
                                                                      << " not supported");
    }
    return queryCompilationOptions;
}

}// namespace NES::Runtime