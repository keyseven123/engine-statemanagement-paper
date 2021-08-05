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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <utility>

namespace NES {

CoordinatorConfigPtr CoordinatorConfig::create() { return std::make_shared<CoordinatorConfig>(CoordinatorConfig()); }

CoordinatorConfig::CoordinatorConfig() {
    NES_INFO("Generated new Coordinator Config object. Configurations initialized with default values.");
    restIp = ConfigOption<std::string>::create("restIp", "127.0.0.1", "NES ip of the REST server.");
    coordinatorIp = ConfigOption<std::string>::create("coordinatorIp", "127.0.0.1", "RPC IP address of NES Coordinator.");
    rpcPort = ConfigOption<uint32_t>::create("rpcPort", 4000, "RPC server port of the NES Coordinator");
    restPort = ConfigOption<uint32_t>::create("restPort", 8081, "Port exposed for rest endpoints");
    dataPort = ConfigOption<uint32_t>::create("dataPort", 3001, "NES data server port");
    numberOfSlots = ConfigOption<uint32_t>::create("numberOfSlots", UINT16_MAX, "Number of computing slots for NES Coordinator");
    logLevel = ConfigOption<std::string>::create("logLevel",
                                                 "LOG_DEBUG",
                                                 "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)");
    numberOfBuffersInGlobalBufferManager =
        ConfigOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1024, "Number buffers in global buffer pool.");
    numberOfBuffersPerPipeline =
        ConfigOption<uint32_t>::create("numberOfBuffersPerPipeline", 128, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool",
                                                                            64,
                                                                            "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigOption<uint32_t>::create("bufferSizeInBytes", 4096, "BufferSizeInBytes.");
    numWorkerThreads = ConfigOption<uint32_t>::create("numWorkerThreads", 1, "Number of worker threads.");
    queryBatchSize = ConfigOption<uint32_t>::create("queryBatchSize", 1, "The number of queries to be processed together");

    queryMergerRule = ConfigOption<std::string>::create("queryMergerRule",
                                                        "DefaultQueryMergerRule",
                                                        "The rule to be used for performing query merging");

    enableSemanticQueryValidation =
        ConfigOption<bool>::create("enableSemanticQueryValidation", false, "Enable semantic query validation feature");

    queryReconfiguration =
        ConfigOption<bool>::create("queryReconfiguration", false, "Toggles query reconfiguration functionality after merging");
}

void CoordinatorConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("CoordinatorConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config["restPort"].As<std::string>().empty() && config["restPort"].As<std::string>() != "\n") {
                setRestPort(config["restPort"].As<uint16_t>());
            }
            if (!config["rpcPort"].As<std::string>().empty() && config["rpcPort"].As<std::string>() != "\n") {
                setRpcPort(config["rpcPort"].As<uint16_t>());
            }
            if (!config["dataPort"].As<std::string>().empty() && config["dataPort"].As<std::string>() != "\n") {
                setDataPort(config["dataPort"].As<uint16_t>());
            }
            if (!config["restIp"].As<std::string>().empty() && config["restIp"].As<std::string>() != "\n") {
                setRestIp(config["restIp"].As<std::string>());
            }
            if (!config["coordinatorIp"].As<std::string>().empty() && config["coordinatorIp"].As<std::string>() != "\n") {
                setCoordinatorIp(config["coordinatorIp"].As<std::string>());
            }
            if (!config["numberOfSlots"].As<std::string>().empty() && config["numberOfSlots"].As<std::string>() != "\n") {
                setNumberOfSlots(config["numberOfSlots"].As<uint16_t>());
            }
            if (!config["logLevel"].As<std::string>().empty() && config["logLevel"].As<std::string>() != "\n") {
                setLogLevel(config["logLevel"].As<std::string>());
            }
            if (!config["queryBatchSize"].As<std::string>().empty() && config["queryBatchSize"].As<std::string>() != "\n") {
                setQueryBatchSize(config["queryBatchSize"].As<uint32_t>());
            }
            if (!config["numberOfBuffersInGlobalBufferManager"].As<std::string>().empty()
                && config["numberOfBuffersInGlobalBufferManager"].As<std::string>() != "\n") {
                setNumberOfBuffersInGlobalBufferManager(config["numberOfBuffersInGlobalBufferManager"].As<uint32_t>());
            }
            if (!config["numberOfBuffersPerPipeline"].As<std::string>().empty()
                && config["numberOfBuffersPerPipeline"].As<std::string>() != "\n") {
                setNumberOfBuffersPerPipeline(config["numberOfBuffersPerPipeline"].As<uint32_t>());
            }
            if (!config["numberOfBuffersInSourceLocalBufferPool"].As<std::string>().empty()
                && config["numberOfBuffersInSourceLocalBufferPool"].As<std::string>() != "\n") {
                setNumberOfBuffersInSourceLocalBufferPool(config["numberOfBuffersInSourceLocalBufferPool"].As<uint32_t>());
            }
            if (!config["bufferSizeInBytes"].As<std::string>().empty() && config["bufferSizeInBytes"].As<std::string>() != "\n") {
                setBufferSizeInBytes(config["bufferSizeInBytes"].As<uint32_t>());
            }
            if (!config["queryBatchSize"].As<std::string>().empty() && config["queryBatchSize"].As<std::string>() != "\n") {
                setBufferSizeInBytes(config["queryBatchSize"].As<uint32_t>());
            }
            if (!config["queryMergerRule"].As<std::string>().empty() && config["queryMergerRule"].As<std::string>() != "\n") {
                setQueryMergerRule(config["queryMergerRule"].As<std::string>());
            }
            if (!config["enableSemanticQueryValidation"].As<std::string>().empty()
                && config["enableSemanticQueryValidation"].As<std::string>() != "\n") {
                setEnableSemanticQueryValidation(config["enableSemanticQueryValidation"].As<bool>());
            }
            if (!config["queryReconfiguration"].As<std::string>().empty()
                && config["queryReconfiguration"].As<std::string>() != "\n") {
                setQueryReconfiguration(config["enableSemanticQueryValidation"].As<bool>());
            }
            if (!config["numWorkerThreads"].As<std::string>().empty() && config["numWorkerThreads"].As<std::string>() != "\n") {
                setNumWorkerThreads(config["numWorkerThreads"].As<uint32_t>());
            }
        } catch (std::exception& e) {
            NES_ERROR("CoordinatorConfig: Error while initializing configuration parameters from YAML file. " << e.what());
            NES_WARNING("CoordinatorConfig: Keeping default values.");
            resetCoordinatorOptions();
        }
        return;
    }
    NES_ERROR("CoordinatorConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("CoordinatorConfig: Keeping default values for Coordinator Config.");
}

void CoordinatorConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--restIp" && !it->second.empty()) {
                setRestIp(it->second);
            } else if (it->first == "--coordinatorIp" && !it->second.empty()) {
                setCoordinatorIp(it->second);
            } else if (it->first == "--coordinatorPort" && !it->second.empty()) {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--restPort" && !it->second.empty()) {
                setRestPort(stoi(it->second));
            } else if (it->first == "--dataPort" && !it->second.empty()) {
                setDataPort(stoi(it->second));
            } else if (it->first == "--numberOfSlots" && !it->second.empty()) {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--logLevel" && !it->second.empty()) {
                setLogLevel(it->second);
            } else if (it->first == "--numberOfBuffersInGlobalBufferManager" && !it->second.empty()) {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--numberOfBuffersPerPipeline" && !it->second.empty()) {
                setNumberOfBuffersPerPipeline(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInSourceLocalBufferPool" && !it->second.empty()) {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--bufferSizeInBytes" && !it->second.empty()) {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--numWorkerThreads" && !it->second.empty()) {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--queryBatchSize" && !it->second.empty()) {
                setQueryBatchSize(stoi(it->second));
            } else if (it->first == "--queryMergerRule" && !it->second.empty()) {
                setQueryMergerRule(it->second);
            } else if (it->first == "--enableSemanticQueryValidation" && !it->second.empty()) {
                setEnableSemanticQueryValidation((it->second == "true"));
            } else if (it->first == "--queryReconfiguration" && !it->second.empty()) {
                setQueryReconfiguration((it->second == "true"));
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("CoordinatorConfig: Error while initializing configuration parameters from command line.");
        NES_WARNING("CoordinatorConfig: Keeping default values.");
        resetCoordinatorOptions();
    }
}

void CoordinatorConfig::resetCoordinatorOptions() {
    setRestPort(restPort->getDefaultValue());
    setRpcPort(rpcPort->getDefaultValue());
    setDataPort(dataPort->getDefaultValue());
    setRestIp(restIp->getDefaultValue());
    setCoordinatorIp(coordinatorIp->getDefaultValue());
    setNumberOfSlots(numberOfSlots->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
    setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager->getDefaultValue());
    setNumberOfBuffersPerPipeline(numberOfBuffersPerPipeline->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setBufferSizeInBytes(bufferSizeInBytes->getDefaultValue());
    setNumWorkerThreads(numWorkerThreads->getDefaultValue());
    setQueryBatchSize(queryBatchSize->getDefaultValue());
    setQueryMergerRule(queryMergerRule->getDefaultValue());
    setEnableSemanticQueryValidation(enableSemanticQueryValidation->getDefaultValue());
}

StringConfigOption CoordinatorConfig::getRestIp() { return restIp; }

void CoordinatorConfig::setRestIp(std::string restIpValue) { restIp->setValue(std::move(restIpValue)); }

StringConfigOption CoordinatorConfig::getCoordinatorIp() { return coordinatorIp; }

void CoordinatorConfig::setCoordinatorIp(std::string coordinatorIpValue) {
    coordinatorIp->setValue(std::move(coordinatorIpValue));
}

IntConfigOption CoordinatorConfig::getRpcPort() { return rpcPort; }

void CoordinatorConfig::setRpcPort(uint16_t rpcPortValue) { rpcPort->setValue(rpcPortValue); }

IntConfigOption CoordinatorConfig::getRestPort() { return restPort; }

void CoordinatorConfig::setRestPort(uint16_t restPortValue) { restPort->setValue(restPortValue); }

IntConfigOption CoordinatorConfig::getDataPort() { return dataPort; }

void CoordinatorConfig::setDataPort(uint16_t dataPortValue) { dataPort->setValue(dataPortValue); }

IntConfigOption CoordinatorConfig::getNumberOfSlots() { return numberOfSlots; }

void CoordinatorConfig::setNumberOfSlots(uint16_t numberOfSlotsValue) { numberOfSlots->setValue(numberOfSlotsValue); }

void CoordinatorConfig::setNumWorkerThreads(uint16_t numWorkerThreadsValue) { numWorkerThreads->setValue(numWorkerThreadsValue); }

IntConfigOption CoordinatorConfig::getNumWorkerThreads() { return numWorkerThreads; }

StringConfigOption CoordinatorConfig::getLogLevel() { return logLevel; }

void CoordinatorConfig::setLogLevel(std::string logLevelValue) { logLevel->setValue(std::move(logLevelValue)); }

IntConfigOption CoordinatorConfig::getNumberOfBuffersInGlobalBufferManager() { return numberOfBuffersInGlobalBufferManager; }
IntConfigOption CoordinatorConfig::getNumberOfBuffersPerPipeline() { return numberOfBuffersPerPipeline; }
IntConfigOption CoordinatorConfig::getNumberOfBuffersInSourceLocalBufferPool() { return numberOfBuffersInSourceLocalBufferPool; }

void CoordinatorConfig::setNumberOfBuffersInGlobalBufferManager(uint64_t count) {
    numberOfBuffersInGlobalBufferManager->setValue(count);
}
void CoordinatorConfig::setNumberOfBuffersPerPipeline(uint64_t count) { numberOfBuffersPerPipeline->setValue(count); }
void CoordinatorConfig::setNumberOfBuffersInSourceLocalBufferPool(uint64_t count) {
    numberOfBuffersInSourceLocalBufferPool->setValue(count);
}

IntConfigOption CoordinatorConfig::getBufferSizeInBytes() { return bufferSizeInBytes; }

void CoordinatorConfig::setBufferSizeInBytes(uint64_t sizeInBytes) { bufferSizeInBytes->setValue(sizeInBytes); }

IntConfigOption CoordinatorConfig::getQueryBatchSize() { return queryBatchSize; }

void CoordinatorConfig::setQueryBatchSize(uint32_t batchSize) { queryBatchSize->setValue(batchSize); }

StringConfigOption CoordinatorConfig::getQueryMergerRule() { return queryMergerRule; }

void CoordinatorConfig::setQueryMergerRule(std::string queryMergerRuleValue) {
    queryMergerRule->setValue(std::move(queryMergerRuleValue));
}

BoolConfigOption CoordinatorConfig::getEnableSemanticQueryValidation() { return enableSemanticQueryValidation; }

void CoordinatorConfig::setEnableSemanticQueryValidation(bool enableSemanticQueryValidation) {
    CoordinatorConfig::enableSemanticQueryValidation->setValue(enableSemanticQueryValidation);
}
BoolConfigOption CoordinatorConfig::getQueryReconfiguration() { return queryReconfiguration; }

void CoordinatorConfig::setQueryReconfiguration(bool queryReconfigurationState) {
    CoordinatorConfig::queryReconfiguration->setValue(queryReconfigurationState);
}

}// namespace NES
