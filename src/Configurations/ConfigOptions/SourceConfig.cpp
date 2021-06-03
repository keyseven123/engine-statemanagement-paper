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
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>

namespace NES {

//TODO: check what crashed by changing the three main function to work with vectors

SourceConfigPtr SourceConfig::create() { return std::make_shared<SourceConfig>(SourceConfig()); }

SourceConfig::SourceConfig() {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
    sourceType =
        ConfigOption<std::string>::create("sourceType",
                                          "DefaultSource",
                                          "Type of the Source (available options: DefaultSource, CSVSource, BinarySource).");
    sourceConfig = ConfigOption<std::string>::create(
        "sourceConfig",
        "1",
        "Source configuration. Options depend on source type. See Source Configurations on our wiki page for further details.");
    sourceFrequency = ConfigOption<uint32_t>::create("sourceFrequency", 1, "Sampling frequency of the source.");
    numberOfBuffersToProduce = ConfigOption<uint32_t>::create("numberOfBuffersToProduce", 1, "Number of buffers to produce.");
    numberOfTuplesToProducePerBuffer =
        ConfigOption<uint32_t>::create("numberOfTuplesToProducePerBuffer", 1, "Number of tuples to produce per buffer.");
    physicalStreamName =
        ConfigOption<std::string>::create("physicalStreamName", "default_physical", "Physical name of the stream.");
    std::vector<std::string> defaultLogicalStreamName {"default_logical"};
    logicalStreamName = ConfigOption<std::vector<std::string>>::create("logicalStreamName", defaultLogicalStreamName,
                                                                       "Names of the logical streams.");
    skipHeader = ConfigOption<bool>::create("skipHeader", false, "Skip first line of the file.");
}

void SourceConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesSourceConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());
        try {
            setSourceConfig(config["sourceConfig"].As<std::string>());
            setSourceType(config["sourceType"].As<std::string>());
            setSourceFrequency(config["sourceFrequency"].As<uint16_t>());
            setNumberOfBuffersToProduce(config["numberOfBuffersToProduce"].As<uint64_t>());
            setNumberOfTuplesToProducePerBuffer(config["numberOfTuplesToProducePerBuffer"].As<uint16_t>());
            setPhysicalStreamName(config["physicalStreamName"].As<std::string>());
            std::string lNameVectorString = config["logicalStreamName"].As<std::string>();
            std::vector<std::string> lNames = UtilityFunctions::splitWithStringDelimiter(lNameVectorString, ",");
            setLogicalStreamName(lNames);
            setSkipHeader(config["skipHeader"].As<bool>());
        } catch (std::exception& e) {
            NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from XAML file.");
            NES_WARNING("NesWorkerConfig: Keeping default values.");
            resetSourceOptions();
        }
        return;
    }
    NES_ERROR("NesWorkerConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("NesWorkerConfig: Keeping default values for Coordinator Config.");
}

void SourceConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--sourceType") {
                setSourceType(it->second);
            } else if (it->first == "--sourceConfig") {
                setSourceConfig(it->second);
            } else if (it->first == "--sourceFrequency") {
                setSourceFrequency(stoi(it->second));
            } else if (it->first == "--numberOfBuffersToProduce") {
                setNumberOfBuffersToProduce(stoi(it->second));
            } else if (it->first == "--numberOfTuplesToProducePerBuffer") {
                setNumberOfTuplesToProducePerBuffer(stoi(it->second));
            } else if (it->first == "--physicalStreamName") {
                setPhysicalStreamName(it->second);
            } else if (it->first == "--logicalStreamName") {
                std::string s = it->second;
                setLogicalStreamName(UtilityFunctions::splitWithStringDelimiter(s, ","));
            } else if (it->first == "--skipHeader") {
                setSkipHeader((it->second == "true"));
            } else {
                NES_WARNING("Unknown configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. " << e.what());
        NES_WARNING("NesWorkerConfig: Keeping default values.");
        resetSourceOptions();
    }
}

void SourceConfig::resetSourceOptions() {
    setSourceConfig(sourceConfig->getDefaultValue());
    setSourceType(sourceType->getDefaultValue());
    setSourceFrequency(sourceFrequency->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
    setPhysicalStreamName(physicalStreamName->getDefaultValue());
    // BDAPRO still don't know where the default values come from
    setLogicalStreamName(logicalStreamName->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
}

const StringConfigOption SourceConfig::getSourceType() const { return sourceType; }

const StringConfigOption SourceConfig::getSourceConfig() const { return sourceConfig; }

const IntConfigOption SourceConfig::getSourceFrequency() const { return sourceFrequency; }

const IntConfigOption SourceConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const IntConfigOption SourceConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

const StringConfigOption SourceConfig::getPhysicalStreamName() const { return physicalStreamName; }

const VectorStringConfigOption SourceConfig::getLogicalStreamName() const { return logicalStreamName; }

const BoolConfigOption SourceConfig::getSkipHeader() const { return skipHeader; }

void SourceConfig::setSourceType(std::string sourceTypeValue) { sourceType->setValue(sourceTypeValue); }

void SourceConfig::setSourceConfig(std::string sourceConfigValue) { sourceConfig->setValue(sourceConfigValue); }

void SourceConfig::setSourceFrequency(uint32_t sourceFrequencyValue) { sourceFrequency->setValue(sourceFrequencyValue); }

void SourceConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void SourceConfig::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void SourceConfig::setPhysicalStreamName(std::string physicalStreamNameValue) {
    physicalStreamName->setValue(physicalStreamNameValue);
}
void SourceConfig::setLogicalStreamName(std::vector<std::string> logicalStreamNameValue) {
    logicalStreamName->setValue(logicalStreamNameValue);
}

void SourceConfig::addLogicalStreamName(std::string logicalStreamNameValue) {
    logicalStreamName->getValue().push_back(logicalStreamNameValue);
}

void SourceConfig::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

}// namespace NES