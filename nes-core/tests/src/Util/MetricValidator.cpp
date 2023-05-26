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

#include <Util/MetricValidator.hpp>

namespace NES {
bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::MetricPtr metric) {
    if (metric->getMetricType() == Monitoring::MetricType::DiskMetric) {
        return isValid(reader, metric->getValue<Monitoring::DiskMetrics>());
    } else if (metric->getMetricType() == Monitoring::MetricType::MemoryMetric) {
        return isValid(reader, metric->getValue<Monitoring::MemoryMetrics>());
    } else if (metric->getMetricType() == Monitoring::MetricType::RegistrationMetric) {
        return isValid(reader, metric->getValue<Monitoring::RegistrationMetrics>());
    } else if (metric->getMetricType() == Monitoring::MetricType::WrappedCpuMetrics) {
        return isValid(reader, metric->getValue<Monitoring::CpuMetricsWrapper>());
    } else if (metric->getMetricType() == Monitoring::MetricType::WrappedNetworkMetrics) {
        return isValid(reader, metric->getValue<Monitoring::NetworkMetricsWrapper>());
    } else {
        return false;
    }
};

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::RuntimeMetrics metrics) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for RuntimeMetrics. Returning true");
        return true;
    }

    if (metrics.wallTimeNs <= 0) {
        NES_ERROR2("MetricValidator: Wrong wallTimeNs.");
        check = false;
    }
    if (!(metrics.blkioBytesWritten >= 0)) {
        NES_ERROR2("MetricValidator: Wrong blkioBytesWritten.");
        check = false;
    }
    if (!(metrics.blkioBytesRead >= 0)) {
        NES_ERROR2("MetricValidator: Wrong blkioBytesRead.");
        check = false;
    }
    if (!(metrics.memoryUsageInBytes >= 0)) {
        NES_ERROR2("MetricValidator: Wrong memoryUsageInBytes.");
        check = false;
    }
    if (!(metrics.cpuLoadInJiffies >= 0)) {
        NES_ERROR2("MetricValidator: Wrong cpuLoadInJiffies.");
        check = false;
    }
    if (!(metrics.longCoord >= 0)) {
        NES_ERROR2("MetricValidator: Wrong longCoord.");
        check = false;
    }
    if (!(metrics.latCoord >= 0)) {
        NES_ERROR2("MetricValidator: Wrong latCoord.");
        check = false;
    }
    if (!(metrics.batteryStatusInPercent >= 0)) {
        NES_ERROR2("MetricValidator: Wrong batteryStatusInPercent.");
        check = false;
    }
    return check;
};

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::RegistrationMetrics metrics) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for NodeRegistrationMetrics. Returning true");
        return true;
    }

    if (!(metrics.cpuQuotaUS >= -1)) {
        NES_ERROR2("MetricValidator: Wrong cpuQuotaUS.");
        check = false;
    }
    if (!(metrics.cpuPeriodUS >= -1)) {
        NES_ERROR2("MetricValidator: Wrong cpuPeriodUS.");
        check = false;
    }
    if (!(metrics.totalCPUJiffies > 0)) {
        NES_ERROR2("MetricValidator: Wrong totalCPUJiffies.");
        check = false;
    }
    if (!(metrics.cpuCoreNum > 0)) {
        NES_ERROR2("MetricValidator: Wrong cpuCoreNum.");
        check = false;
    }
    if (!(metrics.totalMemoryBytes >= 0)) {
        NES_ERROR2("MetricValidator: Wrong totalMemoryBytes.");
        check = false;
    }
    if (metrics.hasBattery) {
        NES_ERROR2("MetricValidator: Wrong hasBattery.");
        check = false;
    }
    if (metrics.isMoving) {
        NES_ERROR2("MetricValidator: Wrong isMoving.");
        check = false;
    }
    return check;
};

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::CpuMetricsWrapper cpuMetrics) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for CpuMetricsWrapper. Returning true");
        return true;
    }

    if (cpuMetrics.size() <= 0) {
        NES_ERROR2("MetricValidator: Wrong getNumCores.");
        check = false;
    }
    for (uint64_t i = 0; i < cpuMetrics.size(); i++) {
        if (!(cpuMetrics.getValue(i).user > 0)) {
            NES_ERROR2("MetricValidator: Wrong cpuMetrics.getValue(i).user.");
            check = false;
        }
    }
    if (!(cpuMetrics.getTotal().user > 0)) {
        NES_ERROR2("MetricValidator: Wrong cpuMetrics.getTotal().user.");
        check = false;
    }
    return check;
};

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader,
                              Monitoring::NetworkMetricsWrapper networkMetrics) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for NetworkMetricsWrapper. Returning true");
        return true;
    }

    if (networkMetrics.getInterfaceNames().empty()) {
        NES_ERROR2("MetricValidator: Wrong getInterfaceNames().");
        check = false;
    }
    return check;
};

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::MemoryMetrics memoryMetrics) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for MemoryMetrics. Returning true");
        return true;
    }

    if (!(memoryMetrics.FREE_RAM > 0)) {
        NES_ERROR2("MetricValidator: Wrong FREE_RAM.");
        check = false;
    }
    return check;
};

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::DiskMetrics diskMetrics) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for DiskMetrics. Returning true");
        return true;
    }

    if (!(diskMetrics.fBavail >= 0)) {
        NES_ERROR2("MetricValidator: Wrong fBavail.");
        check = false;
    }
    return check;
}

bool MetricValidator::isValid(Monitoring::AbstractSystemResourcesReaderPtr reader,
                              Monitoring::StoredNodeMetricsPtr storedMetrics,
                              Monitoring::MetricType expectedType,
                              TopologyNodeId expectedNodeId,
                              uint64_t expectedSize) {
    bool check = true;

    if (!storedMetrics->contains(expectedType)) {
        NES_ERROR2("MetricValidator: Metrics for node {} are missing type {}", expectedNodeId, toString(expectedType));
        return false;
    }

    auto metricVec = storedMetrics->at(expectedType);
    Monitoring::TimestampMetricPtr pairedNetworkMetric = metricVec->at(0);
    Monitoring::MetricPtr retMetric = pairedNetworkMetric->second;

    NES_INFO2("MetricValidator: Stored metrics for ID {}: {}", expectedNodeId, Monitoring::MetricUtils::toJson(storedMetrics));
    if (retMetric->getMetricType() != expectedType) {
        NES_ERROR2("MetricValidator: MetricType is not as expected {} != {}",
                   toString(retMetric->getMetricType()),
                   toString(expectedType));
        check = false;
    }

    if (!MetricValidator::isValid(reader, retMetric)) {
        check = false;
    }

    if (!MetricValidator::checkNodeIds(retMetric, expectedNodeId)) {
        check = false;
    }

    if (storedMetrics->size() != expectedSize) {
        NES_ERROR2("MetricValidator: Stored metrics do not match expected size {} != {}", storedMetrics->size(), expectedSize);
        check = false;
    }
    return check;
}

bool MetricValidator::isValidAll(Monitoring::AbstractSystemResourcesReaderPtr reader, nlohmann::json json) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used. Returning true");
        return true;
    }

    if (!json.contains(std::string(magic_enum::enum_name(Monitoring::MetricType::RegistrationMetric)))) {
        NES_ERROR2("MetricValidator: Missing field registration");
        check = false;
    } else {
        check = isValidRegistrationMetrics(reader,
                                           json[std::string(magic_enum::enum_name(Monitoring::MetricType::RegistrationMetric))]);
    }

    if (!json.contains(std::string(magic_enum::enum_name(Monitoring::MetricType::DiskMetric)))) {
        NES_ERROR2("MetricValidator: Missing field disk");
        check = false;
    } else {
        if (!(json[std::string(magic_enum::enum_name(Monitoring::MetricType::DiskMetric))].size() == 6U)) {
            NES_ERROR2("MetricValidator: Values for disk missing");
            check = false;
        }
    }

    if (!json.contains(std::string(magic_enum::enum_name(Monitoring::MetricType::WrappedCpuMetrics)))) {
        NES_ERROR2("MetricValidator: Missing field wrapped cpu");
        check = false;
    } else {
        auto numCpuFields = json[std::string(magic_enum::enum_name(Monitoring::MetricType::WrappedCpuMetrics))].size();
        if (numCpuFields <= 1) {
            NES_ERROR2("MetricValidator: Values for wrapped_cpu missing");
            check = false;
        }
    }

    if (!json.contains(std::string(magic_enum::enum_name(Monitoring::MetricType::WrappedNetworkMetrics)))) {
        NES_ERROR2("MetricValidator: Missing field wrapped network");
        check = false;
    } else {
        auto numFields = json[std::string(magic_enum::enum_name(Monitoring::MetricType::WrappedNetworkMetrics))].size();
        if (numFields < 1) {
            NES_ERROR2("MetricValidator: Values for wrapped_network missing");
            check = false;
        }
    }

    if (!json.contains(std::string(magic_enum::enum_name(Monitoring::MetricType::MemoryMetric)))) {
        NES_ERROR2("MetricValidator: Missing field memory");
        check = false;
    } else {
        auto numFields = json[std::string(magic_enum::enum_name(Monitoring::MetricType::MemoryMetric))].size();
        if (numFields < 13) {
            NES_ERROR2("MetricValidator: Values for wrapped_network missing");
            check = false;
        }
    }
    return check;
}

bool MetricValidator::isValidAllStorage(Monitoring::AbstractSystemResourcesReaderPtr reader, nlohmann::json json) {
    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used. Returning true");
        return true;
    }

    bool check = true;
    if (!json.contains("registration")) {
        NES_ERROR2("MetricValidator: Missing field registration");
        check = false;
    } else {
        check = isValidRegistrationMetrics(reader, json["registration"][0]["value"]);
    }

    if (!json.contains("disk")) {
        NES_ERROR2("MetricValidator: Missing field disk");
        check = false;
    } else {
        if (!(json["disk"][0]["value"].size() == 6U)) {
            NES_ERROR2("MetricValidator: Values for disk missing");
            check = false;
        }
    }

    if (!json.contains("wrapped_cpu")) {
        NES_ERROR2("MetricValidator: Missing field wrapped cpu");
        check = false;
    } else {
        auto numCpuFields = json["wrapped_cpu"][0]["value"].size();
        if (numCpuFields <= 1) {
            NES_ERROR2("MetricValidator: Values for wrapped_cpu missing");
            check = false;
        }
    }

    if (!json.contains("wrapped_network")) {
        NES_ERROR2("MetricValidator: Missing field wrapped network");
        check = false;
    } else {
        auto numFields = json["wrapped_network"][0]["value"].size();
        if (numFields < 1) {
            NES_ERROR2("MetricValidator: Values for wrapped_network missing");
            check = false;
        }
    }

    if (!json.contains("memory")) {
        NES_ERROR2("MetricValidator: Missing field memory");
        check = false;
    } else {
        auto numFields = json["memory"][0]["value"].size();
        if (numFields < 13) {
            NES_ERROR2("MetricValidator: Values for wrapped_network missing");
            check = false;
        }
    }
    return check;
}

bool MetricValidator::isValidRegistrationMetrics(Monitoring::AbstractSystemResourcesReaderPtr reader, nlohmann::json json) {
    bool check = true;

    if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
        NES_WARNING2("MetricValidator: AbstractReader used for DiskMetrics.");
        auto numFields = json.size();
        if (numFields != Monitoring::RegistrationMetrics::getSchema("")->getSize()) {
            NES_ERROR2("MetricValidator: Entries for registration metrics missing");
            return false;
        }
        return true;
    }

    if (!(json.contains("CpuCoreNum"))) {
        NES_ERROR2("MetricValidator: Wrong CpuCoreNum.");
        check = false;
    }

    if (!(json.contains("CpuPeriodUS"))) {
        NES_ERROR2("MetricValidator: Wrong CpuPeriodUS.");
        check = false;
    }

    if (!(json.contains("CpuQuotaUS"))) {
        NES_ERROR2("MetricValidator: Wrong CpuQuotaUS.");
        check = false;
    }

    if (!(json.contains("HasBattery"))) {
        NES_ERROR2("MetricValidator: Wrong HasBattery.");
        check = false;
    }

    if (!(json.contains("IsMoving"))) {
        NES_ERROR2("MetricValidator: Wrong IsMoving.");
        check = false;
    }

    if (!(json.contains("TotalCPUJiffies"))) {
        NES_ERROR2("MetricValidator: Wrong TotalCPUJiffies.");
        check = false;
    }

    if (!(json.contains("TotalMemory"))) {
        NES_ERROR2("MetricValidator: Wrong TotalMemory.");
        check = false;
    }

    return check;
}

bool MetricValidator::checkNodeIds(nlohmann::json json, uint64_t nodeId) {
    bool check = true;
    for (auto& [key, val] : json.items()) {
        if (json[key].contains("NODE_ID") && json[key]["NODE_ID"] != nodeId) {
            NES_ERROR2("MetricValidator: Wrong node ID for {} where {} != {}", key, json[key]["NODE_ID"], nodeId);
            check = false;
        }
    }
    return check;
}

bool MetricValidator::checkNodeIdsStorage(nlohmann::json json, uint64_t nodeId) {
    bool check = true;
    for (auto& [key, val] : json.items()) {
        // This change lets you get the string straight up from "first"
        auto jsonMetric = json[key][0]["value"];
        if (!checkNodeIds(jsonMetric, nodeId)) {
            check = false;
        }
    }
    return check;
}

bool MetricValidator::MetricValidator::checkEntriesOfStream(std::set<std::string> setOfStr, nlohmann::json jsons) {
    bool check = false;
    for (auto elem : setOfStr) {
        check = false;
        for (int i = 0; i < static_cast<int>(jsons.size()); i++) {
            auto json = jsons[i];
            NES_DEBUG2("Json Values of {}. entry: {}", std::to_string(i), json.dump());
            for (auto& [key, val] : json.items()) {
                if (key == "logical_stream" && val == elem) {
                    check = true;
                    continue;
                }
            }
        }
        if (check == false)
            return check;
    }
    return check;
}

bool MetricValidator::checkNodeIds(Monitoring::MetricPtr metric, uint64_t nodeId) {
    if (metric->getMetricType() == Monitoring::MetricType::DiskMetric) {
        auto parsedMetrics = metric->getValue<Monitoring::DiskMetrics>();
        return parsedMetrics.nodeId == nodeId;
    } else if (metric->getMetricType() == Monitoring::MetricType::MemoryMetric) {
        auto parsedMetrics = metric->getValue<Monitoring::MemoryMetrics>();
        return parsedMetrics.nodeId == nodeId;
    } else if (metric->getMetricType() == Monitoring::MetricType::RegistrationMetric) {
        auto parsedMetrics = metric->getValue<Monitoring::RegistrationMetrics>();
        return parsedMetrics.nodeId == nodeId;
    } else if (metric->getMetricType() == Monitoring::MetricType::WrappedCpuMetrics) {
        auto parsedMetrics = metric->getValue<Monitoring::CpuMetricsWrapper>();
        for (uint64_t i = 0; i < parsedMetrics.size(); i++) {
            if (parsedMetrics.getValue(i).nodeId != nodeId) {
                return false;
            }
        }
        return parsedMetrics.getNodeId() == nodeId;
    } else if (metric->getMetricType() == Monitoring::MetricType::WrappedNetworkMetrics) {
        auto parsedMetrics = metric->getValue<Monitoring::NetworkMetricsWrapper>();
        for (uint64_t i = 0; i < parsedMetrics.size(); i++) {
            if (parsedMetrics.getNetworkValue(i).nodeId != nodeId) {
                return false;
            }
        }
        return parsedMetrics.getNodeId() == nodeId;
    } else {
        return false;
    }
}

bool MetricValidator::waitForMonitoringStreamsOrTimeout(std::set<std::string> monitoringStreams,
                                                        uint16_t maxTimeout,
                                                        uint64_t restPort) {
    for (int i = 0; i < maxTimeout; i++) {
        auto json = TestUtils::makeMonitoringRestCall("storage", std::to_string(restPort));
        NES_INFO2("MetricValidator: Testing metric call {}", json);
        bool missing = false;
        for (uint64_t nodeId = 1; nodeId <= json.size(); nodeId++) {
            for (auto stream : monitoringStreams) {
                if (!json[std::to_string(nodeId)].contains(stream)) {
                    NES_ERROR2("MetricValidator: Missing metric {} for node {} ", stream, nodeId);
                    missing = true;
                    break;
                }
            }
            if (missing) {
                break;
            }
        }

        if (missing) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
        } else {
            NES_INFO2("MetricValidator: All metrics available");
            return true;
        }
    }
    NES_ERROR2("MetricValidator: Timeout passed");
    return false;
}

}// namespace NES