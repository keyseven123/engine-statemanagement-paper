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

#include <Monitoring/Metrics/MetricGroup.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>

namespace NES {

MetricGroup::MetricGroup() { NES_INFO("MetricGroup: Ctor called"); }

std::shared_ptr<MetricGroup> MetricGroup::create() { return std::make_shared<MetricGroup>(MetricGroup()); }

bool MetricGroup::add(const std::string& desc, const Metric& metric) {
    auto out = metricMap.insert(std::make_pair(desc, metric)).second;
    return out;
}

bool MetricGroup::remove(const std::string& name) { return metricMap.erase(name); }

void MetricGroup::getSample(std::shared_ptr<Schema> schema, NodeEngine::TupleBuffer& buf) {
    NES_DEBUG("MetricGroup: Collecting sample via serialize(..)");
    for (auto const& x : metricMap) {
        serialize(x.second, schema, buf, x.first);
        NES_DEBUG("MetricGroup: Serialized " + x.first + ". New schema for buffer " + schema->toString());
    }
}

SchemaPtr MetricGroup::createGroupSchema() {
    auto outputSchema = Schema::create();
    for (auto const& x : metricMap) {
        outputSchema->copyFields(getSchema(x.second, x.first));
    }
    return outputSchema;
}

}// namespace NES