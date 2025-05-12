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
#pragma once

#include <istream>
#include <memory>
#include <string>
#include <vector>

#include <Plans/LogicalPlan.hpp>
#include <PlanningContext.hpp>
#include <QueryConfig.hpp>

namespace NES::CLI
{

class YAMLBinder
{
    LogicalPlan plan;
    PlanningContext ctx;
    QueryConfig queryConfig;

    explicit YAMLBinder(QueryConfig&& config);
public:
    static YAMLBinder from(QueryConfig&& config) { return YAMLBinder{std::move(config)}; }

    /// Validated and bound content of a YAML file, the members are not specific to the yaml-binder anymore but our "normal" types.
    /// If something goes wrong, for example, a source is declared twice, the binder will throw an exception.
    struct BoundLogicalPlan
    {
        LogicalPlan plan;
        PlanningContext ctx;
    };
    BoundLogicalPlan bind() &&;

private:
    void bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources);
    void bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources);
    void bindValidateSink(const Sink& unboundSink);
};

class YAMLLoader
{
    static QueryConfig loadFromFile(const std::string& filePath);
    static QueryConfig loadFromStream(std::istream& stream);

public:
    static QueryConfig load(const std::string& inputArgument);
};

}
