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

#include <filesystem>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <LogicalInferModelNameOperator.hpp>

namespace NES::InferModel
{


std::string LogicalInferModelNameOperator::explain(ExplainVerbosity) const
{
    return fmt::format("INFER_MODEL(opId: {}, modelName: {})", id, modelName);
}
}
