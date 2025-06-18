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

#include <string>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES::LogicalFunctionProvider
{

bool contains(const std::string& logicalFunctionType);

std::optional<LogicalFunction>
tryProvide(const std::string& logicalFunctionType, const std::vector<LogicalFunction>& children, DataType dataType);

LogicalFunction provide(const std::string& logicalFunctionType, const std::vector<LogicalFunction>& children, DataType dataType);
}
