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
#include <sstream>
#include <utility>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalShuffleBufferOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalShuffleBufferOperator::PhysicalShuffleBufferOperator(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const float& degreeOfDisorder,
    const std::chrono::milliseconds& minDelay,
    const std::chrono::milliseconds& maxDelay)
    : Operator(id)
    , PhysicalUnaryOperator(id, inputSchema, inputSchema)
    , degreeOfDisorder(degreeOfDisorder)
    , minDelay(minDelay)
    , maxDelay(maxDelay)
{
}

std::shared_ptr<PhysicalOperator> PhysicalShuffleBufferOperator::create(
    std::shared_ptr<Schema> inputSchema,
    const float& degreeOfDisorder,
    const std::chrono::milliseconds& minDelay,
    const std::chrono::milliseconds& maxDelay)
{
    return create(getNextOperatorId(), std::move(inputSchema), degreeOfDisorder, minDelay, maxDelay);
}
std::shared_ptr<PhysicalOperator> PhysicalShuffleBufferOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const float& degreeOfDisorder,
    const std::chrono::milliseconds& minDelay,
    const std::chrono::milliseconds& maxDelay)
{
    return std::make_shared<PhysicalShuffleBufferOperator>(id, inputSchema, degreeOfDisorder, minDelay, maxDelay);
}

float PhysicalShuffleBufferOperator::getUnorderedness() const
{
    return degreeOfDisorder;
}

std::chrono::milliseconds PhysicalShuffleBufferOperator::getMinDelay() const
{
    return minDelay;
}

std::chrono::milliseconds PhysicalShuffleBufferOperator::getMaxDelay() const
{
    return maxDelay;
}

std::shared_ptr<Operator> PhysicalShuffleBufferOperator::copy()
{
    auto result = create(id, inputSchema, degreeOfDisorder, minDelay, maxDelay);
    result->addAllProperties(properties);
    return result;
}

}
