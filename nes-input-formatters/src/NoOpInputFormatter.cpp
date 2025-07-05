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

#include <InputFormatterRegistry.hpp>
#include <NoOpInputFormatter.hpp>

namespace NES::InputFormatters
{

void NoOpInputFormatter::parseTupleBufferRaw(
    const NES::Memory::TupleBuffer& rawTB, NES::PipelineExecutionContext& pipelineExecutionContext, size_t, SequenceShredder&)
{
    pipelineExecutionContext.emitBuffer(rawTB);
}
void NoOpInputFormatter::flushFinalTuple(OriginId, NES::PipelineExecutionContext&, SequenceShredder&)
{
}
size_t NoOpInputFormatter::getSizeOfTupleDelimiter()
{
    return 0;
}
size_t NoOpInputFormatter::getSizeOfFieldDelimiter()
{
    return 0;
}
std::ostream& NoOpInputFormatter::toString(std::ostream& os) const
{
    return os << "NoOpInputFormatter";
}

InputFormatterRegistryReturnType
InputFormatterGeneratedRegistrar::RegisterNoOpInputFormatter(InputFormatterRegistryArguments)
{
    return std::make_unique<NoOpInputFormatter>();
}

}
