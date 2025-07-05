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

#include <InputFormatters/InputFormatter.hpp>

namespace NES::InputFormatters
{

/// This is a no op input formatter that performs no formatting and simply forwards the provided buffers
class NoOpInputFormatter final : public InputFormatter
{
public:
    ~NoOpInputFormatter() override = default;
    void parseTupleBufferRaw(
        const NES::Memory::TupleBuffer& rawTB,
        NES::PipelineExecutionContext& pipelineExecutionContext,
        size_t numBytesInRawTB,
        SequenceShredder& sequenceShredder) override;
    void flushFinalTuple(
        OriginId originId, NES::PipelineExecutionContext& pipelineExecutionContext, SequenceShredder& sequenceShredder) override;
    size_t getSizeOfTupleDelimiter() override;
    size_t getSizeOfFieldDelimiter() override;

protected:
    std::ostream& toString(std::ostream& os) const override;
};

}
