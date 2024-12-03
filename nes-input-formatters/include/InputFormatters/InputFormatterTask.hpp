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

#include <cstddef>
#include <functional>
#include <ostream>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

namespace ReturnType
{
struct Error
{
    Exception ex;
};

struct Data
{
    NES::Memory::TupleBuffer buffer;
};

struct EoS
{
};
struct Stopped
{
};

using InputFormatterTaskReturnType = std::variant<Error, Data, EoS, Stopped>;
using EmitFunction = std::function<void(const OriginId, InputFormatterTaskReturnType)>;
}


/// Takes tuple buffers with raw bytes (TBRaw/TBR), parses the TBRs and writes the formatted data to formatted tuple buffers (TBFormatted/TBF)
class InputFormatterTask : public NES::Runtime::Execution::ExecutablePipelineStage
{
public:
    InputFormatterTask() = default;
    ~InputFormatterTask() override = default;

    InputFormatterTask(const InputFormatterTask&) = delete;
    InputFormatterTask& operator=(const InputFormatterTask&) = delete;
    InputFormatterTask(InputFormatterTask&&) = delete;
    InputFormatterTask& operator=(InputFormatterTask&&) = delete;

    // Todo: think about what to do with setup and stop
    // uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    // uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    ExecutionResult execute(
        Memory::TupleBuffer& inputTupleBuffer,
        Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
        Runtime::WorkerContext& workerContext) override;


    virtual void
    parseTupleBufferRaw(const NES::Memory::TupleBuffer& tbRaw, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext, Runtime::WorkerContext& workerContext,
        size_t numBytesInTBRaw)
        = 0;

    friend std::ostream& operator<<(std::ostream& out, const InputFormatterTask& InputFormatterTask) { return InputFormatterTask.toString(out); }

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};


}

namespace fmt
{
template <>
struct formatter<NES::InputFormatters::InputFormatterTask> : ostream_formatter
{
};
}
