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

#include <atomic>
#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/UnpooledChunksManager.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <PipelineExecutionContext.hpp>
#include "../../nes-runtime/include/Runtime/Execution/OperatorHandler.hpp"

namespace NES::Sources
{

class MemorySource final : public Source,
                           public Memory::BufferRecycler,
                           public std::enable_shared_from_this<MemorySource>,
                           public PipelineExecutionContext
{
public:
    static constexpr std::string_view NAME = "Memory";

    explicit MemorySource(const SourceDescriptor& sourceDescriptor, const std::shared_ptr<Memory::AbstractBufferProvider>& bufferProvider);
    ~MemorySource() override = default;

    MemorySource(const MemorySource&) = delete;
    MemorySource& operator=(const MemorySource&) = delete;
    MemorySource(MemorySource&&) = delete;
    MemorySource& operator=(MemorySource&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    bool setup() override;

    void open() override { }

    void close() override { }

    void recyclePooledBuffer(Memory::detail::MemorySegment*) override { INVARIANT(false, "This method should not be called!"); }

    void recycleUnpooledBuffer(Memory::detail::MemorySegment*, const Memory::ThreadIdCopyLastChunkPtr&) override
    {
        INVARIANT(false, "This method should not be called!");
    }

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    bool emitBuffer(const Memory::TupleBuffer&, ContinuationPolicy) override;
    Memory::TupleBuffer allocateTupleBuffer() override;

    [[nodiscard]] WorkerThreadId getId() const override
    {
        INVARIANT(false, "This method should not be called!");
        return WorkerThreadId(WorkerThreadId::INVALID);
    }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override
    {
        INVARIANT(false, "This method should not be called!");
        return 0;
    }

    [[nodiscard]] std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const override { return bufferProvider; }

    [[nodiscard]] PipelineId getPipelineId() const override
    {
        INVARIANT(false, "This method should not be called!");
        return PipelineId(PipelineId::INVALID);
    }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        INVARIANT(false, "This method should not be called!");
        throw std::runtime_error("This method should not be called!");
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override
    {
        INVARIANT(false, "This method should not be called!");
    }

private:
    static constexpr auto DEFAULT_ALIGNMENT = 64;
    std::atomic<size_t> totalNumBytesRead;
    std::string filePath;
    Schema inputSchema;
    std::atomic<bool> setupFinished = false;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::map<SequenceData, Memory::TupleBuffer> storedBuffers;
    std::map<SequenceData, Memory::TupleBuffer>::const_iterator nextBufferIterator;
};

struct ConfigParametersCSVMemory
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILEPATH);
};

}
