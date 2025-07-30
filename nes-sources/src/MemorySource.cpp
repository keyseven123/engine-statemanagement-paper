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

#include <MemorySource.hpp>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <CSVInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include "Runtime/BufferManager.hpp"


namespace NES::Sources
{

MemorySource::MemorySource(const SourceDescriptor& sourceDescriptor, const std::shared_ptr<Memory::AbstractBufferProvider>& bufferProvider)
    : totalNumBytesRead(0)
    , filePath(sourceDescriptor.getFromConfig(ConfigParametersCSVMemory::FILEPATH))
    , inputSchema(*sourceDescriptor.getLogicalSource().getSchema())
    , bufferProvider(bufferProvider)
{
}

bool MemorySource::setup()
{
    static constexpr std::string tupleDelimiter = "\n";
    static constexpr std::string fieldDelimiter = ",";
    InputFormatters::CSVInputFormatter csvInputFormatter(inputSchema, tupleDelimiter, fieldDelimiter);
    InputFormatters::SequenceShredder sequenceShredder(tupleDelimiter.size());

    /// Reading the file in bufferSizeInBytes large chunks
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(filePath.c_str(), nullptr), std::free};
    auto inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", filePath.c_str(), std::strerror(errno));
    }

    uint64_t numberOfBytesRead = 0;
    uint64_t totalBytesRead = 0;
    uint64_t lastTotalBytesReadMessage = 0;
    const auto fileSize = std::filesystem::file_size(filePath);
    const auto percentageDifferenceForMessage = fileSize * 0.05;
    size_t sequenceNumberGenerator = SequenceNumber::INITIAL;

    do
    {
        /// We do not need to set a buffer recycler as the getUnpooledBuffer() call provides a custom one by itself
        auto tupleBufferRaw = bufferProvider->getBufferBlocking();
        inputFile.read(tupleBufferRaw.getBuffer<char>(), static_cast<std::streamsize>(tupleBufferRaw.getBufferSize()));
        numberOfBytesRead = inputFile.gcount();
        tupleBufferRaw.setUsedMemorySize(numberOfBytesRead);
        tupleBufferRaw.setSequenceNumber(SequenceNumber(sequenceNumberGenerator++));
        totalBytesRead += numberOfBytesRead;
        csvInputFormatter.parseTupleBufferRaw(tupleBufferRaw, *this, numberOfBytesRead, sequenceShredder);
        if (totalBytesRead - lastTotalBytesReadMessage > percentageDifferenceForMessage)
        {
            lastTotalBytesReadMessage = totalBytesRead;
            const auto readInPercentage = (totalBytesRead / static_cast<double>(fileSize)) * 100;
            std::cout << fmt::format("\rTotal bytes read: {} of {} --> {:.2f}%\n", totalBytesRead, fileSize, readInPercentage);
        }
    } while (numberOfBytesRead != 0);

    /// Invalid originId should be fine, as the origin id gets overwrittern once the source emits the tuple buffer to the first operator pipeline
    csvInputFormatter.flushFinalTuple(INVALID<OriginId>, *this, sequenceShredder);

    nextBufferIterator = storedBuffers.begin();
    return true;
}

bool MemorySource::emitBuffer(const Memory::TupleBuffer& buffer, ContinuationPolicy)
{
    storedBuffers[SequenceData{buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()}] = buffer;
    return true;
}

Memory::TupleBuffer MemorySource::allocateTupleBuffer()
{
    return bufferProvider->getBufferBlocking();
}

size_t MemorySource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (nextBufferIterator == storedBuffers.end())
    {
        return 0;
    }

    auto& nextBuffer = nextBufferIterator->second;
    tupleBuffer = std::move(nextBuffer);
    totalNumBytesRead += tupleBuffer.getBufferSize();
    ++nextBufferIterator;
    return tupleBuffer.getBufferSize();
}

NES::Configurations::DescriptorConfig::Config MemorySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSVMemory>(std::move(config), NAME);
}

std::ostream& MemorySource::toString(std::ostream& str) const
{
    str << std::format("\nMemorySource(totalNumBytesRead: {})", this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MemorySource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MemorySource>(sourceRegistryArguments.sourceDescriptor, sourceRegistryArguments.bufferProvider);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMemoryFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    /// Check that the test data dir is defined and that the 'filePath' parameter is set
    /// Replace the 'TESTDATA' placeholder in the filepath
    if (const auto attachSourceFilePath = systestAdaptorArguments.attachSource.fileDataPath)
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = attachSourceFilePath.value();
            return systestAdaptorArguments.physicalSourceConfig;
        }
        throw InvalidConfigParameter("A MemorySource config must contain filePath parameter.");
    }
    throw InvalidConfigParameter("An attach source of type FileData must contain a filePath configuration.");
}


}
