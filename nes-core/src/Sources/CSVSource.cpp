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
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace NES {

CSVSource::CSVSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     CSVSourceTypePtr csvSourceType,
                     OperatorId operatorId,
                     size_t numSourceLocalBuffers,
                     GatheringMode::Value gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      csvSourceType(csvSourceType), filePath(csvSourceType->getFilePath()->getValue()),
      numberOfTuplesToProducePerBuffer(csvSourceType->getNumberOfTuplesToProducePerBuffer()->getValue()),
      delimiter(csvSourceType->getDelimiter()->getValue()), skipHeader(csvSourceType->getSkipHeader()->getValue()) {
    this->numBuffersToProcess = csvSourceType->getNumberOfBuffersToProduce()->getValue();
    this->gatheringInterval = std::chrono::milliseconds(csvSourceType->getGatheringInterval()->getValue());
    this->tupleSize = schema->getSchemaSizeInBytes();

    char* path = realpath(filePath.c_str(), nullptr);
    NES_DEBUG("CSVSource: Opening path " << path);
    input.open(path);

    NES_DEBUG("CSVSource::CSVSource: read buffer");
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1) {
        NES_ERROR("CSVSource::CSVSource File " + filePath + " is corrupted");
    } else {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    this->loopOnFile = csvSourceType->getNumberOfBuffersToProduce()->getValue() == 0;

    NES_DEBUG("CSVSource: tupleSize=" << this->tupleSize << " freq=" << this->gatheringInterval.count() << "ms"
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << this->numberOfTuplesToProducePerBuffer << "loopOnFile=" << this->loopOnFile);

    this->fileEnded = false;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, delimiter);
}

std::optional<Runtime::TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called on " << operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void CSVSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    uint64_t generatedTuplesThisPass = buffer.getCapacity();

    uint64_t tupleCount = 0;
    currentPositionInFile = 0;
    while (tupleCount < generatedTuplesThisPass) {
        buffer[tupleCount][0].write<uint64_t>(100);
        buffer[tupleCount][1].write<uint64_t>(100);
        buffer[tupleCount][2].write<uint64_t>(100);
        buffer[tupleCount][3].write<uint64_t>(100);
        buffer[tupleCount][4].write<uint64_t>(100);
        buffer[tupleCount][5].write<uint64_t>(100);
        buffer[tupleCount][6].write<uint64_t>(100);
        tupleCount++;
    }//end of while
    buffer.setNumberOfTuples(tupleCount);
}

SourceType CSVSource::getType() const { return CSV_SOURCE; }

std::string CSVSource::getFilePath() const { return filePath; }

std::string CSVSource::getDelimiter() const { return delimiter; }

uint64_t CSVSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

bool CSVSource::getSkipHeader() const { return skipHeader; }

const CSVSourceTypePtr& CSVSource::getSourceConfig() const { return csvSourceType; }
}// namespace NES
