/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
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
                     std::string const& filePath,
                     std::string const& delimiter,
                     uint64_t numberOfTuplesToProducePerBuffer,
                     uint64_t numberOfBuffersToProcess,
                     uint64_t frequency,
                     bool skipHeader,
                     OperatorId operatorId,
                     OperatorId logicalSourceOperatorId,
                     size_t numSourceLocalBuffers,
                     GatheringMode gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 logicalSourceOperatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      filePath(filePath), numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), delimiter(delimiter),
      skipHeader(skipHeader) {
    this->numBuffersToProcess = numberOfBuffersToProcess;
    this->gatheringInterval = std::chrono::milliseconds(frequency);
    this->tupleSize = schema->getSchemaSizeInBytes();

    char* path = realpath(filePath.c_str(), nullptr);
    NES_DEBUG("CSVSource: Opening path " << path);
    input.open(path);

    NES_DEBUG("CSVSource::CSVSource: read buffer");
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1) {
        NES_ERROR("CSVSource::CSVSource File " + filePath + " is corrupted");
        //        NES_ASSERT2_FMT(false, "CSVSource::CSVSource File " + filePath + " is corrupted");
    } else {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    this->loopOnFile = numberOfBuffersToProcess == 0;

    NES_DEBUG("CSVSource: tupleSize=" << this->tupleSize << " freq=" << this->gatheringInterval.count() << "ms"
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << this->numberOfTuplesToProducePerBuffer << "loopOnFile=" << this->loopOnFile);

    this->fileEnded = false;
}

std::optional<Runtime::TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called on " << operatorId);
    auto buffer = this->bufferManager->getBufferBlocking();
    fillBuffer(buffer);
    NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer;
}

std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void CSVSource::fillBuffer(Runtime::TupleBuffer& buf) {
    NES_DEBUG("CSVSource::fillBuffer: start at pos=" << currentPosInFile << " fileSize=" << fileSize);
    if (this->fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        buf.setNumberOfTuples(0);
        return;
    }
    input.seekg(currentPosInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buf.getBufferSize() / tupleSize;
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buf.getBufferSize(), "Wrong parameters");
    }
    NES_DEBUG("CSVSource::fillBuffer: fill buffer with #tuples=" << generatedTuplesThisPass << " of size=" << tupleSize);

    std::string line;
    uint64_t tupCnt = 0;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    if (skipHeader && currentPosInFile == 0) {
        NES_DEBUG("CSVSource: Skipping header");
        std::getline(input, line);
        currentPosInFile = input.tellg();
    }

    while (tupCnt < generatedTuplesThisPass) {
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
            NES_DEBUG("CSVSource::fillBuffer: reset tellg()=" << input.tellg() << " fileSize=" << fileSize);
            input.clear();
            input.seekg(0, std::ifstream::beg);
            if (!this->loopOnFile) {
                NES_DEBUG("CSVSource::fillBuffer: break because file ended");
                this->fileEnded = true;
                break;
            }
            if (this->skipHeader) {
                NES_DEBUG("CSVSource: Skipping header");
                std::getline(input, line);
                currentPosInFile = input.tellg();
            }
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line=" << tupCnt << " val=" << line);
        // TODO: there will be a problem with non-printable characters (at least with null terminators). Check sources
        std::vector<std::string> tokens;
        tokens = UtilityFunctions::splitWithStringDelimiter(line, this->delimiter);
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = physicalTypes[j];
            uint64_t fieldSize = field->size();

            NES_ASSERT2_FMT(fieldSize + offset + tupCnt * tupleSize < buf.getBufferSize(),
                            "Overflow detected: buffer size = " << buf.getBufferSize() << " position = "
                                                                << (offset + tupCnt * tupleSize) << " field size " << fieldSize);
            if (field->isBasicType()) {
                NES_ASSERT2_FMT(!tokens[j].empty(), "Field cannot be empty if basic type");
                auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);
                if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_64) {
                    uint64_t val = std::stoull(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_64) {
                    int64_t val = std::stoll(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_32) {
                    uint32_t val = static_cast<uint32_t>(std::stoul(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_32) {
                    int32_t val = static_cast<int32_t>(std::stol(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_16) {
                    uint16_t val = static_cast<uint16_t>(std::stol(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_16) {
                    int16_t val = static_cast<int16_t>(std::stol(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_8) {
                    uint8_t val = static_cast<uint8_t>(std::stoi(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_8) {
                    int8_t val = static_cast<int8_t>(std::stoi(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::DOUBLE) {
                    double val = std::stod(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::FLOAT) {
                    float val = std::stof(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::BOOLEAN) {
                    bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                }
            } else {
                memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, tokens[j].c_str(), fieldSize);
            }

            offset += fieldSize;
        }
        tupCnt++;
    }//end of while

    currentPosInFile = input.tellg();
    buf.setNumberOfTuples(tupCnt);
    generatedTuples += tupCnt;
    generatedBuffers++;
    NES_TRACE("CSVSource::fillBuffer: reading finished read " << tupCnt << " tuples at posInFile=" << currentPosInFile);
    NES_TRACE("CSVSource::fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));
}

SourceType CSVSource::getType() const { return CSV_SOURCE; }

std::string CSVSource::getFilePath() const { return filePath; }

std::string CSVSource::getDelimiter() const { return delimiter; }

uint64_t CSVSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

bool CSVSource::getSkipHeader() const { return skipHeader; }
}// namespace NES
