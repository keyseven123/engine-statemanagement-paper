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
#include <NodeEngine/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include <sstream>
#include <string>
#include <vector>

namespace NES {

CSVSource::CSVSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                     const std::string filePath, const std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                     uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), filePath(filePath), delimiter(delimiter),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), currentPosInFile(0), skipHeader(skipHeader) {
    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = std::chrono::milliseconds(frequency);
    tupleSize = schema->getSchemaSizeInBytes();

    char* path = realpath(filePath.c_str(), NULL);
    NES_DEBUG("CSVSource: Opening path " << path);
    input.open(path);

    NES_DEBUG("CSVSource::CSVSource: read buffer");
    input.seekg(0, input.end);
    fileSize = input.tellg();
    if (fileSize == -1) {
        NES_ERROR("CSVSource::CSVSource File " + filePath + " is corrupted");
    }

    if (numBuffersToProcess != 0) {
        loopOnFile = true;
    } else {
        loopOnFile = false;
    }

    NES_DEBUG("CSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval.count() << "ms"
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << numberOfTuplesToProducePerBuffer << "loopOnFile=" << loopOnFile);

    fileEnded = false;
}

std::optional<NodeEngine::TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called on " << operatorId);
    auto buffer = this->bufferManager->getBufferBlocking();
    fillBuffer(buffer);
    NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    generatedTuples += buffer.getNumberOfTuples();
    generatedBuffers++;

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    } else {
        return buffer;
    }
}

const std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void CSVSource::fillBuffer(NodeEngine::TupleBuffer& buf) {
    NES_DEBUG("CSVSource::fillBuffer: start at pos=" << currentPosInFile << " fileSize=" << fileSize);
    if (fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        buf.setNumberOfTuples(0);
        return;
    }
    input.seekg(currentPosInFile, input.beg);

    uint64_t generated_tuples_this_pass;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generated_tuples_this_pass = buf.getBufferSize() / tupleSize;
    } else {
        generated_tuples_this_pass = numberOfTuplesToProducePerBuffer;
    }
    NES_DEBUG("CSVSource::fillBuffer: fill buffer with #tuples=" << generated_tuples_this_pass << " of size=" << tupleSize);
    NES_ASSERT(generated_tuples_this_pass * tupleSize < buf.getBufferSize(),
               "Error in CSV reading, the required amount of tuples do not fit into one buffer");

    std::string line;
    uint64_t tupCnt = 0;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (auto field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    if (skipHeader && currentPosInFile == 0) {
        NES_DEBUG("CSVSource: Skipping header");
        std::getline(input, line);
        currentPosInFile = input.tellg();
    }

    while (tupCnt < generated_tuples_this_pass && this->isRunning()) {
        if (input.tellg() >= fileSize || input.tellg() == -1) {
            NES_DEBUG("CSVSource::fillBuffer: reset tellg()=" << input.tellg() << " file_size=" << fileSize);
            input.clear();
            input.seekg(0, input.beg);
            if (!loopOnFile) {
                NES_DEBUG("CSVSource::fillBuffer: break because file ended");
                fileEnded = true;
                break;
            }
            if (skipHeader) {
                NES_DEBUG("CSVSource: Skipping header");
                std::getline(input, line);
                currentPosInFile = input.tellg();
            }
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line=" << tupCnt << " val=" << line);
        std::vector<std::string> tokens;
        boost::algorithm::split(tokens, line, boost::is_any_of(this->delimiter));
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = physicalTypes[j];
            uint64_t fieldSize = field->size();

            if (field->isBasicType()) {
                auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);
                /*
             * TODO: this requires proper MIN / MAX size checks, numeric_limits<T>-like
             * TODO: this requires underflow/overflow checks
             * TODO: our types need their own sto/strto methods
             */
                NES_ASSERT2_FMT(fieldSize + offset + tupCnt * tupleSize < buf.getBufferSize(),
                                "Overflow detected: buffer size = " << buf.getBufferSize()
                                                                    << " position = " << (offset + tupCnt * tupleSize)
                                                                    << " field size " << fieldSize);
                if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_64) {
                    uint64_t val = std::stoull(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64) {
                    int64_t val = std::stoll(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32) {
                    uint32_t val = std::stoul(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_32) {
                    int32_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                    uint16_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_16) {
                    int16_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                    uint8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_8) {
                    int8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_8) {
                    int8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::DOUBLE) {
                    double val = std::stod(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::FLOAT) {
                    float val = std::stof(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::BOOLEAN) {
                    bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                }
            } else {
                memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, tokens[j].c_str(), fieldSize);
            }

            offset += fieldSize;
        }
        tupCnt++;
    }//end of while

    currentPosInFile = input.tellg();
    buf.setNumberOfTuples(tupCnt);
    NES_TRACE("CSVSource::fillBuffer: reading finished read " << tupCnt << " tuples at posInFile=" << currentPosInFile);
    NES_TRACE("CSVSource::fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));

    //update statistics
}

SourceType CSVSource::getType() const { return CSV_SOURCE; }

const std::string CSVSource::getFilePath() const { return filePath; }

const std::string CSVSource::getDelimiter() const { return delimiter; }

const uint64_t CSVSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

bool CSVSource::getSkipHeader() const { return skipHeader; }
}// namespace NES
