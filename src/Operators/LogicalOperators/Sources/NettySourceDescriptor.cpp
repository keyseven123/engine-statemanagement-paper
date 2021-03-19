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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/NettySourceDescriptor.hpp>
#include <utility>
namespace NES {

NettySourceDescriptor::NettySourceDescriptor(SchemaPtr schema, std::string filePath, std::string delimiter,
                                         uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                         uint64_t frequency, bool skipHeader, std::string address)
    : SourceDescriptor(std::move(schema)), filePath(std::move(filePath)), delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numBuffersToProcess(numBuffersToProcess),
      frequency(frequency), skipHeader(skipHeader),address(std::move(address)) {}

NettySourceDescriptor::NettySourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath, std::string delimiter,
                                         uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                         uint64_t frequency, bool skipHeader, std::string address)
    : SourceDescriptor(std::move(schema), std::move(streamName)), filePath(std::move(filePath)),
      delimiter(std::move(delimiter)), numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      numBuffersToProcess(numBuffersToProcess), frequency(frequency), skipHeader(skipHeader),address(std::move(address)) {}

SourceDescriptorPtr NettySourceDescriptor::create(SchemaPtr schema, std::string filePath, std::string delimiter,
                                                uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                uint64_t frequency, bool skipHeader, std::string address) {
    return std::make_shared<NettySourceDescriptor>(NettySourceDescriptor(std::move(schema), std::move(filePath), std::move(delimiter),
                                                                     numberOfTuplesToProducePerBuffer, numBuffersToProcess,
                                                                     frequency, skipHeader,std::move(address)));
}

SourceDescriptorPtr NettySourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string filePath,
                                                std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                                uint64_t numBuffersToProcess, uint64_t frequency,
                                                bool skipHeader, std::string address) {
    return std::make_shared<NettySourceDescriptor>(NettySourceDescriptor(
        std::move(schema), std::move(streamName), std::move(filePath), std::move(delimiter), numberOfTuplesToProducePerBuffer,
        numBuffersToProcess, frequency, skipHeader,std::move(address)));
}

const std::string& NettySourceDescriptor::getFilePath() const { return filePath; }

const std::string& NettySourceDescriptor::getDelimiter() const { return delimiter; }

const std::string& NettySourceDescriptor::getAddress()  const { return address; }

bool NettySourceDescriptor::getSkipHeader() const { return skipHeader; }

uint64_t NettySourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }

uint64_t NettySourceDescriptor::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint64_t NettySourceDescriptor::getFrequency() const { return frequency; }

bool NettySourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<NettySourceDescriptor>())
        return false;
    auto otherSource = other->as<NettySourceDescriptor>();
    return filePath == otherSource->getFilePath() && delimiter == otherSource->getDelimiter()
        && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency()
        && getSchema()->equals(otherSource->getSchema()) && address == otherSource->getAddress();
}

std::string NettySourceDescriptor::toString() {
    return "NettySourceDescriptor(" + filePath + "," + delimiter + ", " + std::to_string(numBuffersToProcess) + ", "
        + std::to_string(frequency) + ")";
}


}// namespace NES
