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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_HDFSCSVSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_HDFSCSVSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical CSV source
 */
class HdfsCSVSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string namenode, uint64_t port, std::string hadoopUser,
                                      std::string filePath, std::string delimiter, uint64_t numBuffersToProcess,
                                      uint64_t numberOfTuplesToProducePerBuffer, uint64_t frequency, bool skipHeader);

    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string namenode, uint64_t port,
                                      std::string hadoopUser, std::string filePath, std::string delimiter,
                                      uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                                      bool skipHeader);

    /**
     * @brief get namenode for the HDFS
     */
    const std::string& getNamenode() const;

    /**
     * @brief get port for the HDFS
     */
    uint64_t getPort() const;

    /**
     * @brief get hadoop user name
     */
    const std::string& getHadoopUser() const;

    /**
     * @brief get file path for reading the csv file
     */
    const std::string& getFilePath() const;

    /**
     * @brief get delimiter for the csv file
     */
    const std::string& getDelimiter() const;

    /**
     * @brief Get number of buffers to process
     */
    uint64_t getNumBuffersToProcess() const;

    /**
    * @brief Get number of number of tuples within the buffer
    */
    uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief get the frequency of reading the csv file
     */
    uint64_t getFrequency() const;

    /**
     * @brief get the value of the skipHeader
     */
    bool getSkipHeader() const;

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit HdfsCSVSourceDescriptor(SchemaPtr schema, std::string namenode, uint64_t port, std::string hadoopUser,
                                  std::string filePath, std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                  uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader);

    explicit HdfsCSVSourceDescriptor(SchemaPtr schema, std::string streamName, std::string namenode, uint64_t port,
                                  std::string hadoopUser, std::string filePath, std::string delimiter,
                                  uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                                  bool skipHeader);

    std::string namenode;
    uint64_t port;
    std::string filePath;
    std::string delimiter;
    uint64_t numBuffersToProcess;
    uint64_t numberOfTuplesToProducePerBuffer;
    uint64_t frequency;
    bool skipHeader;
    std::string hadoopUser;
};

typedef std::shared_ptr<HdfsCSVSourceDescriptor> HdfsCSVSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_HDFSSOURCEDESCRIPTOR_HPP_
