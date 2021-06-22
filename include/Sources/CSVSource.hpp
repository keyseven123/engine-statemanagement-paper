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

#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <Sources/DataSource.hpp>
#include <chrono>
#include <fstream>
#include <string>

namespace NES {

/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
  public:
    /**
   * @brief constructor of CSV sou1rce
   * @param schema of the source
   * @param path to the csv file
   * @param delimiter inside the file, default ","
   * @param number of buffers to create
   */
    explicit CSVSource(SchemaPtr schema,
                       NodeEngine::BufferManagerPtr bufferManager,
                       NodeEngine::QueryManagerPtr queryManager,
                       const std::string filePath,
                       const std::string delimiter,
                       uint64_t numberOfTuplesToProducePerBuffer,
                       uint64_t numBuffersToProcess,
                       uint64_t frequency,
                       bool skipHeader,
                       OperatorId operatorId,
                       OperatorId logicalSourceOperatorId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(NodeEngine::TupleBuffer&);

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief Get file path for the csv file
     */
    const std::string getFilePath() const;

    /**
     * @brief Get the csv file delimiter
     */
    const std::string getDelimiter() const;

    /**
     * @brief Get number of tuples per buffer
     */
    const uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief getter for skip header
     */
    bool getSkipHeader() const;

  private:
    std::string filePath;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::string delimiter;
    uint64_t currentPosInFile;
    bool loopOnFile;
    std::ifstream input;
    uint64_t fileSize;
    bool fileEnded;
    bool skipHeader;
};

typedef std::shared_ptr<CSVSource> CSVSourcePtr;
}// namespace NES

#endif /* INCLUDE_CSVSOURCE_H_ */
