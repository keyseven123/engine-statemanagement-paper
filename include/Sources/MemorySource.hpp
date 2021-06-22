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

#ifndef NES_INCLUDE_SOURCES_MEMORYSOURCE_HPP_
#define NES_INCLUDE_SOURCES_MEMORYSOURCE_HPP_

#include <Sources/GeneratorSource.hpp>

namespace NES {

/**
 * @brief Memory Source that reads from main memory and produces buffers.
 * The memory area out of which buffers will be produced must be initialized beforehand and allocated as a shared_ptr
 * that must have ownership of the area, i.e., it must control when to free it.
 * Do not use in distributed settings but only for single node dev and testing.
 */
class MemorySource : public GeneratorSource {
  public:
    /**
     * @brief The constructor of a MemorySource
     * @param schema the schema of the source
     * @param memoryArea the non-null memory area that stores the data that will be used by the source
     * @param memoryAreaSize the non-zero size of the memory area
     * @param bufferManager valid pointer to the buffer manager
     * @param queryManager valid pointer to the query manager
     * @param
     * @param operatorId the valid id of the source
     */
    explicit MemorySource(SchemaPtr schema,
                          std::shared_ptr<uint8_t> memoryArea,
                          size_t memoryAreaSize,
                          NodeEngine::BufferManagerPtr bufferManager,
                          NodeEngine::QueryManagerPtr queryManager,
                          uint64_t numBuffersToProcess,
                          uint64_t gatheringValue,
                          OperatorId operatorId,
                          OperatorId logicalSourceOperatorId,
                          size_t numSourceLocalBuffers,
                          GatheringMode gatheringMode,
                          std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors);
    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     * @brief Provides a string representation of the source
     * @return The string representation of the source
     */
    const std::string toString() const override;

    /**
     * @brief Provides the type of the source
     * @return the type of the source
     */
    SourceType getType() const override;

  private:
    uint64_t numberOfTuplesToProduce;
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
    uint64_t currentPositionInBytes;
};

typedef std::shared_ptr<MemorySource> MemorySourcePtr;

}// namespace NES

#endif//NES_INCLUDE_SOURCES_MEMORYSOURCE_HPP_
