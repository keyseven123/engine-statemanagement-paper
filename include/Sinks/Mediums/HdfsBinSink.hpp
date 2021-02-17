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

#ifndef HdfsBinSink_HPP
#define HdfsBinSink_HPP

#include <Sinks/Mediums/SinkMedium.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <HDFS/hdfs.h>

namespace NES {

/**
 * @brief this class implements the File sing
 */
class HdfsBinSink : public SinkMedium {
  public:
    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param format in which the data is written
     * @param filePath location of file on sink server
     * @param modus of writting (overwrite or append)
     */
    explicit HdfsBinSink(SinkFormatPtr format, char* filePath, bool append, QuerySubPlanId parentPlanId);

    /**
     * @brief dtor
     */
    ~HdfsBinSink();

    /**
     * @brief method to override virtual setup function
     * @Note currently the method does nothing
     */
    void setup() override;

    /**
     * @brief method to override virtual shutdown function
     * @Note currently the method does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write a TupleBuffer
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef);

    /**
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
    const std::string toString() const override;

    /**
     * @brief get file path
     */
    const char *getFilePath() const;

    /**
     * @brief Get sink type
     */
    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  protected:
    char *filePath;
    hdfsFile outputFile;
    hdfsFS fs;
};
typedef std::shared_ptr<HdfsBinSink> HdfsBinSinkPtr;
}// namespace NES

#endif// HdfsBinSink_HPP
