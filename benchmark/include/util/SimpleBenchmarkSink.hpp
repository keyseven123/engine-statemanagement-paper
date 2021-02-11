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

#ifndef NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <future>

using namespace NES;
namespace NES::Benchmarking {
/**
 * @brief SimpleBenchmarkSink will set completed to true, after it gets @param expectedNumberOfTuples have been processed by SimpleBenchmarkSink
 * The schema must have a key field as this field is used to check if the benchmark has ended
 */
class SimpleBenchmarkSink : public SinkMedium {
  public:
    SimpleBenchmarkSink(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0) {
        rowLayout = NodeEngine::createRowLayout(schema);

        // An end of benchmark will be signaled by the source as key field will be equal to -1
        auto fields = getSchemaPtr()->fields;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i]->getName() == "key") {
                this->fieldIndex = i;
                break;
            }
        }
        promiseSet = false;
    };

    static std::shared_ptr<SimpleBenchmarkSink> create(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) {
        return std::make_shared<SimpleBenchmarkSink>(schema, bufferManager);
    }

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContext& workerContext) override {
        std::unique_lock lock(m);
        NES_DEBUG("SimpleBenchmarkSink: got buffer with " << input_buffer.getNumberOfTuples() << " number of tuples!");
        NES_INFO("WorkerContextID=" << workerContext.getId());

        currentTuples += input_buffer.getNumberOfTuples();
        bool endOfBenchmark = true;

        if (promiseSet)
            return true;

        auto fields = getSchemaPtr()->fields;
        uint64_t recordIndex = 1;
        auto dataType = fields[fieldIndex]->getDataType();
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
            if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                if (*rowLayout->getFieldPointer<char>(input_buffer, recordIndex, fieldIndex) != (char) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                if (*rowLayout->getFieldPointer<uint8_t>(input_buffer, recordIndex, fieldIndex) != (uint8_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                if (*rowLayout->getFieldPointer<uint16_t>(input_buffer, recordIndex, fieldIndex) != (uint16_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                if (*rowLayout->getFieldPointer<uint32_t>(input_buffer, recordIndex, fieldIndex) != (uint32_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                if (*rowLayout->getFieldPointer<uint64_t>(input_buffer, recordIndex, fieldIndex) != (uint64_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                if (*rowLayout->getFieldPointer<int8_t>(input_buffer, recordIndex, fieldIndex) != (int8_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                if (*rowLayout->getFieldPointer<int16_t>(input_buffer, recordIndex, fieldIndex) != (int16_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                if (*rowLayout->getFieldPointer<int32_t>(input_buffer, recordIndex, fieldIndex) != (int32_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                if (*rowLayout->getFieldPointer<int64_t>(input_buffer, recordIndex, fieldIndex) != (int64_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                if (*rowLayout->getFieldPointer<float>(input_buffer, recordIndex, fieldIndex) != (float) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                if (*rowLayout->getFieldPointer<double>(input_buffer, recordIndex, fieldIndex) != (double) -1) {
                    endOfBenchmark = false;
                }
            } else {
                NES_DEBUG("This data sink only accepts data for numeric fields");
            }
        } else {
            NES_DEBUG("This data sink only accepts data for numeric fields");
        }

        if (currentTuples % (100 * 1000 * 1000) == 0) {
            NES_WARNING("SimpleBenchmarkSink: endOfBenchmark = " << endOfBenchmark << " with " << input_buffer.getNumberOfTuples()
                                                                 << " number of tuples!");
            NES_DEBUG("SimpleBenchmarkSink: currentTuples=" << currentTuples);
        }
        if (endOfBenchmark && input_buffer.getNumberOfTuples() > 0 && !promiseSet) {
            NES_WARNING("SimpleBenchmarkSink: setting promise to true!");
            completed.set_value(endOfBenchmark);
            promiseSet = true;
        }

        return true;
    }

    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    NodeEngine::TupleBuffer& get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override { return ""; }

    void setup() override{};

    std::string toString() override { return "Test_Sink"; }

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~SimpleBenchmarkSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

  private:
    void cleanupBuffers() {
        for (auto& buffer : resultBuffers) {
            buffer.release();
        }
        resultBuffers.clear();
    }

    bool promiseSet = false;
    uint64_t fieldIndex = 0;
    uint64_t currentTuples = 0;
    std::vector<NodeEngine::TupleBuffer> resultBuffers;
    std::mutex m;
    std::shared_ptr<NodeEngine::MemoryLayout> rowLayout;

  public:
    std::promise<bool> completed;
};
}// namespace NES::Benchmarking

#endif//NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
