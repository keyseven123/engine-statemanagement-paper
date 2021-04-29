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

#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sources/AdaptiveSource.hpp>

#include <boost/algorithm/string.hpp>
#include <gtest/gtest.h>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <sstream>
#include <string>

namespace NES {
using NodeEngine::TupleBuffer;
class AdaptiveSourceTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("AdaptiveSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AdaptiveSourceTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down AdaptiveSourceTest test class."); }

    void SetUp() override { NES_INFO("Setup AdaptiveSourceTest class."); }

    void TearDown() override { NES_INFO("Tear down AdaptiveSourceTest test case."); }
};

struct __attribute__((packed)) inputRow {
    uint32_t value;
};

class MockCSVAdaptiveSource : public AdaptiveSource {
  public:
    MockCSVAdaptiveSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                          uint64_t initialGatheringInterval, std::string filePath, uint64_t intervalIncrease)
        : AdaptiveSource(schema, bufferManager, queryManager, initialGatheringInterval, 1, 12,
                         DataSource::GatheringMode::FREQUENCY_MODE),
          filePath(filePath) {
        this->intervalIncrease = std::chrono::milliseconds(intervalIncrease);
    };

    ~MockCSVAdaptiveSource() = default;

    std::string filePath;
    std::chrono::milliseconds intervalIncrease;

    const std::string toString() const override {
        std::stringstream ss;
        ss << "ADAPTIVE_CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath
           << " gatherInterval=" << this->getGatheringIntervalCount() << "ms, numBuff=" << this->numBuffersToProcess << ")";
        return ss.str();
    };

  private:
    /**
     * @brief read a dummy csv
     */
    void sampleSourceAndFillBuffer(TupleBuffer& tupleBuffer) override {
        std::ifstream input(this->filePath.c_str());
        input.seekg(0, input.beg);
        uint64_t generated_tuples_this_pass = tupleBuffer.getBufferSize() / 4096;
        std::string line;
        uint64_t i = 0;
        while (i < generated_tuples_this_pass) {
            std::getline(input, line);
            std::vector<std::string> tokens;
            boost::algorithm::split(tokens, line, boost::is_any_of(","));
            uint64_t offset = 0;
            offset += sizeof(uint32_t);
            uint32_t val = std::stoul(tokens[0].c_str());
            memcpy(tupleBuffer.getBufferAs<char>() + offset + i * 4096, &val, 4);
            ++i;
        }
        generatedTuples += generated_tuples_this_pass;
        tupleBuffer.setNumberOfTuples(generated_tuples_this_pass);
        generatedBuffers++;
    };

    /**
     * @brief naively increment the gathering interval
     */
    void decideNewGatheringInterval() override {
        auto oldIntervalMillis = this->gatheringInterval.count();
        NES_DEBUG("Old sampling interval: " << oldIntervalMillis << "ms");
        this->setGatheringInterval(std::chrono::milliseconds(oldIntervalMillis + intervalIncrease.count()));
        NES_DEBUG("New sampling interval: " << this->gatheringInterval.count() << "ms");
    };
};

const DataSourcePtr createMockCSVAdaptiveSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                                NodeEngine::QueryManagerPtr queryManager, uint64_t initialGatheringInterval,
                                                std::string filePath, uint64_t intervalIncrease) {
    return std::make_shared<MockCSVAdaptiveSource>(schema, bufferManager, queryManager, initialGatheringInterval, filePath,
                                                   intervalIncrease);
}

/**
 * @brief start a source and check that interval has changed, increase by +1 sec
 * @component AdaptiveSampler::decideNewGatheringInterval()
 * @result true, if source starts, changes interval, and stops
 */
TEST_F(AdaptiveSourceTest, testSamplingChange) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 3133, streamConf);

    std::string path_to_file = "../tests/test_data/adaptive-test-mock.csv";

    SchemaPtr schema = Schema::create()->addField("temperature", UINT32);

    uint64_t num_of_buffers = 1;
    uint64_t initialGatheringInterval = 4000;

    const DataSourcePtr source = createMockCSVAdaptiveSource(
        schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), initialGatheringInterval, path_to_file, 1000);
    source->open();
    while (source->getNumberOfGeneratedBuffers() < num_of_buffers) {
        auto optBuf = source->receiveData();
    }

    ASSERT_NE(initialGatheringInterval, source->getGatheringIntervalCount());
    EXPECT_TRUE(nodeEngine->stop());
}

/**
 * @brief start a source and check that interval has changed, increase +0.1 sec
 * @component AdaptiveSampler::decideNewGatheringInterval()
 * @result true, if source starts, changes interval, interval has increased by increments
 * of 0.1 seconds (100ms), and stops
 */
TEST_F(AdaptiveSourceTest, testSamplingChangeSubSecond) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 3133, streamConf);

    std::string path_to_file = "../tests/test_data/adaptive-test-mock.csv";

    SchemaPtr schema = Schema::create()->addField("temperature", UINT32);

    uint64_t num_of_buffers = 1;
    uint64_t initialGatheringInterval = 350;
    uint64_t intervalIncrease = 100;

    const DataSourcePtr source =
        createMockCSVAdaptiveSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                    initialGatheringInterval, path_to_file, intervalIncrease);

    source->open();
    while (source->getNumberOfGeneratedBuffers() < num_of_buffers) {
        auto optBuf = source->receiveData();
    }

    ASSERT_NE(source->getGatheringIntervalCount(), initialGatheringInterval);
    EXPECT_TRUE(source->getGatheringIntervalCount() > initialGatheringInterval);
    EXPECT_TRUE(source->getGatheringIntervalCount() < 1000);// we don't control how much the change will be
    ASSERT_EQ((source->getGatheringIntervalCount() - initialGatheringInterval) % intervalIncrease, 0);
    EXPECT_TRUE(nodeEngine->stop());
}

}// namespace NES