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

#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSinkTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSinkTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down ConvertLogicalToPhysicalSinkTest test class." << std::endl; }

    void SetUp() {
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
        nodeEngine = NodeEngine::create("127.0.0.1", 12345, conf);
    }

    void TearDown() {
        nodeEngine->stop();
        nodeEngine.reset();
    }

    NodeEngine::NodeEnginePtr nodeEngine{nullptr};
};

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingFileLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = FileSinkDescriptor::create("file.log", "CSV_FORMAT", "APPEND");
    DataSinkPtr fileOutputSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor, nodeEngine, 0);
    EXPECT_EQ(fileOutputSink->toString(), "FILE_SINK");
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingZMQLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = ZmqSinkDescriptor::create("127.0.0.1", 2000);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    DataSinkPtr zmqSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor, nodeEngine, 0);
    EXPECT_EQ(zmqSink->toString(), "ZMQ_SINK");
}
#ifdef ENABLE_KAFKA_BUILD
TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingKafkaLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = KafkaSinkDescriptor::create("test", "localhost:9092", 1000);
    DataSinkPtr kafkaSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor);
    EXPECT_EQ(kafkaSink->getType(), KAFKA_SINK);
}
#endif
TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingPrintLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = PrintSinkDescriptor::create();
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    DataSinkPtr printSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor, nodeEngine, 0);
    EXPECT_EQ(printSink->toString(), "PRINT_SINK");
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingNetworkLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    Network::NodeLocation nodeLocation{1, "localhost", 31337};
    Network::NesPartition nesPartition{1, 22, 33, 44};
    SinkDescriptorPtr sinkDescriptor =
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(1), 1);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    DataSinkPtr networkSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor, nodeEngine, 0);
    EXPECT_EQ(networkSink->toString(), "NETWORK_SINK");
}

}// namespace NES
