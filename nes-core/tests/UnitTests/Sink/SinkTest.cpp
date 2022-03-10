/*
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

#include "SerializableOperator.pb.h"
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <NesBaseTest.hpp>
#include <Network/NetworkChannel.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <ostream>

using namespace std;

/**
 * @brief tests for sinks
 * @todo add tests for processing multiple buffers in a row
 */
namespace NES {
using Runtime::TupleBuffer;
class SinkTest : public Testing::NESBaseTest {
  public:
    SchemaPtr test_schema;
    std::array<uint32_t, 8> test_data{};
    bool write_result{};
    std::string path_to_csv_file;
    std::string path_to_bin_file;
    std::string path_to_osfile_file;
    Testing::BorrowedPortPtr borrowedZmqPort;
    int zmqPort;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("SinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SinkTest class.");
    }

    /* Called before a single test. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        test_schema =
            Schema::create()->addField("KEY", DataTypeFactory::createInt32())->addField("VALUE", DataTypeFactory::createUInt32());
        write_result = false;
        path_to_csv_file = getTestResourceFolder() / "sink.csv";
        path_to_bin_file = getTestResourceFolder() / "sink.bin";
        path_to_osfile_file = getTestResourceFolder() / "testOs.txt";
        PhysicalSourcePtr streamConf = PhysicalSource::create("x", "x1");
        this->nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 0, {streamConf});
        borrowedZmqPort = getAvailablePort();
        zmqPort = *borrowedZmqPort;
    }

    /* Called after a single test. */
    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        borrowedZmqPort.reset();
        Testing::NESBaseTest::TearDown();
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
};

TEST_F(SinkTest, testCSVFileSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    const DataSinkPtr csvSink = createCSVFileSink(test_schema, 0, 0, nodeEngine, 1, path_to_csv_file, true);

    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = Util::prettyPrintTupleBuffer(buffer, test_schema);

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //search for each value
    string contentWOHeader = fileContent.erase(0, fileContent.find('\n') + 1);
    Util::findAndReplaceAll(contentWOHeader, "\n", ",");
    stringstream ss(contentWOHeader);
    string item;
    while (getline(ss, item, ',')) {
        EXPECT_TRUE(bufferContent.find(item) != std::string::npos);
    }
    buffer.release();
}

TEST_F(SinkTest, testTextFileSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();

    const DataSinkPtr binSink = createTextFileSink(test_schema, 0, 0, nodeEngine, 1, path_to_csv_file, true);
    for (uint64_t i = 0; i < 5; ++i) {
        for (uint64_t j = 0; j < 5; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(25);
    write_result = binSink->writeData(buffer, wctx);
    EXPECT_TRUE(write_result);
    std::string bufferContent = Util::prettyPrintTupleBuffer(buffer, test_schema);
    //cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //cout << "File Content=" << fileContent << endl;
    EXPECT_EQ(bufferContent, fileContent);
    buffer.release();
}

TEST_F(SinkTest, testNESBinaryFileSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr binSink = createBinaryNESFileSink(test_schema, 0, 0, nodeEngine, 1, path_to_bin_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    write_result = binSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = Util::prettyPrintTupleBuffer(buffer, test_schema);
    //cout << "Buffer Content= " << bufferContent << endl;

    //deserialize schema
    uint64_t idx = path_to_bin_file.rfind('.');
    std::string shrinkedPath = path_to_bin_file.substr(0, idx + 1);
    std::string schemaFile = shrinkedPath + "schema";
    //cout << "load=" << schemaFile << endl;
    ifstream testFileSchema(schemaFile.c_str());
    EXPECT_TRUE(testFileSchema.good());
    auto* serializedSchema = new SerializableSchema();
    serializedSchema->ParsePartialFromIstream(&testFileSchema);
    SchemaPtr ptr = SchemaSerializationUtil::deserializeSchema(serializedSchema);
    //test SCHEMA
    //cout << "deserialized schema=" << ptr->toString() << endl;
    EXPECT_EQ(ptr->toString(), test_schema->toString());

    auto deszBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    deszBuffer.setNumberOfTuples(4);

    ifstream ifs(path_to_bin_file, ios_base::in | ios_base::binary);
    if (ifs) {
        ifs.read(reinterpret_cast<char*>(deszBuffer.getBuffer()), deszBuffer.getBufferSize());
    } else {
        FAIL();
    }

    //cout << "expected=" << endl << Util::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    //cout << "result=" << endl << Util::prettyPrintTupleBuffer(deszBuffer, test_schema) << endl;

    //cout << "File path = " << path_to_bin_file << " Content=" << Util::prettyPrintTupleBuffer(deszBuffer, test_schema);
    EXPECT_EQ(Util::prettyPrintTupleBuffer(deszBuffer, test_schema), Util::prettyPrintTupleBuffer(buffer, test_schema));
    buffer.release();
}

TEST_F(SinkTest, testCSVPrintSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto csvSink = createCSVPrintSink(test_schema, 0, 0, nodeEngine, 1, os);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "bufferContent before write=" << Util::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = Util::prettyPrintTupleBuffer(buffer, test_schema);
    //cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_osfile_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_osfile_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //cout << "File Content=" << fileContent << endl;
    //search for each value
    string contentWOHeader = fileContent.erase(0, fileContent.find('\n') + 1);
    Util::findAndReplaceAll(contentWOHeader, "\n", ",");
    //cout << "File Content shrinked=" << contentWOHeader << endl;

    stringstream ss(contentWOHeader);
    string item;
    while (getline(ss, item, ',')) {
        //cout << item << endl;
        if (bufferContent.find(item) != std::string::npos) {
            //cout << "found token=" << item << endl;
        } else {
            //cout << "NOT found token=" << item << endl;
            EXPECT_TRUE(false);
        }
    }
    fb.close();
    buffer.release();
}

TEST_F(SinkTest, testNullOutSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto nullSink = createNullOutputSink(1, 0, nodeEngine, 1);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "bufferContent before write=" << Util::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = nullSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = Util::prettyPrintTupleBuffer(buffer, test_schema);
    //cout << "Buffer Content= " << bufferContent << endl;
}

TEST_F(SinkTest, testTextPrintSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);

    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();

    const DataSinkPtr binSink = createTextPrintSink(test_schema, 0, 0, nodeEngine, 1, os);
    for (uint64_t i = 0; i < 5; ++i) {
        for (uint64_t j = 0; j < 5; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(25);
    write_result = binSink->writeData(buffer, wctx);
    EXPECT_TRUE(write_result);
    std::string bufferContent = Util::prettyPrintTupleBuffer(buffer, test_schema);
    ////cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_osfile_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_osfile_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(bufferContent, fileContent.substr(0, fileContent.size() - 1));
    buffer.release();
    fb.close();
    buffer.release();
}

TEST_F(SinkTest, testCSVZMQSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createCSVZmqSink(test_schema, 0, 0, nodeEngine, 1, "localhost", zmqPort);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    ////cout << "buffer before send=" << Util::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      "localhost",
                                      zmqPort,
                                      1,
                                      12,
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    ////std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        // Receive data.
        zmq_source->open();
        auto schemaData = zmq_source->receiveData();
        TupleBuffer bufSchema = schemaData.value();
        std::string schemaStr;
        schemaStr.assign(bufSchema.getBuffer<char>(), bufSchema.getNumberOfTuples());
        //cout << "Schema=" << schemaStr << endl;
        EXPECT_EQ(Util::toCSVString(test_schema), schemaStr);

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        //cout << "Buffer=" << bufData.getBuffer<char>() << endl;

        std::string bufferContent = Util::printTupleBufferAsCSV(buffer, test_schema);
        std::string dataStr;
        dataStr.assign(bufData.getBuffer<char>(), bufData.getNumberOfTuples());
        //cout << "Buffer Content received= " << bufferContent << endl;
        EXPECT_EQ(bufferContent, dataStr);
        receiving_finished = true;
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testTextZMQSink) {
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createTextZmqSink(test_schema, 0, 0, nodeEngine, 1, "localhost", zmqPort);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "buffer before send=" << Util::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      "localhost",
                                      zmqPort,
                                      1,
                                      12,
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    //std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        zmq_source->open();
        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();

        std::string bufferContent = Util::printTupleBufferAsText(bufData);
        //cout << "Buffer Content received= " << bufferContent << endl;
        EXPECT_EQ(bufferContent, Util::prettyPrintTupleBuffer(buffer, test_schema));
        receiving_finished = true;
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testBinaryZMQSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createBinaryZmqSink(test_schema, 0, 0, nodeEngine, 1, "localhost", zmqPort, false);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "buffer before send=" << Util::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      "localhost",
                                      zmqPort,
                                      1,
                                      12,
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    //std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    auto receiving_thread = std::thread([&]() {
        zmq_source->open();
        auto schemaData = zmq_source->receiveData();
        TupleBuffer bufSchema = schemaData.value();
        auto* serializedSchema = new SerializableSchema();
        serializedSchema->ParseFromArray(bufSchema.getBuffer(), bufSchema.getNumberOfTuples());
        SchemaPtr ptr = SchemaSerializationUtil::deserializeSchema(serializedSchema);
        EXPECT_EQ(ptr->toString(), test_schema->toString());

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        //cout << "rec buffer tups=" << bufData.getNumberOfTuples()
//        << " content=" << Util::prettyPrintTupleBuffer(bufData, test_schema) << endl;
        //cout << "ref buffer tups=" << buffer.getNumberOfTuples()
//        << " content=" << Util::prettyPrintTupleBuffer(buffer, test_schema) << endl;
        EXPECT_EQ(Util::prettyPrintTupleBuffer(bufData, test_schema), Util::prettyPrintTupleBuffer(buffer, test_schema));
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testWatermarkForZMQ) {
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    buffer.setWatermark(1234567);
    const DataSinkPtr zmq_sink = createBinaryZmqSink(test_schema, 0, 0, nodeEngine, 1, "localhost", zmqPort, false);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);

    // Create ZeroMQ Data Source.
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      "localhost",
                                      zmqPort,
                                      1,
                                      12,
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    //std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    auto receiving_thread = std::thread([&]() {
        zmq_source->open();
        auto schemaData = zmq_source->receiveData();

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        EXPECT_EQ(bufData.getWatermark(), 1234567ull);
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testWatermarkCsvSource) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    buffer.setWatermark(1234567);

    const DataSinkPtr csvSink = createCSVFileSink(test_schema, 0, 0, nodeEngine, 1, path_to_csv_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "watermark=" << buffer.getWatermark() << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_EQ(buffer.getWatermark(), 1234567ull);
    buffer.release();
}

}// namespace NES
