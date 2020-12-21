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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;
namespace NES {

class TupleBufferTest : public testing::Test {
  public:
    NodeEngine::BufferManagerPtr bufferManager;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup TupleBufferTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("TupleBufferTest.log", NES::LOG_DEBUG);
        std::cout << "Setup TupleBufferTest test case." << std::endl;
        bufferManager = std::make_shared<NodeEngine::BufferManager>(1024, 1024);
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Tear down TupleBufferTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TupleBufferTest test class." << std::endl; }
};

TEST_F(TupleBufferTest, testPrintingOfTupleBuffer) {

    struct __attribute__((packed)) MyTuple {
        uint64_t i64;
        float f;
        double d;
        uint32_t i32;
        char s[5];
    };

    auto optBuf = bufferManager->getBufferNoBlocking();
    auto buf = *optBuf;
    //    MyTuple* my_array = (MyTuple*)malloc(5 * sizeof(MyTuple));
    auto my_array = buf.getBufferAs<MyTuple>();
    for (unsigned int i = 0; i < 5; ++i) {
        my_array[i] = MyTuple{i, float(0.5f * i), double(i * 0.2), i * 2, "1234"};
        std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d << "|" << my_array[i].i32 << "|"
                  << std::string(my_array[i].s, 5) << std::endl;
    }
    buf.setNumberOfTuples(5);

    SchemaPtr s = Schema::create()
                      ->addField("i64", DataTypeFactory::createUInt64())
                      ->addField("f", DataTypeFactory::createFloat())
                      ->addField("d", DataTypeFactory::createDouble())
                      ->addField("i32", DataTypeFactory::createUInt32())
                      ->addField("s", DataTypeFactory::createFixedChar(5));

    std::string reference = "+----------------------------------------------------+\n"
                            "|i64:UINT64|f:FLOAT32|d:FLOAT64|i32:UINT32|s:CHAR|\n"
                            "+----------------------------------------------------+\n"
                            "|0|0.000000|0.000000|0|1234 |\n"
                            "|1|0.500000|0.200000|2|1234 |\n"
                            "|2|1.000000|0.400000|4|1234 |\n"
                            "|3|1.500000|0.600000|6|1234 |\n"
                            "|4|2.000000|0.800000|8|1234 |\n"
                            "+----------------------------------------------------+";

    std::string result = UtilityFunctions::prettyPrintTupleBuffer(buf, s);
    std::cout << "RES=" << result << std::endl;
    NES_DEBUG("Reference size=" << reference.size() << " content=" << std::endl << reference);
    NES_DEBUG("Result size=" << result.size() << " content=" << std::endl << result);
    NES_DEBUG("----");
    EXPECT_EQ(reference.size(), result.size());
    //    EXPECT_EQ(reference, result);//TODO fix bug
}

TEST_F(TupleBufferTest, testEndianessOneItem) {

    struct __attribute__((packed)) TestStruct {
        u_int8_t v1;
        u_int16_t v2;
        u_int32_t v3;
        u_int64_t v4;
        int8_t v5;
        int16_t v6;
        int32_t v7;
        int64_t v8;
        float v9;
        double v10;
    };

    TestStruct ts;
    ts.v1 = 1;
    ts.v2 = 1;
    ts.v3 = 1;
    ts.v4 = 1;
    ts.v5 = 1;
    ts.v6 = 1;
    ts.v7 = -2;
    ts.v8 = -1;
    ts.v9 = 1.1;
    ts.v10 = 1.2;

    auto optBuf = bufferManager->getBufferNoBlocking();
    auto testBuf = *optBuf;
    SchemaPtr s = Schema::create()
                      ->addField("v1", UINT8)
                      ->addField("v2", UINT16)
                      ->addField("v3", UINT32)
                      ->addField("v4", UINT64)
                      ->addField("v5", INT8)
                      ->addField("v6", INT16)
                      ->addField("v7", INT32)
                      ->addField("v8", INT64)
                      ->addField("v9", FLOAT32)
                      ->addField("v10", FLOAT64);

    testBuf.getBufferAs<TestStruct>()[0] = ts;
    testBuf.setNumberOfTuples(1);
    cout << "to string=" << endl;
    std::string result = UtilityFunctions::prettyPrintTupleBuffer(testBuf, s);

    cout << "to printTupleBufferAsCSV=" << UtilityFunctions::printTupleBufferAsCSV(testBuf, s) << endl;

    testBuf.revertEndianness(s);
    cout << "after reverse1=" << UtilityFunctions::printTupleBufferAsCSV(testBuf, s) << endl;

    testBuf.revertEndianness(s);
    cout << "after reverse2=" << UtilityFunctions::printTupleBufferAsCSV(testBuf, s) << endl;

    string expected = "1,1,1,1,1,1,-2,-1,1.100000,1.200000\n";
    EXPECT_EQ(expected, UtilityFunctions::printTupleBufferAsCSV(testBuf, s));
}

TEST_F(TupleBufferTest, testEndianessTwoItems) {

    struct __attribute__((packed)) TestStruct {
        u_int8_t v1;
        u_int16_t v2;
        u_int32_t v3;
        u_int64_t v4;
        int8_t v5;
        int16_t v6;
        int32_t v7;
        int64_t v8;
        float v9;
        double v10;
    };

    auto testBuf = bufferManager->getBufferNoBlocking().value();

    TestStruct* ts = testBuf.getBufferAs<TestStruct>();

    for (uint64_t i = 0; i < 5; i++) {
        ts[i].v1 = i;
        ts[i].v2 = i;
        ts[i].v3 = i;
        ts[i].v4 = i + 1;
        ts[i].v5 = i;
        ts[i].v6 = 1;
        ts[i].v7 = i;
        ts[i].v8 = i + 5;
        ts[i].v9 = 1.1 * i + 3;
        ts[i].v10 = 1.2 * i;
    }

    testBuf.setNumberOfTuples(5);

    SchemaPtr s = Schema::create()
                      ->addField("v1", UINT8)
                      ->addField("v2", UINT16)
                      ->addField("v3", UINT32)
                      ->addField("v4", UINT64)
                      ->addField("v5", INT8)
                      ->addField("v6", INT16)
                      ->addField("v7", INT32)
                      ->addField("v8", INT64)
                      ->addField("v9", FLOAT32)
                      ->addField("v10", FLOAT64);

    cout << "to string=" << endl;
    std::string result = UtilityFunctions::prettyPrintTupleBuffer(testBuf, s);

    cout << "to printTupleBufferAsCSV=" << UtilityFunctions::printTupleBufferAsCSV(testBuf, s) << endl;

    testBuf.revertEndianness(s);
    cout << "after reverse1=" << UtilityFunctions::printTupleBufferAsCSV(testBuf, s) << endl;

    testBuf.revertEndianness(s);
    cout << "after reverse2=" << UtilityFunctions::printTupleBufferAsCSV(testBuf, s) << endl;

    string expected = "0,0,0,1,0,1,0,5,3.000000,0.000000\n1,1,1,2,1,1,1,6,4.100000,1.200000\n2,2,2,3,2,1,2,7,5.200000,"
                      "2.400000\n3,3,3,4,3,1,3,8,6.300000,3.600000\n4,4,4,5,4,1,4,9,7.400000,4.800000\n";
    EXPECT_EQ(expected, UtilityFunctions::printTupleBufferAsCSV(testBuf, s));
}

TEST_F(TupleBufferTest, testCopyAndSwap) {
    auto buffer = bufferManager->getBufferBlocking();
    for (auto i = 0; i < 10; ++i) {
        *buffer.getBufferAs<uint64_t>() = 0;
        buffer = bufferManager->getBufferBlocking();
    }
}

}// namespace NES
