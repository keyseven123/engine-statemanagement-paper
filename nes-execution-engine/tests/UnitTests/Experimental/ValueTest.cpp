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
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class ValueTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ValueTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ValueTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ValueTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ValueTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ValueTest test class." << std::endl; }
};

TEST_F(ValueTest, assignMentTest) {
    auto intValue = std::make_unique<Int32>(42);
    std::unique_ptr<Any> valAny = cast<Any>(intValue);
    std::unique_ptr<Any> valAny2 = cast<Any>(valAny);

    auto anyValue = Value<Int32>(std::move(intValue));
    anyValue = anyValue + 10;

    Value<Int8> val = Value<Int8>((int8_t) 42);
    ASSERT_TRUE(val.value->getType()->isInteger());
    Value<Int8> val2 = (int8_t) 42;
    ASSERT_TRUE(val2.value->getType()->isInteger());
    Value<Any> va = val2;
    Value<Any> va2 = Value<>(10);
    auto anyValueNew1 = Value<>(10);
    auto anyValueNew2 = Value<>(10);
    auto anyValueNew3 = anyValueNew1;
    anyValueNew3 = anyValueNew2;
    val2 = val;
}

TEST_F(ValueTest, addValueTest) {
    auto x = Value<>(1);
    auto y = Value<>(2);
    auto intZ = y + x;
    ASSERT_EQ(intZ.as<Int32>().value->getValue(), 3);

    Value<Any> anyZ = y + x;
    ASSERT_EQ(anyZ.as<Int32>().value->getValue(), 3);

    anyZ = intZ + intZ;
    ASSERT_EQ(anyZ.as<Int32>().value->getValue(), 6);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter