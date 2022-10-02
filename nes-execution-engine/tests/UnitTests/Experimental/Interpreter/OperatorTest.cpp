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

#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class OperatorTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup OperatorTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup OperatorTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down OperatorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down OperatorTest test class." << std::endl; }
};

TEST_F(OperatorTest, FilterOperatorTest) {
   // setup operator
   auto readField1 =  std::make_shared<ReadFieldExpression>("0");
   auto readField2 =  std::make_shared<ReadFieldExpression>("1");
   auto equalsExpression =  std::make_shared<EqualsExpression>(readField1, readField2);
   auto selection = std::make_shared<Selection>(equalsExpression);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter