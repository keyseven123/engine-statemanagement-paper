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

#include <Exceptions/InvalidQueryException.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

namespace NES {

class SyntacticQueryValidationTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SyntacticQueryValidationTest.log", NES::LOG_NONE);
        NES_INFO("Setup SyntacticQueryValidationTest class.");
    }
    void TearDown() { NES_INFO("Tear down SyntacticQueryValidationTest class."); }

    void PrintQString(std::string s) { std::cout << std::endl << "QUERY STRING:" << std::endl << s << std::endl; }

    void TestForException(std::string queryString) {
        PrintQString(queryString);
        auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create();
        EXPECT_THROW(syntacticQueryValidation->checkValidity(queryString), InvalidQueryException);
    }
};

// Positive test for a syntactically valid query
TEST_F(SyntacticQueryValidationTest, validQueryTest) {
    NES_INFO("Valid Query test");

    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create();

    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100); ";

    syntacticQueryValidation->checkValidity(queryString);
}

// Test a query with missing ; at line end
TEST_F(SyntacticQueryValidationTest, missingSemicolonTest) {
    NES_INFO("Missing semicolon test");

    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100) ";

    TestForException(queryString);
}

// Test a query where filter is misspelled as fliter
TEST_F(SyntacticQueryValidationTest, typoInFilterTest) {
    NES_INFO("Typo in filter test");

    std::string queryString = "Query::from(\"default_logical\").fliter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100); ";

    TestForException(queryString);
}

// Test a query where a closing parenthesis is missing
TEST_F(SyntacticQueryValidationTest, missingClosingParenthesisTest) {
    NES_INFO("Missing closing parenthesis test");

    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100; ";

    TestForException(queryString);
}

// Test a query where a boolean operator is invalid (& instead of &&)
TEST_F(SyntacticQueryValidationTest, invalidBoolOperatorTest) {
    NES_INFO("Invalid bool operator test");

    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 & Attribute(\"id\") < 100); ";

    TestForException(queryString);
}

}// namespace NES