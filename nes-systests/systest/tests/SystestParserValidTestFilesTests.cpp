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
#include <string>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SystestParser.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Systest
{
/// Tests if SLT Parser accepts ands parses valid .test files correctly
class SystestParserValidTestFileTest : public Testing::BaseUnitTest
{
public:
    static std::string testFileName;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestParserValidTestFileTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestParserValidTestFileTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestParserValidTestFileTest test class."); }
};

TEST_F(SystestParserValidTestFileTest, ValidTestFile)
{
    const std::string filename = std::string(TEST_DATA_DIR) + "valid.dummy";

    const std::string expectQuery1 = R"(Query::from("e123").filter(Attribute("i") >= 10).SINK;)";
    const std::string expectQuery2 = "Query::from(\"e124\")\n    .filter(Attribute(\"i\") >= 10)\n    .SINK;";
    const std::vector<std::string> expectResult = {{"1,1,1"}, {"1,1,1"}, {"1,1,1"}};
    bool hasFirstSourceBeenParsed = false;
    bool hasSecondSourceBeenParsed = false;
    SystestParser::SystestLogicalSource firstExpectedSystestLogicalSource
        = {.name = "e123", .fields = {{.type = DataTypeProvider::provideDataType(LogicalType::UINT32), .name = "id"}}};
    SystestParser::SystestLogicalSource secondExpectedSystestLogicalSource
        = {.name = "e124",
           .fields
           = {{.type = DataTypeProvider::provideDataType(LogicalType::INT8), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::UINT8), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::INT16), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::UINT16), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::INT32), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::UINT32), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::INT64), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::FLOAT32), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::FLOAT64), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::BOOLEAN), .name = "i"},
              {.type = DataTypeProvider::provideDataType(LogicalType::CHAR), .name = "i"}}};

    SystestParser parser{};
    parser.registerOnQueryCallback([&expectQuery1, &expectQuery2](const std::string& query, size_t)
                                   { ASSERT_TRUE(query == expectQuery1 || query == expectQuery2); });
    parser.registerOnAttachSourceCallback([](const SystestAttachSource&&) { /* noop */ });
    parser.registerOnSystestLogicalSourceCallback(
        [&hasFirstSourceBeenParsed, &hasSecondSourceBeenParsed, &firstExpectedSystestLogicalSource, &secondExpectedSystestLogicalSource](
            const SystestParser::SystestLogicalSource& source)
        {
            if (source.name == firstExpectedSystestLogicalSource.name)
            {
                hasFirstSourceBeenParsed = true;
                ASSERT_EQ(source, firstExpectedSystestLogicalSource);
                return;
            }
            if (source.name == secondExpectedSystestLogicalSource.name)
            {
                hasSecondSourceBeenParsed = true;
                ASSERT_EQ(source, secondExpectedSystestLogicalSource);
                return;
            }
            FAIL();
        });

    ASSERT_TRUE(parser.loadFile(filename));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(hasFirstSourceBeenParsed);
    ASSERT_TRUE(hasSecondSourceBeenParsed);
}

TEST_F(SystestParserValidTestFileTest, Comments1TestFile)
{
    const auto* const filename = TEST_DATA_DIR "comments.dummy";

    SystestParser::SystestLogicalSource expectedSystestLogicalSource;
    SystestAttachSource expectedAttachSource{};
    expectedSystestLogicalSource.name = "window";
    expectedSystestLogicalSource.fields
        = {{.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "id"},
           {.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "value"},
           {.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "timestamp"}};
    expectedAttachSource.tuples = {"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",
                                   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",   "1,6,6000",   "1,7,7000",
                                   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
                                   "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"};

    /// Expected queries and results
    const auto* const expectedQuery1 = R"(Query::from("window")
    .filter(Attribute("value") == 1)
    .SINK;)";

    const auto* const expectedQuery2 = R"(Query::from("window")
    .filter(Attribute("value") != 1)
    .SINK;)";

    std::vector<std::string> expectedQueries = {expectedQuery1, expectedQuery2};

    std::vector<std::vector<std::string>> expectedResults
        = {{"1,1,1000", "12,1,1001", "4,1,1002"},
           {"1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",
            "1,6,6000",   "1,7,7000",   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
            "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"}};

    size_t queryCounter = 0;

    SystestParser parser{};
    parser.registerOnSystestLogicalSourceCallback(
        [&](SystestParser::SystestLogicalSource&& source)
        {
            ASSERT_EQ(source.name, expectedSystestLogicalSource.name);
            ASSERT_EQ(source.fields, expectedSystestLogicalSource.fields);
        });

    parser.registerOnAttachSourceCallback(
        [&](SystestAttachSource&& attachSource)
        { ASSERT_TRUE(attachSource.tuples and attachSource.tuples.value() == expectedAttachSource.tuples.value()); });

    parser.registerOnQueryCallback(
        [&](const std::string& query, size_t)
        {
            ASSERT_LT(queryCounter, expectedQueries.size());
            ASSERT_EQ(query, expectedQueries[queryCounter]);
        });

    ASSERT_TRUE(parser.loadFile(filename));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
}

TEST_F(SystestParserValidTestFileTest, FilterTestFile)
{
    const auto* const filename = TEST_DATA_DIR "filter.dummy";

    SystestParser::SystestLogicalSource expectedSystestLogicalSource;
    SystestAttachSource expectedAttachSource{};
    expectedSystestLogicalSource.name = "window";
    expectedSystestLogicalSource.fields
        = {{.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "id"},
           {.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "value"},
           {.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "timestamp"}};
    expectedAttachSource.tuples = {"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",
                                   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",   "1,6,6000",   "1,7,7000",
                                   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
                                   "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"};

    std::vector<std::string> expectedQueries = {
        R"(Query::from("window")
    .filter(Attribute("value") == 1)
    .SINK;)",

        R"(Query::from("window")
    .filter(Attribute("id") >= 10)
    .SINK;)",

        R"(Query::from("window")
    .filter(Attribute("timestamp") <= 10000)
    .SINK;)",

        R"(Query::from("window")
    .filter(Attribute("timestamp") >=  5000)
    .filter(Attribute("timestamp") <=  15000)
    .SINK;)",

        R"(Query::from("window")
    .filter(Attribute("value") !=  1)
    .SINK;)",

        R"(Query::from("window")
    .filter(Attribute("id") = Attribute("id") - 1)
    .SINK;)"};

    std::vector<std::vector<std::string>> expectedResults
        = {{"1,1,1000", "12,1,1001", "4,1,1002"},
           {"12,1,1001", "11,2,2001", "16,2,2002", "11,3,3001"},
           {"1,1,1000",
            "12,1,1001",
            "4,1,1002",
            "1,2,2000",
            "11,2,2001",
            "16,2,2002",
            "1,3,3000",
            "11,3,3001",
            "1,3,3003",
            "1,3,3200",
            "1,4,4000",
            "1,5,5000",
            "1,6,6000",
            "1,7,7000",
            "1,8,8000",
            "1,9,9000",
            "1,10,10000"},
           {"1,5,5000",
            "1,6,6000",
            "1,7,7000",
            "1,8,8000",
            "1,9,9000",
            "1,10,10000",
            "1,11,11000",
            "1,12,12000",
            "1,13,13000",
            "1,14,14000",
            "1,15,15000"},
           {"1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",
            "1,6,6000",   "1,7,7000",   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
            "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"},
           {"0,1,1000",   "11,1,1001",  "3,1,1002",   "0,2,2000",   "10,2,2001",  "15,2,2002",  "10,3,3000",
            "10,3,3001",  "0,3,3003",   "0,3,3200",   "0,4,4000",   "0,5,5000",   "0,6,6000",   "0,7,7000",
            "0,8,8000",   "0,9,9000",   "0,10,10000", "0,11,11000", "0,12,12000", "0,13,13000", "0,14,14000",
            "0,15,15000", "0,16,16000", "0,17,17000", "0,18,18000", "0,19,19000", "0,20,20000", "0,21,21000"}};

    size_t queryCounter = 0;

    SystestParser parser{};
    parser.registerOnSystestLogicalSourceCallback(
        [&](SystestParser::SystestLogicalSource&& source)
        {
            ASSERT_EQ(source.name, expectedSystestLogicalSource.name);
            ASSERT_EQ(source.fields, expectedSystestLogicalSource.fields);
        });

    parser.registerOnAttachSourceCallback(
        [&](SystestAttachSource&& attachSource)
        { ASSERT_TRUE(attachSource.tuples and attachSource.tuples.value() == expectedAttachSource.tuples.value()); });

    parser.registerOnQueryCallback(
        [&](const std::string& query, size_t)
        {
            ASSERT_LT(queryCounter, expectedQueries.size());
            ASSERT_EQ(query, expectedQueries[queryCounter]);
        });

    ASSERT_TRUE(parser.loadFile(filename));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
}

}
