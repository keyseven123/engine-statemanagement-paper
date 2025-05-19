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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Sources/SourceProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace
{

/// Parses the stream into a schema. It expects a string in the format: FIELDNAME FIELDTYPE, FIELDNAME FIELDTYPE, ...
NES::Systest::SystestSchema parseSchemaFields(const std::vector<std::string>& arguments)
{
    NES::Systest::SystestSchema schema;
    if (arguments.size() % 2 != 0)
    {
        if (const auto& lastArg = arguments.back(); lastArg.ends_with(".csv"))
        {
            throw NES::SLTUnexpectedToken(
                "Incomplete fieldtype/fieldname pair for arguments {}; {} potentially is a CSV file? Are you mixing semantics",
                fmt::join(arguments, ","),
                lastArg);
        }
        throw NES::SLTUnexpectedToken("Incomplete fieldtype/fieldname pair for arguments {}", fmt::join(arguments, ", "));
    }

    for (size_t i = 0; i < arguments.size(); i += 2)
    {
        std::shared_ptr<NES::DataType> dataType;
        if (auto type = magic_enum::enum_cast<NES::BasicType>(arguments[i]); type.has_value())
        {
            dataType = NES::DataTypeProvider::provideBasicType(type.value());
        }
        else if (NES::Util::toLowerCase(arguments[i]) == "varsized")
        {
            dataType = NES::DataTypeProvider::provideDataType(NES::LogicalType::VARSIZED);
        }
        else
        {
            throw NES::SLTUnexpectedToken("Unknown basic type: " + arguments[i]);
        }
        schema.emplace_back(dataType, arguments[i + 1]);
    }

    return schema;
}
}

namespace
{

std::vector<std::string> validateAttachSource(
    const std::unordered_set<std::string>& seenLogicalSourceNames, const std::string& line, const size_t numberOfTokensInCustomConfig)
{
    const auto attachSourceTokens = NES::Util::splitWithStringDelimiter<std::string>(line, " ");
    const auto hasCustomConfig = attachSourceTokens.size() == numberOfTokensInCustomConfig;
    INVARIANT(NES::Util::toUpperCase(attachSourceTokens.front()) == "ATTACH", "Expected first token of attach source to be 'ATTACH'");
    INVARIANT(
        NES::Sources::SourceProvider::contains(attachSourceTokens.at(1)),
        "Expected second token of attach source to be valid source type, but was: {}",
        attachSourceTokens.at(1));
    INVARIANT(
        attachSourceTokens.size() == (numberOfTokensInCustomConfig - 1) or hasCustomConfig,
        "Expected {} or {} tokens in attach source statement, but got {}",
        (numberOfTokensInCustomConfig - 1),
        numberOfTokensInCustomConfig,
        attachSourceTokens.size());
    INVARIANT(
        not(hasCustomConfig) or not(std::filesystem::path(attachSourceTokens.at(2)).empty()),
        "The provided custom source config path was not valid: {}",
        attachSourceTokens.at(2));
    INVARIANT(
        seenLogicalSourceNames.contains(attachSourceTokens.at(attachSourceTokens.size() - 2)),
        "Expected second to last token of attach source to be an existing logical source name, but was: {}",
        attachSourceTokens.at(attachSourceTokens.size() - 2));
    (void)seenLogicalSourceNames; /// allows release mode to build (fails because of 'unused-parameter' otherwise
    INVARIANT(
        magic_enum::enum_cast<NES::TestDataIngestionType>(NES::Util::toUpperCase(attachSourceTokens.back())),
        "Last keyword of attach source must be a valid TestDataIngestionType, but was: {}",
        attachSourceTokens.back());
    return attachSourceTokens;
}
}

namespace NES::Systest
{

static constexpr auto SystestLogicalSourceToken = "Source"s;
static constexpr auto AttachSourceToken = "Attach"s;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto SinkToken = "SINK"s;
static constexpr auto ResultDelimiter = "----"s;

static constexpr std::array<std::pair<std::string_view, TokenType>, 6> stringToToken
    = {{{SystestLogicalSourceToken, TokenType::SLT_SOURCE},
        {AttachSourceToken, TokenType::ATTACH_SOURCE},
        {QueryToken, TokenType::QUERY},
        {SinkToken, TokenType::SINK},
        {ResultDelimiter, TokenType::RESULT_DELIMITER}}};

static bool emptyOrComment(const std::string& line)
{
    return line.empty() /// completely empty
        || line.find_first_not_of(" \t\n\r\f\v") == std::string::npos /// only whitespaces
        || line.starts_with('#'); /// slt comment
}

void SystestParser::registerSubstitutionRule(const SubstitutionRule& rule)
{
    auto found = std::ranges::find_if(substitutionRules, [&rule](const SubstitutionRule& r) { return r.keyword == rule.keyword; });
    PRECONDITION(
        found == substitutionRules.end(),
        "substitution rule keywords must be unique. Tried to register for the second time: {}",
        rule.keyword);
    substitutionRules.emplace_back(rule);
}

/// We do not load the file in a constructor, as we want to be able to handle errors
bool SystestParser::loadFile(const std::filesystem::path& filePath)
{
    std::ifstream infile(filePath);
    if (!infile.is_open() || infile.bad())
    {
        return false;
    }
    std::stringstream buffer;
    buffer << infile.rdbuf();
    return loadString(buffer.str());
}

bool SystestParser::loadString(const std::string& str)
{
    currentLine = 0;
    lines.clear();

    std::istringstream stream(str);
    std::string line;
    while (std::getline(stream, line))
    {
        /// Remove commented code
        const size_t commentPos = line.find('#');
        if (commentPos != std::string::npos)
        {
            line = line.substr(0, commentPos);
        }
        /// add lines that do not start with a comment
        if (commentPos != 0)
        {
            /// Apply subsitutions & add to parsing lines
            applySubstitutionRules(line);
            lines.push_back(line);
        }
    }
    return true;
}

void SystestParser::registerOnQueryCallback(QueryCallback callback)
{
    this->onQueryCallback = std::move(callback);
}

void SystestParser::registerOnSystestLogicalSourceCallback(SystestLogicalSourceCallback callback)
{
    this->onSystestLogicalSourceCallback = std::move(callback);
}
void SystestParser::registerOnAttachSourceCallback(AttachSourceCallback callback)
{
    this->onAttachSourceCallback = std::move(callback);
}

void SystestParser::registerOnSystestSystestSinkCallback(SystestSinkCallback callback)
{
    this->onSystestSinkCallback = std::move(callback);
}

void SystestParser::parse(SystestStarterGlobals& systestStarterGlobals, const std::string_view testFileName)
{
    SystestQueryNumberAssigner queryNumberAssigner{};
    while (auto token = nextToken())
    {
        switch (token.value())
        {
            case TokenType::ATTACH_SOURCE: {
                if (onAttachSourceCallback)
                {
                    onAttachSourceCallback(expectAttachSource());
                }
                break;
            }
            case TokenType::SLT_SOURCE: {
                auto source = expectSystestLogicalSource();
                if (onSystestLogicalSourceCallback)
                {
                    onSystestLogicalSourceCallback(std::move(source));
                }
                break;
            }
            case TokenType::SINK: {
                auto sink = expectSink();
                if (onSystestSinkCallback)
                {
                    onSystestSinkCallback(std::move(sink));
                }
                break;
            }
            case TokenType::QUERY: {
                if (onQueryCallback)
                {
                    onQueryCallback(expectQuery(), queryNumberAssigner.getNextQueryNumber());
                }
                break;
            }
            case TokenType::RESULT_DELIMITER: {
                systestStarterGlobals.addQueryResult(testFileName, queryNumberAssigner.getNextQueryResultNumber(), expectTuples());
                break;
            }
            case TokenType::INVALID: {
                throw SLTUnexpectedToken("got invalid token in line: {}", lines[currentLine]);
            }
        }
    }
}

void SystestParser::applySubstitutionRules(std::string& line)
{
    for (const auto& rule : substitutionRules)
    {
        size_t pos = 0;
        const std::string& keyword = rule.keyword;

        while ((pos = line.find(keyword, pos)) != std::string::npos)
        {
            /// Apply the substitution function to the part of the string found
            std::string substring = line.substr(pos, keyword.length());
            rule.ruleFunction(substring);

            /// Replace the found substring with the modified substring
            line.replace(pos, keyword.length(), substring);
            pos += substring.length();
        }
    }
}

std::optional<TokenType> SystestParser::getTokenIfValid(std::string potentialToken)
{
    /// Query is a special case as it's identifying token is not space seperated
    if (potentialToken.starts_with(QueryToken))
    {
        return TokenType::QUERY;
    }
    /// Lookup in map
    const auto* it = std::ranges::find_if(stringToToken, [&potentialToken](const auto& pair) { return pair.first == potentialToken; });
    if (it != stringToToken.end())
    {
        return it->second;
    }
    return std::nullopt;
}

bool SystestParser::moveToNextToken()
{
    /// Do not move to next token if its the first
    if (firstToken)
    {
        firstToken = false;
    }
    else
    {
        ++currentLine;
    }

    /// Ignore comments
    while (currentLine < lines.size() && emptyOrComment(lines[currentLine]))
    {
        ++currentLine;
    }

    /// Return false if we reached the end of the file
    return currentLine < lines.size();
}


std::optional<TokenType> SystestParser::nextToken()
{
    if (!moveToNextToken())
    {
        return std::nullopt;
    }

    std::string potentialToken;
    std::istringstream stream(lines[currentLine]);
    stream >> potentialToken;

    INVARIANT(!potentialToken.empty(), "a potential token should never be empty");

    return getTokenIfValid(potentialToken);
}

SystestParser::SystestSink SystestParser::expectSink() const
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    SystestSink sink;
    const auto& line = lines[currentLine];
    std::istringstream lineAsStream(line);

    /// Read and discard the first word as it is always Source
    std::string discard;
    if (!(lineAsStream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }
    INVARIANT(discard == SinkToken, "Expected first word to be `{}` for sink statement", SystestLogicalSourceToken);

    /// Read the source name and check if successful
    if (!(lineAsStream >> sink.name))
    {
        throw SLTUnexpectedToken("failed to read sink name in {}", line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (lineAsStream >> argument)
    {
        arguments.push_back(argument);
    }

    /// After the source definition line we expect schema fields
    sink.fields = parseSchemaFields(arguments);

    return sink;
}

SystestParser::SystestLogicalSource SystestParser::expectSystestLogicalSource()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    SystestLogicalSource source;
    auto& line = lines[currentLine];
    std::istringstream stream(line);

    /// Read and discard the first word as it is always Source
    std::string discard;
    if (!(stream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }
    INVARIANT(discard == SystestLogicalSourceToken, "Expected first word to be `{}` for source statement", SystestLogicalSourceToken);

    /// Read the source name and check if successful
    if (!(stream >> source.name))
    {
        throw SLTUnexpectedToken("failed to read source name in {}", line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (stream >> argument)
    {
        arguments.push_back(argument);
    }

    /// After the source definition line we expect schema fields
    source.fields = parseSchemaFields(arguments);
    seenLogicalSourceNames.emplace(source.name);

    return source;
}

/// Attach SOURCE_TYPE LOGICAL_SOURCE_NAME DATA_SOURCE_TYPE
/// Attach SOURCE_TYPE SOURCE_CONFIG_PATH LOGICAL_SOURCE_NAME DATA_SOURCE_TYPE
SystestAttachSource SystestParser::expectAttachSource()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    static constexpr size_t NUMBER_OF_TOKENS_IN_CUSTOM_CONFIG = 5;
    const auto attachSourceTokens = validateAttachSource(seenLogicalSourceNames, lines[currentLine], NUMBER_OF_TOKENS_IN_CUSTOM_CONFIG);

    SystestAttachSource attachSource;
    attachSource.sourceType = std::string(attachSourceTokens.at(1));

    /// parse (optional) configuration path
    attachSource.configurationPath = [](const std::vector<std::string>& tokens, SystestAttachSource& attachSource)
    {
        if (tokens.size() == NUMBER_OF_TOKENS_IN_CUSTOM_CONFIG)
        {
            return std::filesystem::path(tokens.at(2));
        }
        /// Default config path
        return std::filesystem::path(TEST_CONFIGURATION_DIR)
            / fmt::format("sources/{}_{}_default.yaml", Util::toLowerCase(attachSource.sourceType), Util::toLowerCase(tokens.back()));
    }(attachSourceTokens, attachSource);

    /// parse logical source name to attach (physical) source to
    attachSource.logicalSourceName = std::string(attachSourceTokens.at(attachSourceTokens.size() - 2));

    /// parse data ingestion type
    attachSource.testDataIngestionType = magic_enum::enum_cast<TestDataIngestionType>(attachSourceTokens.back()).value();
    switch (attachSource.testDataIngestionType)
    {
        case TestDataIngestionType::INLINE: {
            attachSource.tuples = {expectTuples(true)};
            break;
        }
        case TestDataIngestionType::FILE: {
            attachSource.fileDataPath = {expectFilePath()};
            break;
        }
    }

    return attachSource;
}

std::filesystem::path SystestParser::expectFilePath()
{
    ++currentLine;
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    if (const auto parsedFilePath = std::filesystem::path(lines.at(currentLine));
        std::filesystem::exists(parsedFilePath) and parsedFilePath.has_filename())
    {
        return parsedFilePath;
    }
    throw TestException("Attach source with FileData must be followed by valid file path, but got: {}", lines.at(currentLine));
}
SystestParser::ResultTuples SystestParser::expectTuples(const bool ignoreFirst)
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    std::vector<std::string> tuples;
    /// skip the result line `----`
    if (currentLine < lines.size() && (lines[currentLine] == ResultDelimiter || ignoreFirst))
    {
        currentLine++;
    }
    /// read the tuples until we encounter an empty line
    while (currentLine < lines.size() && !lines[currentLine].empty())
    {
        tuples.push_back(lines[currentLine]);
        currentLine++;
    }
    return tuples;
}

std::string SystestParser::expectQuery()
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    std::string queryString;
    bool firstLine = true;
    while (currentLine < lines.size())
    {
        if (!emptyOrComment(lines[currentLine]))
        {
            /// Query definition ends with result delimiter.
            if (lines[currentLine] == ResultDelimiter)
            {
                --currentLine;
                break;
            }
            if (!firstLine)
            {
                queryString += "\n";
            }
            queryString += lines[currentLine];
            firstLine = false;
        }
        currentLine++;
    }
    INVARIANT(!queryString.empty(), "when expecting a query keyword the queryString should not be empty");
    return queryString;
}
}
