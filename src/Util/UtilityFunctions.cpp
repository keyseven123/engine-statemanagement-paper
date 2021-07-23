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

#include <API/Query.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <unistd.h>
#include <utility>

namespace NES {

std::string UtilityFunctions::escapeJson(const std::string& s) {
    std::ostringstream o;
    for (char c : s) {
        if (c == '"' || c == '\\' || ('\x00' <= c && c <= '\x1f')) {
            o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int) c;
        } else {
            o << c;
        }
    }
    return o.str();
}

std::string UtilityFunctions::trim(std::string s) {
    auto not_space = [](char c) {
        return isspace(c) == 0;
    };
    // trim left
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    // trim right
    s.erase(find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
    return s;
}

QueryPtr UtilityFunctions::createQueryFromCodeString(const std::string& queryCodeSnippet) {

    if (queryCodeSnippet.find("Stream(") != std::string::npos || queryCodeSnippet.find("Schema::create()") != std::string::npos) {
        NES_ERROR("QueryCatalog: queries are not allowed to specify schemas anymore.");
        throw InvalidQueryException("Queries are not allowed to define schemas anymore");
    }

    bool pattern = queryCodeSnippet.find("Pattern::") != std::string::npos;
    bool merge = queryCodeSnippet.find(".unionWith") != std::string::npos;
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Query.hpp>" << std::endl;
        code << "#include <API/Pattern.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>" << std::endl;
        code << "#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>" << std::endl;
        code << "#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "using namespace NES::API;" << std::endl;
        code << "namespace NES{" << std::endl;
        code << "Query createQuery(){" << std::endl;

        std::string streamName = queryCodeSnippet.substr(queryCodeSnippet.find("::from("));
        streamName = streamName.substr(7, streamName.find(')') - 7);
        NES_DEBUG(" UtilityFunctions: stream name = " << streamName);

        std::string newQuery = queryCodeSnippet;

        if (merge) {//if contains merge
            auto pos1 = queryCodeSnippet.find("unionWith(");
            uint64_t closingLoc = findSubQueryTermination(pos1, queryCodeSnippet);
            std::string subquery = queryCodeSnippet.substr(pos1 + 10, closingLoc - pos1 - 10);
            NES_DEBUG("UtilityFunctions: subquery = " << subquery);
            code << "auto subQuery = " << subquery << ";" << std::endl;
            newQuery.replace(pos1, closingLoc - pos1, "unionWith(&subQuery");
            NES_DEBUG("UtilityFunctions: newQuery = " << newQuery);
        }

        // add return statement in front of input query/pattern
        //if pattern
        if (pattern) {
            findAndReplaceAll(newQuery, "Pattern::from", "return Pattern::from");
        } else {// if Query
            newQuery = replaceFirst(newQuery, "Query::from", "return Query::from");
        }

        NES_DEBUG("UtilityFunctions: parsed query = " << newQuery);
        code << newQuery << std::endl;
        code << "}" << std::endl;
        code << "}" << std::endl;
        NES_DEBUG("UtilityFunctions: query code \n" << code.str());
        Compiler compiler;
        CompiledCodePtr compiled_code = compiler.compile(code.str());
        if (!code) {
            NES_ERROR("Compilation of query code failed! Code: " << code.str());
        }

        using CreateQueryFunctionPtr = Query (*)();
        auto func = compiled_code->getFunctionPointer<CreateQueryFunctionPtr>("_ZN3NES11createQueryEv");
        if (!func) {
            NES_ERROR("UtilityFunctions: Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Query query((*func)());

        return std::make_shared<Query>(query);
    } catch (std::exception& exc) {
        NES_ERROR("UtilityFunctions: Failed to create the query from input code string: " << queryCodeSnippet << exc.what());
        throw;
    } catch (...) {
        NES_ERROR("UtilityFunctions: Failed to create the query from input code string: " << queryCodeSnippet);
        throw "Failed to create the query from input code string";
    }
}

uint64_t UtilityFunctions::findSubQueryTermination(uint64_t startOfUnionWith, const std::string& queryCodeSnippet) {
    uint64_t closingLoc = 0;
    uint64_t parenthesisCount = 0;
    //Iterate over the code string and identify the last location where the parenthesis are getting balanced
    for (uint64_t i = startOfUnionWith + 9; i < queryCodeSnippet.size(); i++) {
        if (queryCodeSnippet[i] == '(') {//When found open parenthesis, increment the parenthesis count
            parenthesisCount++;
        } else if (queryCodeSnippet[i] == ')') {//When found open parenthesis, decrement the parenthesis count
            parenthesisCount--;
        }

        if (parenthesisCount == 0) {//When found parenthesis count as zero then return the position
            closingLoc = i;
            break;
        }
    }
    //return the location where the parenthesis are getting balanced
    return closingLoc;
}

SchemaPtr UtilityFunctions::createSchemaFromCode(const std::string& queryCodeSnippet) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;

        code << "Schema createSchema(){" << std::endl;
        code << "return " << queryCodeSnippet << ";";
        code << "}" << std::endl;
        code << "}" << std::endl;
        Compiler compiler;
        NES_DEBUG("generated code=" << code.str());
        CompiledCodePtr compiled_code = compiler.compile(code.str());
        if (!code) {
            NES_ERROR("Compilation of schema code failed! Code: " << code.str());
        }

        using CreateSchemaFunctionPtr = Schema (*)();
        auto func = compiled_code->getFunctionPointer<CreateSchemaFunctionPtr>(
            "_ZN3NES12createSchemaEv");// was   _ZN5iotdb12createSchemaEv
        if (!func) {
            NES_ERROR("Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Schema query((*func)());
        return std::make_shared<Schema>(query);

    } catch (std::exception& exc) {
        NES_ERROR("Failed to create the query from input code string: " << queryCodeSnippet);
        throw;
    } catch (...) {
        NES_ERROR("Failed to create the query from input code string: " << queryCodeSnippet);
        throw "Failed to create the query from input code string";
    }
}

std::string UtilityFunctions::generateIdString() {
    static std::random_device dev;
    static std::mt19937 rng(dev());

    std::uniform_int_distribution<int> dist(0, 15);

    const char* v = "0123456789abcdef";
    const bool dash[] =
        {false, false, false, false, true, false, true, false, true, false, true, false, false, false, false, false};

    std::string res;
    for (bool i : dash) {
        if (i) {
            res += "-";
        }
        res += v[dist(rng)];
        res += v[dist(rng)];
    }
    NES_DEBUG("UtilityFunctions: generateIdString: " + res);
    return res;
}

std::uint64_t UtilityFunctions::generateIdInt() {
    std::string linkID_string = UtilityFunctions::generateIdString();
    NES_DEBUG("UtilityFunctions: generateIdInt: create a new string_id=" << linkID_string);
    std::hash<std::string> hash_fn;
    return hash_fn(linkID_string);
}

std::string
UtilityFunctions::getFirstStringBetweenTwoDelimiters(const std::string& input, const std::string& s1, const std::string& s2) {
    unsigned firstDelimPos = input.find(s1);
    unsigned endPosOfFirstDelim = firstDelimPos + s1.length();

    unsigned lastDelimPos = input.find_first_of(s2, endPosOfFirstDelim);

    return input.substr(endPosOfFirstDelim, lastDelimPos - endPosOfFirstDelim);
}

std::vector<std::string> UtilityFunctions::splitWithStringDelimiter(std::string& s, const std::string& delim) {
    std::string copy = s;
    size_t pos = 0;
    std::vector<std::string> elems;
    while ((pos = copy.find(delim)) != std::string::npos) {
        elems.push_back(copy.substr(0, pos));
        copy.erase(0, pos + delim.length());
    }
    if (!copy.substr(0, pos).empty()) {
        elems.push_back(copy.substr(0, pos));
    }

    return elems;
}

std::string UtilityFunctions::printTupleBufferAsText(NodeEngine::TupleBuffer& buffer) {
    std::stringstream ss;
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        ss << buffer.getBuffer<char>()[i];
    }
    return ss.str();
}

std::string UtilityFunctions::prettyPrintTupleBuffer(NodeEngine::TupleBuffer& buffer, const SchemaPtr& schema) {
    if (!buffer.isValid()) {
        return "INVALID_BUFFER_PTR";
    }
    std::stringstream str;
    std::vector<uint32_t> offsets;
    std::vector<PhysicalTypePtr> types;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        auto physicalType = physicalDataTypeFactory.getPhysicalType(schema->get(i)->getDataType());
        offsets.push_back(physicalType->size());
        types.push_back(physicalType);
        NES_TRACE("CodeGenerator: " + std::string("Field Size ") + schema->get(i)->toString() + std::string(": ")
                  + std::to_string(physicalType->size()));
    }

    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < offsets.size(); ++i) {
        uint32_t val = offsets[i];
        offsets[i] = prefix_sum;
        prefix_sum += val;
        NES_TRACE("CodeGenerator: " + std::string("Prefix SumAggregationDescriptor: ") + schema->get(i)->toString()
                  + std::string(": ") + std::to_string(offsets[i]));
    }

    str << "+----------------------------------------------------+" << std::endl;
    str << "|";
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        str << schema->get(i)->getName() << ":"
            << physicalDataTypeFactory.getPhysicalType(schema->get(i)->getDataType())->toString() << "|";
    }
    str << std::endl;
    str << "+----------------------------------------------------+" << std::endl;

    auto* buf = buffer.getBuffer<char>();
    for (uint32_t i = 0; i < buffer.getNumberOfTuples() * schema->getSchemaSizeInBytes(); i += schema->getSchemaSizeInBytes()) {
        str << "|";
        for (uint32_t s = 0; s < offsets.size(); ++s) {
            void* value = &buf[i + offsets[s]];
            std::string tmp = types[s]->convertRawToString(value);
            str << tmp << "|";
        }
        str << std::endl;
    }
    str << "+----------------------------------------------------+";
    return str.str();
}

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string UtilityFunctions::printTupleBufferAsCSV(NodeEngine::TupleBuffer& tbuffer, const SchemaPtr& schema) {
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++) {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto ptr = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
            auto fieldSize = physicalType->size();
            auto str = physicalType->convertRawToString(buffer + offset + i * schema->getSchemaSizeInBytes());
            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

void UtilityFunctions::findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
    // Get the first occurrence
    uint64_t pos = data.find(toSearch);
    // Repeat till end is reached
    while (pos != std::string::npos) {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

std::string UtilityFunctions::replaceFirst(std::string origin, const std::string& search, const std::string& replace) {
    if (origin.find(search) != std::string::npos) {
        return origin.replace(origin.find(search), search.size(), replace);
    }
    return origin;
}

std::string UtilityFunctions::toCSVString(const SchemaPtr& schema) {
    std::stringstream ss;
    for (auto& f : schema->fields) {
        ss << f->toString() << ",";
    }
    ss.seekp(-1, std::ios_base::end);
    ss << std::endl;
    return ss.str();
}

bool UtilityFunctions::endsWith(const std::string& fullString, const std::string& ending) {
    if (fullString.length() >= ending.length()) {
        // get the start of the ending index of the full string and compare with the ending string
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    }// if full string is smaller than the ending automatically return false
    return false;
}

bool UtilityFunctions::startsWith(const std::string& fullString, const std::string& ending) {
    return (fullString.rfind(ending, 0) == 0);
}

OperatorId UtilityFunctions::getNextOperatorId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t UtilityFunctions::getNextPipelineId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t UtilityFunctions::getNextTopologyNodeId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t UtilityFunctions::getNextNodeEngineId() {
    static std::atomic_uint64_t id = time(nullptr) ^ getpid();
    return ++id;
}

uint64_t UtilityFunctions::getNextTaskId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

web::json::value UtilityFunctions::getTopologyAsJson(TopologyNodePtr root) {
    NES_INFO("UtilityFunctions: getting topology as JSON");

    web::json::value topologyJson{};

    std::deque<TopologyNodePtr> parentToAdd{std::move(root)};
    std::deque<TopologyNodePtr> childToAdd;

    std::vector<web::json::value> nodes = {};
    std::vector<web::json::value> edges = {};

    while (!parentToAdd.empty()) {
        // Current topology node to add to the JSON
        TopologyNodePtr currentNode = parentToAdd.front();
        web::json::value currentNodeJsonValue{};

        parentToAdd.pop_front();
        // Add properties for current topology node
        currentNodeJsonValue["id"] = web::json::value::number(currentNode->getId());
        currentNodeJsonValue["available_resources"] = web::json::value::number(currentNode->getAvailableResources());
        currentNodeJsonValue["ip_address"] = web::json::value::string(currentNode->getIpAddress());

        for (const auto& child : currentNode->getChildren()) {
            // Add edge information for current topology node
            web::json::value currentEdgeJsonValue{};
            currentEdgeJsonValue["source"] = web::json::value::number(child->as<TopologyNode>()->getId());
            currentEdgeJsonValue["target"] = web::json::value::number(currentNode->getId());
            edges.push_back(currentEdgeJsonValue);

            childToAdd.push_back(child->as<TopologyNode>());
        }

        if (parentToAdd.empty()) {
            parentToAdd.insert(parentToAdd.end(), childToAdd.begin(), childToAdd.end());
            childToAdd.clear();
        }

        nodes.push_back(currentNodeJsonValue);
    }
    NES_INFO("UtilityFunctions: no more topology node to add");

    // add `nodes` and `edges` JSON array to the final JSON result
    topologyJson["nodes"] = web::json::value::array(nodes);
    topologyJson["edges"] = web::json::value::array(edges);
    return topologyJson;
}

bool UtilityFunctions::assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan,
                                                        std::vector<std::map<std::string, std::any>> properties) {
    // count the number of operators in the query
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    size_t numOperators = queryPlanIterator.snapshot().size();
    ;

    // check if we supply operator properties for all operators
    if (numOperators != properties.size()) {
        NES_ERROR("UtilityFunctions::assignPropertiesToQueryOperators: the number of properties does not match the number of "
                  "operators. The query plan is:\n"
                  << queryPlan->toString());
        return false;
    }

    // prepare the query plan iterator
    auto propertyIterator = properties.begin();

    // iterate over all operators in the query
    for (auto&& node : queryPlanIterator) {
        for (auto const& [key, val] : *propertyIterator) {
            // add the current property to the current operator
            node->as<LogicalOperatorNode>()->addProperty(key, val);
        }
        ++propertyIterator;
    }

    return true;
}

void UtilityFunctions::fillEnvelopeBuffer(char* buffer, bool isSchema, uint64_t numberOfTuples, uint64_t watermark) {
    int offset = 0;

    std::memcpy(buffer, &isSchema, sizeof(bool));
    offset += sizeof(bool);

    std::memcpy(buffer + offset, &numberOfTuples, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(buffer + offset, &watermark, sizeof(uint64_t));
}

}// namespace NES