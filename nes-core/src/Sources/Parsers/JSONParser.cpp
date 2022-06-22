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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <simdjson.h>
#include <string>
#include <utility>

namespace NES {

JSONParser::JSONParser(uint64_t numberOfSchemaFields,
                       std::vector<std::string> schemaKeys,
                       std::vector<NES::PhysicalTypePtr> physicalTypes)
    : Parser(physicalTypes), numberOfSchemaFields(numberOfSchemaFields), schemaKeys(std::move(schemaKeys)),
      physicalTypes(std::move(physicalTypes)) {}

JSONParser::JSONParser(std::vector<PhysicalTypePtr> physical_types) : Parser(physical_types){};

bool JSONParser::writeInputTupleToTupleBuffer(const std::string& jsonTuple,
                                              uint64_t tupleCount,
                                              Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                              const SchemaPtr& schema) {
    NES_TRACE("JSONParser::parseJSONMessage: Current TupleCount: " << tupleCount);

    std::vector<std::string> helperToken;

    // extract values as strings from MQTT message - should be improved with JSON library
    auto parsedJSONObject = web::json::value::parse(jsonTuple);

    // iterate over fields of schema and cast string values to correct type
    std::basic_string<char> jsonValue;
    for (uint64_t fieldIndex = 0; fieldIndex < numberOfSchemaFields; fieldIndex++) {
        auto field = physicalTypes[fieldIndex];
        try {
            //serialize() is called to get the web::json::value as a string. This is done for 2 reasons:
            // 1. to keep 'Parser.cpp' independent of cpprest (no need to deal with 'web::json::value' object)
            // 2. to have a single place for NESBasicPhysicalType conversion (could change this)
            jsonValue = parsedJSONObject.at(schemaKeys[fieldIndex]).serialize();
        } catch (web::json::json_exception& jsonException) {
            NES_ERROR("JSONParser::writeInputTupleToTupleBuffer: Error when parsing jsonTuple: " << jsonException.what());
            return false;
        }

        writeFieldValueToTupleBuffer(jsonValue, fieldIndex, tupleBuffer, true, schema, tupleCount);
    }
    return true;
}

bool JSONParser::writeInputTupleToTupleBuffer(simdjson::ondemand::document_reference doc,
                                               uint64_t tupleCount,
                                               Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                               const SchemaPtr& schema) {
    // TODO
    /*
     * for field in schema
     *      auto val = doc.next()
     *      writeIntoTupleBuffer<field.type>(val)
     */
    return true;
}

}// namespace NES