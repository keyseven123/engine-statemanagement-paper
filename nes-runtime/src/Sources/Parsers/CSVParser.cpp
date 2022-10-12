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

#include "Sources/Parsers/CSVParser.hpp"
#include "API/Schema.hpp"
#include "Common/PhysicalTypes/BasicPhysicalType.hpp"
#include "Exceptions/RuntimeException.hpp"
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include "Util/Logger/Logger.hpp"
#include "Util/UtilityFunctions.hpp"
#include <string>

using namespace std::string_literals;
namespace NES {

CSVParser::CSVParser(uint64_t numberOfSchemaFields, std::vector<NES::PhysicalTypePtr> physicalTypes, std::string delimiter)
    : Parser(physicalTypes), numberOfSchemaFields(numberOfSchemaFields), physicalTypes(std::move(physicalTypes)),
      delimiter(std::move(delimiter)) {}

bool CSVParser::writeInputTupleToTupleBuffer(const std::string& csvInputLine,
                                             uint64_t tupleCount,
                                             Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                             const SchemaPtr& schema) {
    NES_TRACE("CSVParser::parseCSVLine: Current TupleCount: " << tupleCount);

    std::vector<std::string> values = NES::Util::splitWithStringDelimiter<std::string>(csvInputLine, delimiter);

    if (values.size() != schema->getSize()) {
        throw Exceptions::RuntimeException("CSVParser: The input line does not contain the right number of delited fiels."s
                                           + " Fields in schema: " + std::to_string(schema->getSize())
                                           + " Fields in line: " + std::to_string(values.size())
                                           + " Schema: " + schema->toString() + " Line: " + csvInputLine);
    }
    // iterate over fields of schema and cast string values to correct type
    for (uint64_t j = 0; j < numberOfSchemaFields; j++) {
        auto field = physicalTypes[j];
        NES_TRACE("Current value is: " << values[j]);
        writeFieldValueToTupleBuffer(values[j], j, tupleBuffer, schema, tupleCount);
    }
    return true;
}
}// namespace NES