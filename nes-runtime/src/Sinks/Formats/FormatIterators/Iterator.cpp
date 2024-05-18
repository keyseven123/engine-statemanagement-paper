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

#include <Sinks/Formats/FormatIterators/Iterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
namespace NES {

std::string Iterator::dataJson() {
    NES_DEBUG("Executing dataJson");
    uint8_t* tuplePointer = &this->buffer.getBuffer<uint8_t>()[currentSeek];

    // Iterate over all fields in a tuple. Get field offsets from fieldOffsets array. Use fieldNames as keys and TupleBuffer
    // values as the corresponding values
    // Adding the first tuple before the loop avoids checking if last tuple is processed in order to omit "," after json value
    auto jsonObject = nlohmann::json{};
    try {
        for (uint32_t currentField = 0; currentField < fieldNames.size(); currentField++) {
            auto currentFieldOffset = fieldOffsets[currentField];
            auto currentFieldType = fieldTypes[currentField];
            auto fieldName = fieldNames[currentField];

            // If the current field is TEXT, read the uint32_t index to the child buffer, and pass the child buffer ptr.
            auto dataTypePtr = (currentFieldType->isTextType())
                    ? this->buffer.loadChildBuffer(*reinterpret_cast<uint32_t*>(tuplePointer + currentFieldOffset)).getBuffer()
                    : tuplePointer + currentFieldOffset;
            auto fieldValue = currentFieldType->convertRawToStringWithoutFill(dataTypePtr);
            jsonObject[fieldName] = fieldValue;
        }
    } catch (nlohmann::json::exception& jsonException) {
        NES_ERROR("FormatIterator::dataJson: Error when creating JSON object from TupleBuffer values {}", jsonException.what());
        return "";
    }
    return jsonObject.dump();
}

}// namespace NES
