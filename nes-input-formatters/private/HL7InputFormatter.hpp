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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string_view>
#include <vector>
#include <InputFormatters/InputFormatter.hpp>

namespace NES::InputFormatters
{

class HL7InputFormatter final : public InputFormatter
{
    /// If we consider the message header identifier MSH to be at index 0
    static constexpr size_t INDEX_OF_ESCAPE_CHAR = 6;
    static constexpr size_t INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE = 9;

public:
    HL7InputFormatter() = default;
    ~HL7InputFormatter() override = default;

    HL7InputFormatter(const HL7InputFormatter&) = delete;
    HL7InputFormatter& operator=(const HL7InputFormatter&) = delete;
    HL7InputFormatter(HL7InputFormatter&&) = delete;
    HL7InputFormatter& operator=(HL7InputFormatter&&) = delete;

    void indexSpanningTuple(
        std::string_view tuple,
        std::string_view segmentDelimiter,
        FieldOffsetIterator& offsets,
        FieldOffsetsType startIdxOfCurrentMessage,
        FieldOffsetsType endIdxOfCurrentTuple) override;

    void indexRawBuffer(
        const char* rawTB,
        uint32_t numberOfBytesInRawTB,
        FieldOffsetIterator& fieldOffsets,
        std::string_view messageDelimiter,
        std::string_view segmentDelimiter,
        FieldOffsetsType& offsetOfFirstMessageDelimiter,
        FieldOffsetsType& offsetOfLastMessageDelimiter) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string_view currentEscapeChar = std::string_view("\\");
};

}
