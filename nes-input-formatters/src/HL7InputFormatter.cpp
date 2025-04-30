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
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <memory.h>
#include <InputFormatters/AsyncInputFormatterTask.hpp>
#include <AsyncInputFormatterRegistry.hpp>
#include <FieldOffsetsIterator.hpp>
#include <HL7InputFormatter.hpp>
#include <SyncInputFormatterRegistry.hpp>

namespace NES::InputFormatters
{

/// It can be assumed that HL7 v2 escape chars consist of only 1 char
size_t findFirstIndexOfSymbolWithoutEscape(
    std::string_view str, const std::string_view symbol, const std::string_view escapeCharacter, const size_t startIndex)
{
    size_t pos = startIndex;
    size_t index = str.find(symbol, pos);
    while (index != std::string_view::npos)
    {
        if (index == 0 || str[index - 1] != escapeCharacter[0])
        {
            break;
        }
        pos = index + 1;
        index = str.find(symbol, pos);
    }
    return index;
}

void indexSegment(
    std::string_view segmentString,
    std::string_view compositeDelim,
    std::string_view subCompositeDelim,
    std::string_view escapeCharacter,
    std::string_view subSubCompositeDelim,
    FieldOffsetIterator& offsets,
    FieldOffsetsType startOfSegment,
    FieldOffsetsType endOfSegment)
{
    size_t currentOffset = 0;
    bool hasAnotherValue = true;
    while (hasAnotherValue)
    {
        offsets.write(currentOffset + startOfSegment);
        ++offsets;
        /// Find next nearest possible symbol
        const size_t nextCompositeDelim
            = findFirstIndexOfSymbolWithoutEscape(segmentString, compositeDelim, escapeCharacter, currentOffset);
        const size_t nextSubCompositeDelim
            = findFirstIndexOfSymbolWithoutEscape(segmentString, subCompositeDelim, escapeCharacter, currentOffset);
        const size_t nextSubSubCompositeDelim
            = findFirstIndexOfSymbolWithoutEscape(segmentString, subSubCompositeDelim, escapeCharacter, currentOffset);
        /// If we start handeling NULL values, we need to check if the next delimiter and the currentOffset are at the same index.
        /// Furthermore, we need to check if we have multiple invisible NULL values, when missing composites are not captured by delimiters next to each other (usually happens at the end of the segment, when all remaining values are NULL)
        /// In this case, having access to the schema will be necessary to add the correct amount of NULL-values
        /// The size of every possible delimiter of composites is 1
        currentOffset = std::min({nextCompositeDelim, nextSubCompositeDelim, nextSubSubCompositeDelim}) + 1;
        hasAnotherValue = currentOffset != std::string_view::npos + 1;
    }
    /// We also add the offset to the \r behind the segment. This will mark the place where the segment ends and mark the start of the next segment identifier (-1).
    /// The InputFormatterTask will have to jump forward 1 char when parsing the value after this offset to ignore the \r. (CSV already does something similar)
    offsets.write(endOfSegment);
    ++offsets;
}

/// Think of "indexMessage" in this case
/// Gets the messages delimiter symbols and writes the offsets segment per segment by calling the indexSegment function
void HL7InputFormatter::indexSpanningTuple(
    std::string_view tuple,
    std::string_view segmentDelimiter,
    FieldOffsetIterator& offsets,
    FieldOffsetsType startIdxOfCurrentMessage,
    FieldOffsetsType endIndxOfCurrentMessage)
{
    /// Adding the MSH identifier offset manually
    offsets.write(startIdxOfCurrentMessage);
    ++offsets;
    /// Since the first segment is the message header segment (MSH), we get the 5 important delimiters from the first 5 characters after the "MSH", being |^~\& per default
    const auto compositeDelim = std::string_view(tuple.begin() + 3, 1);
    const auto subCompositeDelim = std::string_view(tuple.begin() + 4, 1);
    /// This will be relevant in case we build an extra OffsetIterator for repeating components
    auto repetitionDelim = std::string_view(tuple.begin() + 5, 1);
    /// In case that we go over the older, stored message, we can't be sure that the relevant escape char is currentEscapeChar
    const auto escapeChar = std::string_view(tuple.begin() + 6, 1);
    const auto subSubCompositeDelim = std::string_view(tuple.begin() + 7, 1);
    /// Store the delimiter-composite-offsets manually since it will mess with the search for delimiter symbols otherwise
    /// This is also the only instance where 2 composites are not seperated via a composite seperator is treated as the value itself here.
    /// This needs to be accounted for in the formatter task.
    /// For reference, see https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/MSH
    offsets.write(startIdxOfCurrentMessage + 3);
    ++offsets;
    offsets.write(startIdxOfCurrentMessage + 4);
    ++offsets;
    /// The first value after the delimiter symbols starts at this exact index
    size_t currentSegmentStart = INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE;
    size_t currentSegmentEnd = findFirstIndexOfSymbolWithoutEscape(tuple, segmentDelimiter, escapeChar, currentSegmentStart);
    while (currentSegmentEnd != std::string_view::npos)
    {
        /// Currently we ignore the the character segment identifier at the start of each segment e.g. MSH or PID. Later, we need to use these to determine the current nested datatype and whether a group of segments repeated
        auto currentSegment = std::string_view(tuple.begin() + currentSegmentStart, currentSegmentEnd - currentSegmentStart);
        indexSegment(
            currentSegment,
            compositeDelim,
            subCompositeDelim,
            escapeChar,
            subSubCompositeDelim,
            offsets,
            startIdxOfCurrentMessage + currentSegmentStart,
            startIdxOfCurrentMessage + currentSegmentEnd);
        /// Already jump to the value after the segment identifier, since its offset already was added at the end of indexSegment.
        currentSegmentStart = currentSegmentEnd + segmentDelimiter.size() + 4;
        currentSegmentEnd = findFirstIndexOfSymbolWithoutEscape(tuple, segmentDelimiter, escapeChar, currentSegmentStart);
    }
    /// The following sugment delimiter of the very last segment of the message is not included in the tuple, we still need to parse it though
    auto currentSegment
        = std::string_view(tuple.begin() + currentSegmentStart, endIndxOfCurrentMessage - (currentSegmentStart + startIdxOfCurrentMessage));
    indexSegment(
        currentSegment,
        compositeDelim,
        subCompositeDelim,
        escapeChar,
        subSubCompositeDelim,
        offsets,
        startIdxOfCurrentMessage + currentSegmentStart,
        endIndxOfCurrentMessage);
}

/// In the case of HL7 v2, we probably want to use \r as message delimiter as well as segment delimiter.
/// The reason why \rMSH would not work as a message delimiter is that we assume, that the first ever message to arrive arrives without the message delimiter in front of it.
/// However, the first message will arrive with the MSH identifier (although without the \r in front of it). This would mess with the processLeadingSpanningTuple method.
///
/// Currently, we treat the identifiers of the segments, e.g. MSH or PID as values of the segment. However, this is NOT the case according to the official HL7 standard.
/// In order to tell the FormatterTask to skip over these parts, we would have to change the OffsetIterator, which HL7 currently shares with CSV.
/// Once HL7 gets a different OffsetIterator (perhaps its own one), we can stop treating segment identifiers as values.
void HL7InputFormatter::indexRawBuffer(
    const char* rawTB,
    uint32_t numberOfBytesInRawTB,
    FieldOffsetIterator& fieldOffsets,
    std::string_view messageDelimiter,
    std::string_view segmentDelimiter,
    FieldOffsetsType& offsetOfFirstMessageDelimiter,
    FieldOffsetsType& offsetOfLastMessageDelimiter)
{
    std::string tempMessageDelim(messageDelimiter);
    tempMessageDelim.append("MSH");
    const std::string_view trueMessageDelimiter(tempMessageDelim);
    const auto bufferString = std::string_view(rawTB, numberOfBytesInRawTB);
    /// Finding the first instance of segmentDelim + MSH since we want to start parsing at a whole new message
    /// currentEscapeChar is the most recently scanned escape character. (This only works if the buffers come in the right order).
    offsetOfFirstMessageDelimiter
        = static_cast<FieldOffsetsType>(findFirstIndexOfSymbolWithoutEscape(bufferString, trueMessageDelimiter, currentEscapeChar, 0));
    /// startIndexOfCurrentMessage is now the index the "MSH" identifier
    size_t startIndexOfCurrentMessage = offsetOfFirstMessageDelimiter + messageDelimiter.size();
    if (bufferString.size() > startIndexOfCurrentMessage + INDEX_OF_ESCAPE_CHAR)
    {
        /// The buffer still contains the escape char of the current message, which should be exactly 6 positions after the startIndex of the current Message
        currentEscapeChar = std::string_view(bufferString.begin() + startIndexOfCurrentMessage + INDEX_OF_ESCAPE_CHAR, 1);
    }
    /// This step might lead to not finding the end of a message, which also marks the end of the raw buffer as a whole. This especially has to be considered when parsing the staged buffers.
    size_t endIndexOfCurrentMessage
        = findFirstIndexOfSymbolWithoutEscape(bufferString, trueMessageDelimiter, currentEscapeChar, startIndexOfCurrentMessage);
    while (endIndexOfCurrentMessage != std::string_view::npos)
    {
        auto currentMessage
            = std::string_view(bufferString.begin() + startIndexOfCurrentMessage, endIndexOfCurrentMessage - startIndexOfCurrentMessage);
        indexSpanningTuple(currentMessage, segmentDelimiter, fieldOffsets, startIndexOfCurrentMessage, endIndexOfCurrentMessage);
        startIndexOfCurrentMessage = endIndexOfCurrentMessage + messageDelimiter.size();
        if (bufferString.size() > startIndexOfCurrentMessage + INDEX_OF_ESCAPE_CHAR)
        {
            currentEscapeChar = std::string_view(bufferString.begin() + startIndexOfCurrentMessage + INDEX_OF_ESCAPE_CHAR, 1);
        }
        endIndexOfCurrentMessage
            = findFirstIndexOfSymbolWithoutEscape(bufferString, trueMessageDelimiter, currentEscapeChar, startIndexOfCurrentMessage);
    }
    offsetOfLastMessageDelimiter = startIndexOfCurrentMessage - messageDelimiter.size();
}

std::ostream& HL7InputFormatter::toString(std::ostream& os) const
{
    os << "HL7InputFormatter: {}\n";
    return os;
}

std::unique_ptr<AsyncInputFormatterRegistryReturnType>
AsyncInputFormatterGeneratedRegistrar::RegisterHL7AsyncInputFormatter(AsyncInputFormatterRegistryArguments)
{
    return std::make_unique<HL7InputFormatter>();
}

std::unique_ptr<SyncInputFormatterRegistryReturnType>
SyncInputFormatterGeneratedRegistrar::RegisterHL7SyncInputFormatter(SyncInputFormatterRegistryArguments)
{
    return std::make_unique<HL7InputFormatter>();
}

}
