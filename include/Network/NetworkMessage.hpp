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

#ifndef NES_NETWORKMESSAGE_HPP
#define NES_NETWORKMESSAGE_HPP

#include <Network/ChannelId.hpp>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace NES {
namespace Network {
namespace Messages {

/**
 * @brief This magic number is written as first 64bits of every NES network message.
 * We use this as a checksum to validate that we are not transferring garbage data.
 */
using nes_magic_number_t = uint64_t;
static constexpr nes_magic_number_t NES_NETWORK_MAGIC_NUMBER = 0xBADC0FFEE;

enum MessageType {
    /// message type that the client uses to announce itself to the server
    kClientAnnouncement,
    /// message type that the servers uses to reply to the client regarding the availability
    /// of a partition
    kServerReady,
    /// message type of a data buffer
    kDataBuffer,
    /// type of a message that contains an error
    kErrorMessage,
    /// type of a message that marks a stream subpartition as finished, i.e., no more records are expected
    kEndOfStream,
    /// message type of an event buffer
    kEventBuffer,
};

/// this enum defines the errors that can occur in the network stack logic
enum ErrorType : uint8_t { kNoError, kPartitionNotRegisteredError, kUnknownError, kUnknownPartition };

class MessageHeader {
  public:
    explicit MessageHeader(MessageType msgType, uint32_t msgLength)
        : magicNumber(NES_NETWORK_MAGIC_NUMBER), msgType(msgType), msgLength(msgLength) {}

    nes_magic_number_t getMagicNumber() const { return magicNumber; }

    MessageType getMsgType() const { return msgType; }

    uint32_t getMsgLength() const { return msgLength; }

  private:
    const nes_magic_number_t magicNumber;
    const MessageType msgType;
    const uint32_t msgLength;
};

class ExchangeMessage {
  public:
    explicit ExchangeMessage(ChannelId channelId) : channelId(channelId) {}

    const ChannelId& getChannelId() const { return channelId; }

  private:
    const ChannelId channelId;
};

class ClientAnnounceMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = kClientAnnouncement;

    explicit ClientAnnounceMessage(ChannelId channelId) : ExchangeMessage(channelId) {}
};

class ServerReadyMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = kServerReady;

    explicit ServerReadyMessage(ChannelId channelId, ErrorType withError = kNoError)
        : ExchangeMessage(channelId), withError(withError) {}

    /**
     * @brief check if the message does not contain any error
     * @return true if no error was raised
     */
    bool isOk() const { return withError == kNoError; }

    /**
     * @brief this checks if the message contains a PartitionNotRegisteredError
     * @return true if the message contains a PartitionNotRegisteredError
     */
    bool isPartitionNotFound() const { return withError == kPartitionNotRegisteredError; }

    /**
     * @return the underlying error
     */
    ErrorType getErrorType() const { return withError; }

  private:
    const ErrorType withError{kNoError};
};

class EndOfStreamMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = kEndOfStream;

    explicit EndOfStreamMessage(ChannelId channelId, bool graceful = true) : ExchangeMessage(channelId), graceful(graceful) {}

    bool isGraceful() const { return graceful; }

  private:
    bool graceful;
};

class ErrorMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = kErrorMessage;

    explicit ErrorMessage(ChannelId channelId, ErrorType error) : ExchangeMessage(channelId), error(error){};

    ErrorType getErrorType() const { return error; }

    std::string getErrorTypeAsString() const {
        if (error == ErrorType::kPartitionNotRegisteredError) {
            return "PartitionNotRegisteredError";
        } else {
            return "UnknownError";
        }
    }

  private:
    const ErrorType error;
};

class DataBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = kDataBuffer;

    explicit DataBufferMessage(uint32_t payloadSize, uint32_t numOfRecords, uint64_t originId, uint64_t watermark,
                               uint64_t creationTimestamp)
        : payloadSize(payloadSize), numOfRecords(numOfRecords), originId(originId), watermark(watermark),
          creationTimestamp(creationTimestamp) {}

    /**
     * @brief get the payloadSize of the BufferMessage
     * @return the payloadSize
     */
    const uint32_t getPayloadSize() const { return payloadSize; }

    /**
     * @brief get the number of records within the current BufferMessage
     * @return the number of records
     */
    const uint32_t getNumOfRecords() const { return numOfRecords; }

    /**
      * @brief get the origin id within the current BufferMessage
      * @return the origin id
    */
    const uint64_t getOriginId() const { return originId; }

    /**
     * @brief get the watermark of the current BufferMessage
    * @return the watermark
    */
    const uint64_t getWatermark() const { return watermark; }

    /**
     * @brief get the createion ts of the current BufferMessage
     * @return ts
    */
    const uint64_t getCreationTimestamp() const { return creationTimestamp; }

  private:
    const uint32_t payloadSize;
    const uint32_t numOfRecords;
    const uint64_t originId;
    const uint64_t watermark;
    const uint64_t creationTimestamp;
};

class NesNetworkError : public std::runtime_error {
  public:
    explicit NesNetworkError(ErrorMessage& msg) : std::runtime_error(msg.getErrorTypeAsString()), msg(msg) {}

    const ErrorMessage& getErrorMessage() const { return msg; }

  private:
    const ErrorMessage msg;
};

}// namespace Messages
}// namespace Network
}// namespace NES

#endif//NES_NETWORKMESSAGE_HPP
