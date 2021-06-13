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
#include <Plans/Query/QuerySubPlanId.hpp>
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
    /// message type of communicating reconfiguration events
    kQueryReconfiguration,
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

class QueryReconfigurationMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = kQueryReconfiguration;

    explicit QueryReconfigurationMessage(ChannelId channelId, std::vector<QuerySubPlanId> querySubPlansToStart,
                                         std::map<QuerySubPlanId, QuerySubPlanId> querySubPlansIdToReplace,
                                         std::vector<QuerySubPlanId> querySubPlansToStop)
        : ExchangeMessage(channelId), querySubPlansToStart(querySubPlansToStart),
          querySubPlansIdToReplace(querySubPlansIdToReplace), querySubPlansToStop(querySubPlansToStop) {}

    std::vector<QuerySubPlanId> getQuerySubPlansToStart() const { return querySubPlansToStart; }
    std::map<QuerySubPlanId, QuerySubPlanId> getQuerySubPlansIdToReplace() const { return querySubPlansIdToReplace; }
    std::vector<QuerySubPlanId> getQuerySubPlansToStop() const { return querySubPlansToStop; }

  private:
    std::vector<QuerySubPlanId> querySubPlansToStart;
    std::map<QuerySubPlanId, QuerySubPlanId> querySubPlansIdToReplace;
    std::vector<QuerySubPlanId> querySubPlansToStop;
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

    explicit inline DataBufferMessage(uint32_t payloadSize,
                                      uint32_t numOfRecords,
                                      uint64_t originId,
                                      uint64_t watermark,
                                      uint64_t creationTimestamp) noexcept
        : payloadSize(payloadSize), numOfRecords(numOfRecords), originId(originId), watermark(watermark),
          creationTimestamp(creationTimestamp) {}

    uint32_t const payloadSize;
    uint32_t const numOfRecords;
    uint64_t const originId;
    uint64_t const watermark;
    uint64_t const creationTimestamp;
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
