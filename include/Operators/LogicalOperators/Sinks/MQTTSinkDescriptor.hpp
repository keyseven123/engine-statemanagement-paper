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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_

#ifdef ENABLE_MQTT_BUILD

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>

namespace NES {
/**
 * @brief Descriptor defining properties used for creating a physical MQTT sink
 */
class MQTTSinkDescriptor : public SinkDescriptor {
  public:
    /**
     * @brief Creates the MQTT sink description
     * @param address: address name of MQTT broker
     * @param clientId: client ID for MQTT client
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param msgDelay: time before next message is sent by client to broker
     * @param qualityOfService: either 'at most once' or 'at least once'
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @return descriptor for MQTT sink
     */
    static SinkDescriptorPtr create(const std::string address, const std::string clientId, const std::string topic,
                                    const std::string user, uint64_t maxBufferedMSGs, const MQTTSink::TimeUnits timeUnit,
                                    uint64_t msgDelay, MQTTSink::ServiceQualities qualityOfService, bool asynchronousClient);

    /**
     * @brief get address information from a MQTT sink client
     * @return address of MQTT broker
     */
    const std::string getAddress() const;

    /**
     * @brief get clientId information from a MQTT sink client
     * @return id used by client
     */
    const std::string getClientId() const;

    /**
     * @brief get topic information from a MQTT sink client
     * @return topic to which MQTT client sends messages
     */
    const std::string getTopic() const;

    /**
     * @brief get user name for a MQTT sink client
     * @return user name used by MQTT client
     */
    const std::string getUser() const;

    /**
     * @brief get the number of MSGs that can maximally be buffered (default is 60)
     * @return number of messages that can maximally be buffered
     */
    uint64_t getMaxBufferedMSGs();

    /**
     * @brief get the user chosen time unit (default is milliseconds)
     * @return time unit chosen for the message delay
     */
    const MQTTSink::TimeUnits getTimeUnit() const;

    /**
     * @brief get the user chosen delay between two sent messages (default is 500)
     * @return length of the message delay
     */
    uint64_t getMsgDelay();

    /**
     * @brief get the value for the current quality of service
     * @return quality of service value
     */
    const MQTTSink::ServiceQualities getQualityOfService() const;

    /**
     * @brief get bool that indicates whether the client is asynchronous or synchronous (default is true)
     * @return true if client is asynchronous, else false
     */
    bool getAsynchronousClient();

    std::string toString() override;
    bool equal(SinkDescriptorPtr other) override;

  private:
    /**
     * @brief Creates the MQTT sink
     * @param address: address name of MQTT broker
     * @param clientId: client ID for MQTT client
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param msgDelay: time before next message is sent by client to broker
     * @param qualityOfService: either 'at most once' or 'at least once'
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @return MQTT sink
     */
    explicit MQTTSinkDescriptor(const std::string address, const std::string clientId, const std::string topic,
                                const std::string user, uint64_t maxBufferedMSGs, const MQTTSink::TimeUnits timeUnit,
                                uint64_t msgDelay, const MQTTSink::ServiceQualities qualityOfService, bool asynchronousClient);

    std::string address;
    std::string clientId;
    std::string topic;
    std::string user;
    uint64_t maxBufferedMSGs;
    MQTTSink::TimeUnits timeUnit;
    uint64_t msgDelay;
    MQTTSink::ServiceQualities qualityOfService;
    bool asynchronousClient;
};

typedef std::shared_ptr<MQTTSinkDescriptor> MQTTSinkDescriptorPtr;

}// namespace NES
#endif

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
