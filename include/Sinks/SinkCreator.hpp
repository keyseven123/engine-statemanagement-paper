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

#ifndef INCLUDE_SOURCESINK_SINKCREATOR_HPP_
#define INCLUDE_SOURCESINK_SINKCREATOR_HPP_
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Runtime/NodeEngine.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#endif
#ifdef ENABLE_MQTT_BUILD
#include <Sinks/Mediums/MQTTSink.hpp>
#endif

namespace NES {
/**
 * @brief create test sink
 * @Note this method is currently not implemented
 */
DataSinkPtr createTestSink();

/**
 * @brief create a csv test sink without a schema and append to existing file
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */

DataSinkPtr createCSVFileSink(OperatorId logicalOperatorId,
                              const SchemaPtr& schema,
                              QuerySubPlanId parentPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              const std::string& filePath,
                              bool append);

/**
 * @brief create a binary test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
DataSinkPtr createTextFileSink(OperatorId logicalOperatorId,
                               const SchemaPtr& schema,
                               QuerySubPlanId parentPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               const std::string& filePath,
                               bool append);

/**
 * @brief create a binary test sink with a schema into the nes
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
DataSinkPtr createBinaryNESFileSink(OperatorId logicalOperatorId,
                                    const SchemaPtr& schema,
                                    QuerySubPlanId parentPlanId,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    const std::string& filePath,
                                    bool append);

/**
 * @brief create a JSON test sink with a schema int
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
DataSinkPtr createJSONFileSink(OperatorId logicalOperatorId,
                               const SchemaPtr& schema,
                               QuerySubPlanId parentPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               const std::string& filePath,
                               bool append);

/**
 * @brief create a ZMQ test sink with a schema and Text format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @param internal refers to the usage of this zmq sink as a fwd operator such that we dont have to send the schema, only the data
 * @return a data sink pointer
 */
DataSinkPtr createTextZmqSink(OperatorId logicalOperatorId,
                              const SchemaPtr& schema,
                              QuerySubPlanId parentPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              const std::string& host,
                              uint16_t port);
#ifdef ENABLE_OPC_BUILD
/**
 * @brief create a OPC test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param url to OPC server as string
 * @param nodeId to save data in
 * @param user name as string to log in to the OPC server
 * @param password as string to log in to the OPC server
 * @return a data sink pointer
 */
DataSinkPtr createOPCSink(OperatorId logicalOperatorId,
                                SchemaPtr schema,
                          QuerySubPlanId parentPlanId,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password);
#endif
/**
 * @brief create a ZMQ test sink with a schema and CSV format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @return a data sink pointer
 */
DataSinkPtr createCSVZmqSink(OperatorId logicalOperatorId,
                             const SchemaPtr& schema,
                             QuerySubPlanId parentPlanId,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             const std::string& host,
                             uint16_t port);

/**
 * @brief create a ZMQ test sink with a schema and NES_FORMAT format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @return a data sink pointer
 */
DataSinkPtr createBinaryZmqSink(OperatorId logicalOperatorId,
                                const SchemaPtr& schema,
                                QuerySubPlanId parentPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                const std::string& host,
                                uint16_t port,
                                bool internal);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param output stream
 * @return a data sink pointer
 */
DataSinkPtr createTextPrintSink(OperatorId logicalOperatorId,
                                const SchemaPtr& schema,
                                QuerySubPlanId parentPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                std::ostream& out);

/**
 * @brief create a print that does not output something
 * @return a data sink pointer
 */
DataSinkPtr createNullOutputSink(OperatorId logicalOperatorId, QuerySubPlanId parentPlanId);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param parentPlanId id of the parent qep
 * @param bufferManager
 * @param output stream
 * @return a data sink pointer
 */
DataSinkPtr createCSVPrintSink(OperatorId logicalOperatorId,
                               const SchemaPtr& schema,
                               QuerySubPlanId parentPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               std::ostream& out);

/**
 * @brief create a network data sink
 * @param schema
 * @param networkManager
 * @param nodeLocation
 * @param nesPartition
 * @param waitTime
 * @param retryTimes
 * @return a data sink pointer
 */
DataSinkPtr createNetworkSink(OperatorId logicalOperatorId,
                              const SchemaPtr& schema,
                              QuerySubPlanId parentPlanId,
                              const Network::NodeLocation& nodeLocation,
                              Network::NesPartition nesPartition,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              std::chrono::seconds waitTime = std::chrono::seconds(2),
                              uint8_t retryTimes = 5);

#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief create kafka sink
 * @param schema: schema of the data
 * @param brokers: broker list
 * @param topic: kafka topic to write to
 * @param kafkaProducerTimeout: kafka producer timeout
 * @return a data sink pointer
 */
DataSinkPtr
createKafkaSinkWithSchema(OperatorId logicalOperatorId, SchemaPtr schema, const std::string& brokers, const std::string& topic, uint64_t kafkaProducerTimeout);
#endif

#ifdef ENABLE_MQTT_BUILD
/**
 * @brief create MQTT sink
 * @param schema: schema of the data
 * @param parentPlanId: the Id of the parent plan
 * @param nodeEngine: a node engine pointer, e.g. for access to the buffer manager
 * @param address: address of a MQTT broker
 * @param clientID: ID that identifies the MQTT client
 * @param topic: broker topic/path to which the MQTT messages are published
 * @param user: used to identify client at broker
 * @param maxBufferedMSGs: maximal number for how many messages can be buffered at client
 * @param timeUnit: unit used to interpret the msgDelay value (seconds vs milliseconds vs nanoseconds)
 * @param msgDelay: how long to wait before sending the next message from client
 * @param asynchronousClient: 1 if client should be asynchronous, else 0
 * @return a data sink pointer
 */
DataSinkPtr createMQTTSink(OperatorId logicalOperatorId,
                           const SchemaPtr& schema,
                           QuerySubPlanId parentPlanId,
                           const Runtime::NodeEnginePtr& nodeEngine,
                           std::string const& address,
                           std::string const& clientID,
                           std::string const& topic,
                           std::string const& user,
                           uint64_t maxBufferedMSGs,
                           MQTTSinkDescriptor::TimeUnits timeUnit,
                           uint64_t msgDelay,
                           MQTTSinkDescriptor::ServiceQualities qualityOfService,
                           bool asynchronousClient);
#endif

}// namespace NES
#endif /* INCLUDE_SOURCESINK_SINKCREATOR_HPP_ */
