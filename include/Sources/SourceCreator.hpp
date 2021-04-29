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

#ifndef INCLUDE_SOURCESINK_SOURCECREATOR_HPP_
#define INCLUDE_SOURCESINK_SOURCECREATOR_HPP_

#include <Network/NetworkManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <chrono>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>
#endif

namespace NES {

/**
 * @brief function to create a test source which produces 10 tuples within one buffer with value one based on a schema
 * @param schema of the data source
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                                                  NodeEngine::QueryManagerPtr queryManager, OperatorId operatorId,
                                                                  size_t numSourceLocalBuffers);

/**
 * @brief function to create a test source which produces   tuples with value one in N buffers of based on a schema
 * @param schema of the data source
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @param number of buffers that should be produced
 * @param frequency when to gather the next buffer
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                                                   NodeEngine::QueryManagerPtr queryManager,
                                                                   uint64_t numbersOfBufferToProduce, uint64_t frequency,
                                                                   OperatorId operatorId, size_t numSourceLocalBuffers);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultSourceWithoutSchemaForOneBuffer(NodeEngine::BufferManagerPtr bufferManager,
                                                                 NodeEngine::QueryManagerPtr queryManager, OperatorId operatorId,
                                                                 size_t numSourceLocalBuffers);

/**
 * @brief function to create a lambda source
 * @param schema of the data source
 * @param bufferManager
 * @param queryManager
 * @param number of buffers that should be produced
 * @param frequency when to gather the next buffer
 * @param generationFunction
 * @param operatorId
 * @return a const data source pointer */
const DataSourcePtr createLambdaSource(
    SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
    uint64_t numbersOfBufferToProduce, uint64_t gatheringValue,
    std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    OperatorId operatorId, size_t numSourceLocalBuffers, DataSource::GatheringMode gatheringMode);

/**
 * @brief function to create an empty zmq source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param host
 * @param port
 * @param operatorId
 * @return a const data source pointer
 */
const DataSourcePtr createZmqSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                    NodeEngine::QueryManagerPtr queryManager, const std::string& host, const uint16_t port,
                                    OperatorId operatorId, size_t numSourceLocalBuffers);

/**
 * @brief function to create a binary file source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param path to the file to reading
 * @param operatorId
 * @return a const data source pointer
 */
const DataSourcePtr createBinaryFileSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                           NodeEngine::QueryManagerPtr queryManager, const std::string& pathToFile,
                                           OperatorId operatorId, size_t numSourceLocalBuffers);

/**
 * @brief function to create a sense source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param udfs of the file
 * @param operatorId
 * @return a const data source pointer
 */
const DataSourcePtr createSenseSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                      NodeEngine::QueryManagerPtr queryManager, const std::string& udfs, OperatorId operatorId,
                                      size_t numSourceLocalBuffers);

/**
 * @brief function to create a csvfile source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param pathToFile
 * @param delimiter
 * @param numberOfTuplesToProducePerBuffer
 * @param numBuffersToProcess
 * @param frequency
 * @param skipHeader
 * @param operatorId
 * @return a const data source pointer
 */
const DataSourcePtr createCSVFileSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                        NodeEngine::QueryManagerPtr queryManager, const std::string& pathToFile,
                                        const std::string& delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                        uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader, OperatorId operatorId,
                                        size_t numSourceLocalBuffers);

/**
 * @brief create a memory source
 * @param schema of the source
 * @param bufferManager
 * @param queryManager
 * @param memoryArea the memory buffer to scan and create buffers out of
 * @param memoryAreaSize the size of the memory buffer
 * @param numBuffersToProcess
 * @param frequency
 * @param operatorId
 * @return
 */
const DataSourcePtr createMemorySource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                       NodeEngine::QueryManagerPtr queryManager, std::shared_ptr<uint8_t> memoryArea,
                                       size_t memoryAreaSize, uint64_t numBuffersToProcess, uint64_t gatheringValue,
                                       OperatorId operatorId, size_t numSourceLocalBuffers,
                                       DataSource::GatheringMode gatheringMode);

const DataSourcePtr createNettyFileSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                          NodeEngine::QueryManagerPtr queryManager, const std::string& pathToFile,
                                          const std::string& delimiter,uint64_t numberOfTuplesToProducePerBuffer,
                                          uint64_t numBuffersToProcess,uint64_t frequency, bool skipHeader, OperatorId operatorId,const std::string& address
                                          , size_t numSourceLocalBuffers);

/**
 * @brief function to create a ysb source
 * @param schema of data source
 * @return a const data source pointer
 */

const DataSourcePtr createNettyFileSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                          NodeEngine::QueryManagerPtr queryManager, const std::string& pathToFile,
                                          const std::string& delimiter,uint64_t numberOfTuplesToProducePerBuffer,
                                          uint64_t numBuffersToProcess,uint64_t frequency, bool skipHeader, OperatorId operatorId,const std::string& address );

/**
 * @brief function to create a ysb source
 * @param schema of data source
 * @return a const data source pointer
 */

/**
 * @brief function to create a network source
 * @param schema
 * @param bufferManager
 * @param queryManager
 * @param networkManager
 * @param nesPartition
 * @return a const data source pointer
 */
const DataSourcePtr createNetworkSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                        NodeEngine::QueryManagerPtr queryManager, Network::NetworkManagerPtr networkManager,
                                        Network::NesPartition nesPartition, size_t numSourceLocalBuffers);

#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief Create kafka source
 * @param schema schema of the elements
 * @param brokers list of brokers
 * @param topic kafka topic
 * @param groupId group id
 * @param autoCommit bool indicating if offset has to be committed automatically or not
 * @param kafkaConsumerTimeout  kafka consumer timeout
 * @param operatorId: operator id
 * @return
 */
const DataSourcePtr createKafkaSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                      NodeEngine::QueryManagerPtr queryManager, std::string brokers, std::string topic,
                                      std::string groupId, bool autoCommit, uint64_t kafkaConsumerTimeout, OperatorId operatorId,
                                      size_t numSourceLocalBuffers);
#endif

#ifdef ENABLE_OPC_BUILD

/**
 * @brief Create OPC source
 * @param schema schema of the elements
 * @param url the url of the OPC server
 * @param nodeId the node id of the desired node
 * @param user name if connecting with a server with authentication
 * @param password for authentication if needed
 * @return a const data source pointer
 */
const DataSourcePtr createOPCSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                    NodeEngine::QueryManagerPtr queryManager, std::string url, UA_NodeId nodeId, std::string user,
                                    std::string password, OperatorId operatorId, size_t numSourceLocalBuffers);
#endif

#ifdef ENABLE_MQTT_BUILD

/**
 * @brief Create MQTT source
 * @param schema schema of the elements
 * @param serverAddress the serverAddress of the MQTT server
 * @param clientId the client id of the data, we want to obtain
 * @param user name to connect to the server
 * @param topic the topic needed for a subscription
 * @return a const data source pointer
 */
const DataSourcePtr createMQTTSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                     NodeEngine::QueryManagerPtr queryManager, std::string serverAddress, std::string clientId,
                                     std::string user, std::string topic, OperatorId operatorId, size_t numSourceLocalBuffers);
#endif

}// namespace NES
#endif /* INCLUDE_SOURCESINK_SOURCECREATOR_HPP_ */
