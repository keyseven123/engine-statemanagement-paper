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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkSource.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/MemorySource.hpp>
#include <Sources/OPCSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Sources/YSBSource.hpp>
#include <Sources/ZmqSource.hpp>

#ifdef ENABLE_OPC_BUILD
    #include <open62541/client_config_default.h>
    #include <open62541/client_highlevel.h>
    #include <open62541/plugin/log_stdout.h>
#endif

namespace NES {

const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                                                  NodeEngine::QueryManagerPtr queryManager,
                                                                  OperatorId operatorId) {
    return std::make_shared<DefaultSource>(schema, bufferManager, queryManager, /*bufferCnt*/ 1, /*frequency*/ 1, operatorId);
}

const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                                                   NodeEngine::QueryManagerPtr queryManager,
                                                                   uint64_t numbersOfBufferToProduce, uint64_t frequency,
                                                                   OperatorId operatorId) {
    return std::make_shared<DefaultSource>(schema, bufferManager, queryManager, numbersOfBufferToProduce, frequency, operatorId);
}

const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(NodeEngine::BufferManagerPtr bufferManager,
                                                                             NodeEngine::QueryManagerPtr queryManager,
                                                                             OperatorId operatorId) {
    return std::make_shared<DefaultSource>(Schema::create()->addField("id", DataTypeFactory::createUInt64()), bufferManager,
                                           queryManager, /**bufferCnt*/ 1, /*frequency*/ 1, operatorId);
}

const DataSourcePtr createZmqSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                    NodeEngine::QueryManagerPtr queryManager, const std::string& host, const uint16_t port,
                                    OperatorId operatorId) {
    return std::make_shared<ZmqSource>(schema, bufferManager, queryManager, host, port, operatorId);
}

const DataSourcePtr createBinaryFileSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                           NodeEngine::QueryManagerPtr queryManager, const std::string& pathToFile,
                                           OperatorId operatorId) {
    return std::make_shared<BinarySource>(schema, bufferManager, queryManager, pathToFile, operatorId);
}

const DataSourcePtr createSenseSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                      NodeEngine::QueryManagerPtr queryManager, const std::string& udfs, OperatorId operatorId) {
    return std::make_shared<SenseSource>(schema, bufferManager, queryManager, udfs, operatorId);
}

const DataSourcePtr createCSVFileSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                        NodeEngine::QueryManagerPtr queryManager, const std::string& pathToFile,
                                        const std::string& delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                        uint64_t numbersOfBufferToProduce, uint64_t frequency, bool skipHeader,
                                        OperatorId operatorId) {
    return std::make_shared<CSVSource>(schema, bufferManager, queryManager, pathToFile, delimiter,
                                       numberOfTuplesToProducePerBuffer, numbersOfBufferToProduce, frequency, skipHeader,
                                       operatorId);
}

const DataSourcePtr createMemorySource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                       NodeEngine::QueryManagerPtr queryManager, OperatorId operatorId,
                                       std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize) {
    return std::make_shared<MemorySource>(schema, memoryArea, memoryAreaSize, bufferManager, queryManager, operatorId);
}

const DataSourcePtr createYSBSource(NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                                    uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                                    OperatorId operatorId) {
    return std::make_shared<YSBSource>(bufferManager, queryManager, numBuffersToProcess, numberOfTuplesToProducePerBuffer,
                                       frequency, operatorId);
}

const DataSourcePtr createNetworkSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                        NodeEngine::QueryManagerPtr queryManager, Network::NetworkManagerPtr networkManager,
                                        Network::NesPartition nesPartition) {
    return std::make_shared<Network::NetworkSource>(schema, bufferManager, queryManager, networkManager, nesPartition);
}

#ifdef ENABLE_KAFKA_BUILD
const DataSourcePtr createKafkaSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                      NodeEngine::QueryManagerPtr queryManager, std::string brokers, std::string topic,
                                      std::string groupId, bool autoCommit, uint64_t kafkaConsumerTimeout,
                                      OperatorId operatorId) {
    return std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, topic, groupId, autoCommit,
                                         kafkaConsumerTimeout, operatorId);
}
#endif

#ifdef ENABLE_OPC_BUILD
const DataSourcePtr createOPCSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                                    NodeEngine::QueryManagerPtr queryManager, std::string url, UA_NodeId nodeId, std::string user,
                                    std::string password, OperatorId operatorId) {
    return std::make_shared<OPCSource>(schema, bufferManager, queryManager, url, nodeId, user, password, operatorId);
}
#endif
}// namespace NES
