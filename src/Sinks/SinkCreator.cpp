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

#include <Network/NetworkSink.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>
#include <Sinks/Mediums/MaterializedViewSink.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr createTextFileSink(const SchemaPtr& schema,
                               QuerySubPlanId parentPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               const std::string& filePath,
                               bool append) {
    //TODO: this is not nice and should be fixed such that we only provide the paramter once
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

DataSinkPtr createCSVFileSink(const SchemaPtr& schema,
                              QuerySubPlanId parentPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              const std::string& filePath,
                              bool append) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

DataSinkPtr createBinaryNESFileSink(const SchemaPtr& schema,
                                    QuerySubPlanId parentPlanId,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    const std::string& filePath,
                                    bool append) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

DataSinkPtr createJSONFileSink(const SchemaPtr& schema,
                               QuerySubPlanId parentPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               const std::string& filePath,
                               bool append) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

DataSinkPtr createTextZmqSink(const SchemaPtr& schema,
                              QuerySubPlanId parentPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              const std::string& host,
                              uint16_t port) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, host, port, false, parentPlanId);
}

DataSinkPtr createCSVZmqSink(const SchemaPtr& schema,
                             QuerySubPlanId parentPlanId,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             const std::string& host,
                             uint16_t port) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, host, port, false, parentPlanId);
}

DataSinkPtr createBinaryZmqSink(const SchemaPtr& schema,
                                QuerySubPlanId parentPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                const std::string& host,
                                uint16_t port,
                                bool internal) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, host, port, internal, parentPlanId);
}

DataSinkPtr createTextPrintSink(const SchemaPtr& schema,
                                QuerySubPlanId parentPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                std::ostream& out) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, parentPlanId, out);
}

DataSinkPtr createNullOutputSink(QuerySubPlanId parentPlanId) { return std::make_shared<NullOutputSink>(parentPlanId); }

DataSinkPtr createCSVPrintSink(const SchemaPtr& schema,
                               QuerySubPlanId parentPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               std::ostream& out) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, parentPlanId, out);
}

DataSinkPtr createNetworkSink(const SchemaPtr& schema,
                              QuerySubPlanId parentPlanId,
                              Network::NodeLocation const& nodeLocation,
                              Network::NesPartition nesPartition,
                              Runtime::NodeEnginePtr const& nodeEngine,
                              std::chrono::seconds waitTime,
                              uint8_t retryTimes) {
    return std::make_shared<Network::NetworkSink>(schema,
                                                  parentPlanId,
                                                  nodeEngine->getNetworkManager(),
                                                  nodeLocation,
                                                  nesPartition,
                                                  nodeEngine->getBufferManager(),
                                                  nodeEngine->getQueryManager(),
                                                  nodeEngine->getBufferStorage(),
                                                  waitTime,
                                                  retryTimes);
}

namespace Experimental::MaterializedView {

DataSinkPtr createMaterializedViewSink(SchemaPtr schema,
                                       Runtime::NodeEnginePtr const& nodeEngine,
                                       QuerySubPlanId parentPlanId,
                                       uint64_t viewId) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    Experimental::MaterializedView::MaterializedViewPtr view = nullptr;
    if (nodeEngine->getMaterializedViewManager()->containsView(viewId)){
        view = nodeEngine->getMaterializedViewManager()->getView(viewId);
    } else {
        view = nodeEngine->getMaterializedViewManager()->createView(Experimental::MaterializedView::ViewType::TUPLE_VIEW, viewId);
    }
    return std::make_shared<Experimental::MaterializedView::MaterializedViewSink>(
            std::move(view),
            format,
            parentPlanId);
}

} // Experimental::MaterializedView

#ifdef ENABLE_KAFKA_BUILD
DataSinkPtr
createKafkaSinkWithSchema(SchemaPtr schema, const std::string& brokers, const std::string& topic, uint64_t kafkaProducerTimeout) {
    return std::make_shared<KafkaSink>(schema, brokers, topic, kafkaProducerTimeout);
}
#endif

#ifdef ENABLE_OPC_BUILD
DataSinkPtr createOPCSink(SchemaPtr schema,
                          QuerySubPlanId parentPlanId,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password) {
    NES_DEBUG("plz fix me" << parentPlanId);
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<OPCSink>(format, url, nodeId, user, password, parentPlanId);
}
#endif

#ifdef ENABLE_MQTT_BUILD
DataSinkPtr createMQTTSink(const SchemaPtr& schema,
                           QuerySubPlanId parentPlanId,
                           const Runtime::NodeEnginePtr& nodeEngine,
                           const std::string& address,
                           const std::string& clientId,
                           const std::string& topic,
                           const std::string& user,
                           uint64_t maxBufferedMSGs,
                           const MQTTSinkDescriptor::TimeUnits timeUnit,
                           uint64_t msgDelay,
                           MQTTSinkDescriptor::ServiceQualities qualityOfService,
                           bool asynchronousClient) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MQTTSink>(format,
                                      parentPlanId,
                                      address,
                                      clientId,
                                      topic,
                                      user,
                                      maxBufferedMSGs,
                                      timeUnit,
                                      msgDelay,
                                      qualityOfService,
                                      asynchronousClient);
}
#endif
}// namespace NES
