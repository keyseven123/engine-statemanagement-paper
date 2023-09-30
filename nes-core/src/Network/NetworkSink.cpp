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
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>

namespace NES::Network {
NetworkSink::NetworkSink(const SchemaPtr& schema,
                         uint64_t uniqueNetworkSinkDescriptorId,
                         QueryId queryId,
                         QuerySubPlanId querySubPlanId,
                         const NodeLocation& destination,
                         NesPartition nesPartition,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         FaultToleranceType faultToleranceType,
                         uint64_t numberOfOrigins)
    : SinkMedium(
        std::make_shared<NesFormat>(schema, NES::Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()),
        nodeEngine,
        numOfProducers,
        queryId,
        querySubPlanId,
        faultToleranceType,
        numberOfOrigins,
        nullptr),
      uniqueNetworkSinkDescriptorId(nodeEngine->getUniqueSinkDescriptor()), nodeEngine(nodeEngine),
      networkManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getNetworkManager()),
      queryManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getQueryManager()), receiverLocation(destination),
      bufferManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()), nesPartition(nesPartition),
      numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes) {
    //todo: #4230 use the uniqueNetworkSinkDescriptorId parameter to set the id instead of obtaining a value from the node engine
    (void) uniqueNetworkSinkDescriptorId;
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
    if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE) {
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        };
    } else {
        insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {
        };
    }
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    NES_TRACE("context {} writing data", workerContext.getId());
    //if async establishing of connection is in process, do not attempt to send data but buffer it instead
    if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
        NES_TRACE("context {} buffering data", workerContext.getId());
        workerContext.insertIntoReconnectBufferStorage(getUniqueNetworkSinkDescriptorId(), inputBuffer);
        return true;
    }

    auto* channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
    if (channel) {
        auto success = channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        if (success) {
            insertIntoStorageCallback(inputBuffer, workerContext);
        } else {
            //todo 4228: if this sink run on a mobile device (add a flag to indicate this), initiate async reconnect here
        }
        return success;
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

void NetworkSink::preSetup() {
    NES_DEBUG("NetworkSink: method preSetup() called {} qep {}", nesPartition.toString(), querySubPlanId);
    NES_ASSERT2_FMT(
        networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, inherited1::shared_from_this()),
        "Cannot register event listener " << nesPartition.toString());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called {} qep {}", nesPartition.toString(), querySubPlanId);
    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::Initialize,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    queryManager->addReconfigurationMessage(queryId, querySubPlanId, reconf, true);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called {} queryId {} qepsubplan {}", nesPartition.toString(), queryId, querySubPlanId);
    networkManager->unregisterSubpartitionProducer(nesPartition);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called {} qep {}", nesPartition.toString(), querySubPlanId);
    inherited0::reconfigure(task, workerContext);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (task.getType()) {
        case Runtime::ReconfigurationType::Initialize: {
            if (nodeEngine->getConnectSinksAsync()) {
                connectToChannelAsync(workerContext, receiverLocation, nesPartition);
            } else {
                auto channel = networkManager->registerSubpartitionProducer(receiverLocation,
                                                                            nesPartition,
                                                                            bufferManager,
                                                                            waitTime,
                                                                            retryTimes);
                NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), std::move(channel));
                NES_DEBUG("NetworkSink: reconfigure() stored channel on {} Thread {} ref cnt {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId(),
                          task.getUserData<uint32_t>());
            }
            workerContext.setObjectRefCnt(this, task.getUserData<uint32_t>());
            workerContext.createStorage(nesPartition);
            break;
        }
        case Runtime::ReconfigurationType::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            break;
        }
        case Runtime::ReconfigurationType::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            break;
        }
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            break;
        }
        case Runtime::ReconfigurationType::PropagateEpoch: {
            auto* channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto timestamp = task.getUserData<uint64_t>();
            NES_DEBUG("Executing PropagateEpoch on qep queryId={} punctuation={}", queryId, timestamp);
            channel->sendEvent<Runtime::PropagateEpochEvent>(Runtime::EventType::kCustomEvent, timestamp, queryId);
            workerContext.trimStorage(nesPartition, timestamp);
            break;
        }
        case Runtime::ReconfigurationType::ConnectToNewNetworkSource: {
            //retrieve information about which source to connect to
            auto [newReceiverLocation, newPartition] = task.getUserData<std::pair<NodeLocation, NesPartition>>();
            if (newReceiverLocation == receiverLocation && newPartition == nesPartition) {
                NES_THROW_RUNTIME_ERROR("Attempting reconnect but the new source descriptor equals the old one");
            }

            //todo #4229: instead of keeping futures to wait for, implement a mechanism to signal the channel to stop connecting
            if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
                workerContext.abortConnectionProcess(getUniqueNetworkSinkDescriptorId());
            }

            connectToChannelAsync(workerContext, newReceiverLocation, newPartition);
            break;
        }
        case Runtime::ReconfigurationType::ConnectionEstablished: {
            //if the callback was triggered by the channel for another thread becoming ready, we cannot do anything
            if (!workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
                NES_DEBUG("NetworkSink: reconfigure() No network channel future found for operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                break;
            }

            //retrieve new channel
            auto newNetworkChannelFuture = workerContext.getAsyncConnectionResult(getUniqueNetworkSinkDescriptorId());

            //if the connection process did not finish yet, the reconfiguration was triggered by another thread.
            if (!newNetworkChannelFuture.has_value()) {
                NES_DEBUG("NetworkSink: reconfigure() network channel has not connected yet for operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                break;
            }

            //check if channel connected succesfully
            if (newNetworkChannelFuture.value() == nullptr) {
                NES_DEBUG("NetworkSink: reconfigure() network channel retrieved from future is null for operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                //todo 4228: if this sink run on a mobile device (add a flag to indicate this), keep trying to reconnect even after timeout
                break;
            }
            workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), std::move(newNetworkChannelFuture.value()));

            NES_INFO("stop buffering data for context {}", workerContext.getId());
            unbuffer(workerContext);
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        //todo #3013: make sure buffers are kept if the device is currently buffering
        if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
            //wait until channel has either connected or connection times out, so we do not an channel open
            NES_DEBUG("NetworkSink: reconfigure() waiting for channel to connect in order to unbuffer before shutdown. "
                      "operator {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
            auto channel = workerContext.waitForAsyncConnection(getUniqueNetworkSinkDescriptorId());
            if (channel) {
                NES_DEBUG("NetworkSink: reconfigure() established connection for operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), std::move(channel));
            } else {
                //do not release network channel in the next step because none was established
                return;
            }
        }
        unbuffer(workerContext);
        if (workerContext.decreaseObjectRefCnt(this) == 1) {
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(), terminationType),
                            "Cannot remove network channel " << nesPartition.toString());
            NES_DEBUG("NetworkSink: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called {} parent plan {}", nesPartition.toString(), querySubPlanId);
    inherited0::postReconfigurationCallback(task);

    //update info about receiving network source to new target
    if (task.getType() == Runtime::ReconfigurationType::ConnectToNewNetworkSource) {
        auto [newReceiverLocation, newPartition] = task.getUserData<std::pair<NodeLocation, NesPartition>>();
        receiverLocation = newReceiverLocation;
        nesPartition = newPartition;
    }
}

void NetworkSink::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("NetworkSink::onEvent(event) called. uniqueNetworkSinkDescriptorId: {}", this->uniqueNetworkSinkDescriptorId);
    auto qep = queryManager->getQueryExecutionPlan(querySubPlanId);
    qep->onEvent(event);

    if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        // todo jm continue here. how to obtain local worker context?
    }
}
void NetworkSink::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
    NES_DEBUG("NetworkSink::onEvent(event, wrkContext) called. uniqueNetworkSinkDescriptorId: {}",
              this->uniqueNetworkSinkDescriptorId);
    // this function currently has no usage
    onEvent(event);
}

OperatorId NetworkSink::getUniqueNetworkSinkDescriptorId() { return uniqueNetworkSinkDescriptorId; }

Runtime::NodeEnginePtr NetworkSink::getNodeEngine() { return nodeEngine; }

void NetworkSink::connectToChannelAsync(Runtime::WorkerContext& workerContext,
                                        NodeLocation newNodeLocation,
                                        NesPartition newNesPartition) {
    NES_DEBUG("NetworkSink: method connectToChannelAsync() called {} qep {}, by thread {}",
              nesPartition.toString(),
              querySubPlanId,
              Runtime::NesThread::getId());

    if (networkManager->isPartitionProducerRegistered(nesPartition) != PartitionRegistrationStatus::NotFound) {
        networkManager->unregisterSubpartitionProducer(nesPartition);
    }

    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::ConnectionEstablished,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));

    auto networkChannelFuture = networkManager->registerSubpartitionProducerAsync(newNodeLocation,
                                                                                  newNesPartition,
                                                                                  bufferManager,
                                                                                  waitTime,
                                                                                  retryTimes,
                                                                                  reconf,
                                                                                  queryManager);
    //todo: #4227 use QueryTerminationType::Redeployment
    workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(), Runtime::QueryTerminationType::HardStop);
    workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(networkChannelFuture));
}

void NetworkSink::unbuffer(Runtime::WorkerContext& workerContext) {
    auto topBuffer = workerContext.removeBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());
    NES_INFO("sending buffered data");
    while (topBuffer) {
        if (!topBuffer.value().getBuffer()) {
            NES_WARNING("buffer does not exist");
            break;
        }
        if (!writeData(topBuffer.value(), workerContext)) {
            NES_WARNING("could not send all data from buffer");
            break;
        }
        NES_TRACE("buffer sent");
        topBuffer = workerContext.removeBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());
    }
}
}// namespace NES::Network
