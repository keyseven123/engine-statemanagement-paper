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

#include <Network/ExchangeProtocol.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkSource.hpp>
#include <utility>

namespace NES {
namespace Network {

// Important invariant: never leak the protocolListener pointer
// there is a hack that disables the reference counting

ExchangeProtocol::ExchangeProtocol(std::shared_ptr<PartitionManager> partitionManager,
                                   std::shared_ptr<ExchangeProtocolListener> protocolListener)
    : partitionManager(std::move(partitionManager)), protocolListener(std::move(protocolListener)) {
    NES_ASSERT(this->partitionManager, "Wrong parameter partitionManager is null");
    NES_ASSERT(this->protocolListener, "Wrong parameter ExchangeProtocolListener is null");
    NES_DEBUG("ExchangeProtocol: Initializing ExchangeProtocol()");
}

ExchangeProtocol::~ExchangeProtocol() {}

Messages::ServerReadyMessage ExchangeProtocol::onClientAnnouncement(Messages::ClientAnnounceMessage msg) {
    // check if the partition is registered via the partition manager or wait until this is not done
    // if all good, send message back
    NES_INFO("ExchangeProtocol: ClientAnnouncement received for " << msg.getChannelId().toString());
    auto nesPartition = msg.getChannelId().getNesPartition();

    // check if identity is registered
    if (partitionManager->isRegistered(nesPartition)) {
        // increment the counter
        partitionManager->pinSubpartition(nesPartition);
        NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for " << msg.getChannelId().toString() << " REGISTERED");
        // send response back to the client based on the identity
        return Messages::ServerReadyMessage(msg.getChannelId());
    } else {
        NES_WARNING("ExchangeProtocol: ClientAnnouncement received for " << msg.getChannelId().toString() << " NOT REGISTERED");
        protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), Messages::kPartitionNotRegisteredError));
        return Messages::ServerReadyMessage(msg.getChannelId(), Messages::kPartitionNotRegisteredError);
    }
}

void ExchangeProtocol::onBuffer(NesPartition nesPartition, NodeEngine::TupleBuffer& buffer) {
    if (partitionManager->isRegistered(nesPartition)) {
        protocolListener->onDataBuffer(nesPartition, buffer);
        partitionManager->getDataEmitter(nesPartition)->emitWork(buffer);
    } else {
        NES_ERROR("DataBuffer for " + nesPartition.toString() + " is not registered and was discarded!");
    }
}

void ExchangeProtocol::onServerError(const Messages::ErrorMessage error) { protocolListener->onServerError(error); }

void ExchangeProtocol::onChannelError(const Messages::ErrorMessage error) { protocolListener->onChannelError(error); }

void ExchangeProtocol::onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage) {
    NES_DEBUG("ExchangeProtocol: EndOfStream message received from " << endOfStreamMessage.getChannelId().toString());
    if (partitionManager->isRegistered(endOfStreamMessage.getChannelId().getNesPartition())) {
        if (partitionManager->unregisterSubpartition(endOfStreamMessage.getChannelId().getNesPartition())) {
            protocolListener->onEndOfStream(endOfStreamMessage);
        } else {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received from "
                      << endOfStreamMessage.getChannelId().toString() << " but there is still some active subpartition: "
                      << partitionManager->getSubpartitionCounter(endOfStreamMessage.getChannelId().getNesPartition()));
        }
    } else {
        NES_ERROR("ExchangeProtocol: EndOfStream message received from "
                  << endOfStreamMessage.getChannelId().toString() << " however the partition is not registered on this worker");
        protocolListener->onServerError(Messages::ErrorMessage(endOfStreamMessage.getChannelId(), Messages::kUnknownPartition));
    }
}

void ExchangeProtocol::onQueryReconfiguration(Network::Messages::QueryReconfigurationMessage queryReconfigurationMessage) {
    protocolListener->onQueryReconfiguration(queryReconfigurationMessage);
}

std::shared_ptr<PartitionManager> ExchangeProtocol::getPartitionManager() const { return partitionManager; }

}// namespace Network
}// namespace NES