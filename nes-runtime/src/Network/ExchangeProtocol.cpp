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

#include <Network/ExchangeProtocol.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/PartitionManager.hpp>
#include <Runtime/Execution/DataEmitter.hpp>

namespace NES::Network {

// Important invariant: never leak the protocolListener pointer
// there is a hack that disables the reference counting

ExchangeProtocol::ExchangeProtocol(std::shared_ptr<PartitionManager> partitionManager,
                                   std::shared_ptr<ExchangeProtocolListener> protocolListener)
    : partitionManager(std::move(partitionManager)), protocolListener(std::move(protocolListener)) {
    NES_ASSERT(this->partitionManager, "Wrong parameter partitionManager is null");
    NES_ASSERT(this->protocolListener, "Wrong parameter ExchangeProtocolListener is null");
    NES_DEBUG("ExchangeProtocol: Initializing ExchangeProtocol()");
}

std::variant<Messages::ServerReadyMessage, Messages::ErrorMessage>
ExchangeProtocol::onClientAnnouncement(Messages::ClientAnnounceMessage msg) {
    using namespace Messages;
    // check if the partition is registered via the partition manager or wait until this is not done
    // if all good, send message back
    bool isDataChannel = msg.getMode() == Messages::ChannelType::DataChannel;
    NES_INFO("ExchangeProtocol: ClientAnnouncement received for {} {}",
             msg.getChannelId().toString(),
             (isDataChannel ? "Data" : "EventOnly"));
    auto nesPartition = msg.getChannelId().getNesPartition();

    // check if identity is registered
    if (isDataChannel) {
        // we got a connection from a data channel: it means there is/will be a partition consumer running locally
        if (auto status = partitionManager->getConsumerRegistrationStatus(nesPartition);
            status == PartitionRegistrationStatus::Registered) {

            //check version
            if (partitionManager->getVersion(nesPartition) != msg.getVersionNumber()) {
                //todo: implment another message here
                NES_DEBUG("Ignoring client anouncement for version {} because the current version is {}",
                          msg.getVersionNumber(),
                          partitionManager->getVersion(nesPartition));
                return Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError);
            }

            // increment the counter
            partitionManager->pinSubpartitionConsumer(nesPartition);
            NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for DataChannel {} REGISTERED",
                      msg.getChannelId().toString());
            // send response back to the client based on the identity
            return Messages::ServerReadyMessage(msg.getChannelId());
        } else if (status == PartitionRegistrationStatus::Deleted) {
            NES_WARNING("ExchangeProtocol: ClientAnnouncement received for  DataChannel {} but WAS DELETED",
                        msg.getChannelId().toString());
            protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError));
            return Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError);
        }
    } else {
        // we got a connection from an event-only channel: it means there is/will be an event consumer attached to a partition producer
        if (auto status = partitionManager->getProducerRegistrationStatus(nesPartition);
            status == PartitionRegistrationStatus::Registered) {
            NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for EventChannel {} REGISTERED",
                      msg.getChannelId().toString());
            partitionManager->pinSubpartitionProducer(nesPartition);
            // send response back to the client based on the identity
            return Messages::ServerReadyMessage(msg.getChannelId());
        } else if (status == PartitionRegistrationStatus::Deleted) {
            NES_WARNING("ExchangeProtocol: ClientAnnouncement received for event channel {} WAS DELETED",
                        msg.getChannelId().toString());
            protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError));
            return Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError);
        }
    }

    NES_WARNING("ExchangeProtocol: ClientAnnouncement received for {} NOT REGISTERED", msg.getChannelId().toString());
    protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError));
    return Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError);
}

void ExchangeProtocol::onBuffer(NesPartition nesPartition, Runtime::TupleBuffer& buffer) {
    if (partitionManager->getConsumerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        protocolListener->onDataBuffer(nesPartition, buffer);
        partitionManager->getDataEmitter(nesPartition)->emitWork(buffer);
    } else {
        NES_ERROR("DataBuffer for {} is not registered and was discarded!", nesPartition.toString());
    }
}

void ExchangeProtocol::onServerError(const Messages::ErrorMessage error) { protocolListener->onServerError(error); }

void ExchangeProtocol::onChannelError(const Messages::ErrorMessage error) { protocolListener->onChannelError(error); }

void ExchangeProtocol::onEvent(NesPartition nesPartition, Runtime::BaseEvent& event) {
    if (partitionManager->getConsumerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        protocolListener->onEvent(nesPartition, event);
        partitionManager->getDataEmitter(nesPartition)->onEvent(event);
    } else if (partitionManager->getProducerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        protocolListener->onEvent(nesPartition, event);
        if (auto listener = partitionManager->getEventListener(nesPartition); listener != nullptr) {
            listener->onEvent(event);//
        }
    } else {
        NES_ERROR("DataBuffer for {} is not registered and was discarded!", nesPartition.toString());
    }
}

void ExchangeProtocol::onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage) {
    NES_DEBUG("ExchangeProtocol: EndOfStream message received from {}", endOfStreamMessage.getChannelId().toString());
    if (partitionManager->getConsumerRegistrationStatus(endOfStreamMessage.getChannelId().getNesPartition())
        == PartitionRegistrationStatus::Registered) {
        NES_ASSERT2_FMT(!endOfStreamMessage.isEventChannel(),
                        "Received EOS for data channel on event channel for consumer "
                            << endOfStreamMessage.getChannelId().toString());
        auto partition = endOfStreamMessage.getChannelId().getNesPartition();
        //we expect the total connection count to be the number of threads plus one registration of the source itself (happens in NetworkSource::bind())
        auto expectedTotalConnectionsInPartitionManager = endOfStreamMessage.getNumberOfSendingThreads();
        if (!partitionManager->unregisterSubpartitionConsumer(partition)) {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received on data channel from {} but there is still some active "
                      "subpartition: {}",
                      endOfStreamMessage.getChannelId().toString(),
                      *partitionManager->getSubpartitionConsumerCounter(endOfStreamMessage.getChannelId().getNesPartition()));
            //todo #4313: count connects instead of disconnects and implement timeout
        } else if (partitionManager->getSubpartitionConsumerDisconnectCount(partition).value()
                   < expectedTotalConnectionsInPartitionManager /*todo: && !timeout*/) {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received on data channel from {} expected number of total channel "
                      "disconnects for "
                      "subpartition: {} has not been reached: {}/{}",
                      endOfStreamMessage.getChannelId().toString(),
                      *partitionManager->getSubpartitionConsumerCounter(endOfStreamMessage.getChannelId().getNesPartition()),
                      partitionManager->getSubpartitionConsumerDisconnectCount(partition).value(),
                      expectedTotalConnectionsInPartitionManager);
        } else if (partitionManager->newVersionExists(partition)) {
                partitionManager->getDataEmitter(endOfStreamMessage.getChannelId().getNesPartition())->onVersionUpdate();
        } else {
            partitionManager->getDataEmitter(endOfStreamMessage.getChannelId().getNesPartition())
                ->onEndOfStream(endOfStreamMessage.getQueryTerminationType());
            protocolListener->onEndOfStream(endOfStreamMessage);
        }
    } else if (partitionManager->getProducerRegistrationStatus(endOfStreamMessage.getChannelId().getNesPartition())
               == PartitionRegistrationStatus::Registered) {
        NES_ASSERT2_FMT(endOfStreamMessage.isEventChannel(),
                        "Received EOS for event channel on data channel for producer "
                            << endOfStreamMessage.getChannelId().toString());
        if (partitionManager->unregisterSubpartitionProducer(endOfStreamMessage.getChannelId().getNesPartition())) {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received from event channel {} but with no active subpartition",
                      endOfStreamMessage.getChannelId().toString());
        } else {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received from event channel {} but there is still some active "
                      "subpartition: {}",
                      endOfStreamMessage.getChannelId().toString(),
                      *partitionManager->getSubpartitionProducerCounter(endOfStreamMessage.getChannelId().getNesPartition()));
        }
    } else {
        NES_ERROR("ExchangeProtocol: EndOfStream message received from {} however the partition is not registered on this worker",
                  endOfStreamMessage.getChannelId().toString());
        protocolListener->onServerError(
            Messages::ErrorMessage(endOfStreamMessage.getChannelId(), Messages::ErrorType::UnknownPartitionError));
    }
}

std::shared_ptr<PartitionManager> ExchangeProtocol::getPartitionManager() const { return partitionManager; }

}// namespace NES::Network