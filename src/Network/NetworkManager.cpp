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

#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

namespace NES {

namespace Network {

NetworkManager::NetworkManager(const std::string& hostname, uint16_t port, ExchangeProtocol&& exchangeProtocol,
                               NodeEngine::BufferManagerPtr bufferManager, uint16_t numServerThread)
    : exchangeProtocol(std::move(exchangeProtocol)),
      server(std::make_shared<ZmqServer>(hostname, port, numServerThread, this->exchangeProtocol, bufferManager)) {
    bool success = server->start();
    if (success) {
        NES_INFO("NetworkManager: Server started successfully");
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkManager: Server failed to start on " << hostname << ":" << port);
    }
}

NetworkManagerPtr NetworkManager::create(const std::string& hostname, uint16_t port, Network::ExchangeProtocol&& exchangeProtocol,
                                         NodeEngine::BufferManagerPtr bufferManager, uint16_t numServerThread) {
    return std::make_shared<NetworkManager>(hostname, port, std::move(exchangeProtocol), bufferManager, numServerThread);
}

bool NetworkManager::isPartitionRegistered(NesPartition nesPartition) const {
    return exchangeProtocol.getPartitionManager()->isRegistered(nesPartition);
}

uint64_t NetworkManager::registerSubpartitionConsumer(NesPartition nesPartition) {
    NES_DEBUG("NetworkManager: Registering SubpartitionConsumer: " << nesPartition.toString());
    return exchangeProtocol.getPartitionManager()->registerSubpartition(nesPartition);
}

uint64_t NetworkManager::unregisterSubpartitionConsumer(NesPartition nesPartition) {
    NES_INFO("NetworkManager: Unregistering SubpartitionConsumer: " << nesPartition.toString());
    return exchangeProtocol.getPartitionManager()->unregisterSubpartition(nesPartition);
}

OutputChannelPtr NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                              std::chrono::seconds waitTime, uint8_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << nesPartition.toString());
    // method needs to return a pointer so that it can be passed to boost::thread_specific_ptr
    return OutputChannel::create(server->getContext(), nodeLocation.createZmqURI(), nesPartition, exchangeProtocol, waitTime,
                                 retryTimes);
}

}// namespace Network
}// namespace NES