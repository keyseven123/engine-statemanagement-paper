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

#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime {

WorkerContext::WorkerContext(uint32_t workerId,
                             const BufferManagerPtr& bufferManager,
                             uint64_t numberOfBuffersPerWorker,
                             uint32_t numaNode)
    : workerId(workerId), numaNode(numaNode) {
    localBufferPool = bufferManager->createLocalBufferPool(numberOfBuffersPerWorker);
    NES_ASSERT(localBufferPool != NULL, "Local buffer is not allowed to be null");
}

uint32_t WorkerContext::getId() const { return workerId; }

TupleBuffer WorkerContext::allocateTupleBuffer() { return localBufferPool->getBufferBlocking(); }

void WorkerContext::storeChannel(Network::OperatorId id, Network::OutputChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator " << id << " for context " << workerId);
    channels[id] = std::move(channel);
}

void WorkerContext::releaseChannel(Network::OperatorId id) {
    NES_TRACE("WorkerContext: releasing channel for operator " << id << " for context " << workerId);
    auto it = channels.find(id);
    if (it != channels.end()) {
        if (it->second) {
            it->second->close();
        }
        channels.erase(it);
    }
}

Network::OutputChannel* WorkerContext::getChannel(Network::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving channel for operator " << ownerId << " for context " << workerId);
    return channels[ownerId].get();
}
void WorkerContext::updateChannel(Network::OperatorId id, Network::OutputChannelPtr&& channel) {
    removeChannel(id, true);
    channels[id] = std::move(channel);
}
void WorkerContext::removeChannel(Network::OperatorId id, bool withMessagePropagation) {
    auto it = channels.find(id);
    if (it != channels.end()) {
        if (it->second) {
            it->second->shutdownZMQSocket(withMessagePropagation);
        }
        channels.erase(it);
    }
}

}// namespace NES::Runtime