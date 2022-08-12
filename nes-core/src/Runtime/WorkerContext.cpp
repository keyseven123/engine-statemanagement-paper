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
#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime {

WorkerContext::WorkerContext(uint32_t workerId,
                             const BufferManagerPtr& bufferManager,
                             uint64_t numberOfBuffersPerWorker,
                             uint32_t queueId)
    : workerId(workerId), queueId(queueId) {
    //we changed from a local pool to a fixed sized pool as it allows us to manage the numbers that are hold in the cache via the paramter
    localBufferPool = bufferManager->createLocalBufferPool(numberOfBuffersPerWorker);
    NES_ASSERT(localBufferPool != nullptr, "Local buffer is not allowed to be null");
    currentEpoch = 0;
//    statisticsFile.open("latency" + std::to_string(workerId) + ".csv", std::ios::out);
//    storageFile.open("storage" + std::to_string(workerId) + ".csv", std::ios::out);
//    statisticsFile << "time, latency\n";
//    storageFile << "time, numberOfBuffers\n";
}

WorkerContext::~WorkerContext() {
    localBufferPool->destroy();
    storage.clear();
//    statisticsFile.flush();
//    statisticsFile.close();
//    storageFile.flush();
//    storageFile.close();
}

size_t WorkerContext::getStorageSize(Network::NesPartition nesPartitionId) {
    auto iteratorPartitionId = this->storage.find(nesPartitionId);
    if (iteratorPartitionId != this->storage.end()) {
        return iteratorPartitionId->second.size();
    }
    return 0;
}

void WorkerContext::printStatistics(Runtime::TupleBuffer& inputBuffer) {
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_ms.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    auto ts = std::chrono::system_clock::now();
    auto timeNow = std::chrono::system_clock::to_time_t(ts);
    statisticsFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << ",";
    statisticsFile << value.count() - inputBuffer.getCreationTimestamp() << "\n";
}

uint32_t WorkerContext::getId() const { return workerId; }

uint32_t WorkerContext::getQueueId() const { return queueId; }

void WorkerContext::setObjectRefCnt(void* object, uint32_t refCnt) {
    objectRefCounters[reinterpret_cast<uintptr_t>(object)] = refCnt;
}

uint32_t WorkerContext::increaseObjectRefCnt(void* object) { return objectRefCounters[reinterpret_cast<uintptr_t>(object)]++; }

uint32_t WorkerContext::decreaseObjectRefCnt(void* object) {
    auto ptr = reinterpret_cast<uintptr_t>(object);
    if (auto it = objectRefCounters.find(ptr); it != objectRefCounters.end()) {
        auto val = it->second--;
        if (val == 1) {
            objectRefCounters.erase(it);
        }
        return val;
    }
    return 0;
}

TupleBuffer WorkerContext::allocateTupleBuffer() { return localBufferPool->getBufferBlocking(); }

void WorkerContext::storeNetworkChannel(Network::OperatorId id, Network::NetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator " << id << " for context " << workerId);
    dataChannels[id] = std::move(channel);
}

void WorkerContext::createStorage(Network::NesPartition nesPartitionId) {
    this->storage[nesPartitionId] = std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferOrdering>();
}

void WorkerContext::insertIntoStorage(Network::NesPartition nesPartitionId, NES::Runtime::TupleBuffer buffer) {
    NES_ASSERT(buffer.getWatermark() > currentEpoch, "Trying to insert watermark" << buffer.getWatermark() << "smaller than epoch" << currentEpoch);
    storage[nesPartitionId].push(buffer);
    auto ts = std::chrono::system_clock::now();
    auto timeNow = std::chrono::system_clock::to_time_t(ts);
//    storageFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << ",";
//    storageFile << getStorageSize(nesPartitionId) << "\n";
}

void WorkerContext::trimStorage(Network::NesPartition nesPartitionId, uint64_t timestamp) {
    currentEpoch = timestamp;
    auto iteratorPartitionId = this->storage.find(nesPartitionId);
    if (iteratorPartitionId != this->storage.end()) {
        while (!iteratorPartitionId->second.empty() && iteratorPartitionId->second.top().getWatermark() <= timestamp) {
            NES_DEBUG("BufferStorage: Delete tuple with watermark" << iteratorPartitionId->second.top().getWatermark());
            iteratorPartitionId->second.pop();
        }
    }
}
    
bool WorkerContext::releaseNetworkChannel(Network::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_TRACE("WorkerContext: releasing channel for operator " << id << " for context " << workerId);
    if (auto it = dataChannels.find(id); it != dataChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType);
        }
        dataChannels.erase(it);
        return true;
    }
    return false;
}

void WorkerContext::storeEventOnlyChannel(Network::OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator " << id << " for context " << workerId);
    reverseEventChannels[id] = std::move(channel);
}

bool WorkerContext::releaseEventOnlyChannel(Network::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_TRACE("WorkerContext: releasing channel for operator " << id << " for context " << workerId);
    if (auto it = reverseEventChannels.find(id); it != reverseEventChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType);
        }
        reverseEventChannels.erase(it);
        return true;
    }
    return false;
}

Network::NetworkChannel* WorkerContext::getNetworkChannel(Network::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving channel for operator " << ownerId << " for context " << workerId);
    auto it = dataChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(Network::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving event only channel for operator " << ownerId << " for context " << workerId);
    auto it = reverseEventChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}
std::shared_ptr<AbstractBufferProvider> WorkerContext::getBufferProvider() { return localBufferPool; }

}// namespace NES::Runtime