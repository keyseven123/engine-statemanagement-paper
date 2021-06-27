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

#ifndef NES_WORKERCONTEXT_HPP_
#define NES_WORKERCONTEXT_HPP_

#include <Network/NesPartition.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/OutputChannelKey.hpp>
#include <NodeEngine/NesThread.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace NES::NodeEngine {

/**
 * @brief A WorkerContext represents the current state of a worker thread
 * Note that it is not thread-safe per se but it is meant to be used in
 * a thread-safe manner by the ThreadPool.
 */
class WorkerContext {
  private:
    /// the id of this worker context (unique per thread).
    uint32_t workerId;

    std::unordered_map<Network::OutputChannelKey, Network::OutputChannelPtr> channels;

  public:
    explicit WorkerContext(uint32_t workerId);

    /**
     * @brief get current worker context thread id. This is assigned by calling NesThread::getId()
     * @return current worker context thread id
     */
    uint32_t getId() const;

    /**
     * @brief This stores an output channel for an operator
     * @param querySubPlanId and id of the operator
     * @param channel the output channel
     */
    void storeChannel(Network::OutputChannelKey id, Network::OutputChannelPtr&& channel);

    /**
     * @brief removes a registered output channel
     * @param querySubPlanId and id of the operator
     * @param notifyRelease: if true, then channel sends EoS messages
     */
    void releaseChannel(Network::OutputChannelKey id);

    /**
     * @brief retrieve a registered output channel
     * @param ownerId id of the operator that we want to store the output channel
     * @return an output channel
     */
    Network::OutputChannel* getChannel(Network::OutputChannelKey ownerId);
};
}// namespace NES::NodeEngine
#endif//NES_WORKERCONTEXT_HPP_
