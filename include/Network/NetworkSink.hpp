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

#ifndef NES_NETWORKSINK_HPP
#define NES_NETWORKSINK_HPP

#include <Network/NesPartition.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

#include <Network/OutputChannelKey.hpp>
#include <string>

namespace NES {

namespace Network {

class NetworkSink : public SinkMedium {
  public:
    /**
     * @brief constructor for the network sink
     * @param schema
     * @param networkManager
     * @param nodeLocation
     * @param nesPartition
     */
    explicit NetworkSink(SchemaPtr schema,
                         QuerySubPlanId parentPlanId,
                         NetworkManagerPtr networkManager,
                         const NodeLocation nodeLocation,
                         NesPartition nesPartition,
                         NodeEngine::BufferManagerPtr bufferManager,
                         NodeEngine::QueryManagerPtr queryManager,
                         std::chrono::seconds waitTime = std::chrono::seconds(5),
                         uint8_t retryTimes = 10);

    ~NetworkSink();

    /**
     * @brief Writes data to the underlying output channel
     * @param inputBuffer
     * @param workerContext
     * @return true if no error occurred
     */
    bool writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContext& workerContext) override;

    /**
     * @return the string representation of the network sink
     */
    const std::string toString() const override;

    /**
     * @brief reconfiguration machinery for the network sink
     * @param task descriptor of the reconfiguration
     * @param workerContext the thread on which this is called
     */
    void reconfigure(NodeEngine::ReconfigurationMessage& task, NodeEngine::WorkerContext& workerContext) override;

    void postReconfigurationCallback(NodeEngine::ReconfigurationMessage&) override;

    /**
     * @brief setup method to configure the network sink via a reconfiguration
     */
    void setup() override;

    /**
     * @brief Destroys the network sink
     */
    void shutdown() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    NetworkManagerPtr networkManager;
    NodeEngine::QueryManagerPtr queryManager;
    const NodeLocation nodeLocation;
    NesPartition nesPartition;
    OutputChannelKey outputChannelKey;

    const std::chrono::seconds waitTime;
    const uint8_t retryTimes;
};

}// namespace Network
}// namespace NES

#endif// NES_NETWORKSINK_HPP
