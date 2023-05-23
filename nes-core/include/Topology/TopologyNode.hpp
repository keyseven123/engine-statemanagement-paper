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

#ifndef NES_CORE_INCLUDE_TOPOLOGY_TOPOLOGYNODE_HPP_
#define NES_CORE_INCLUDE_TOPOLOGY_TOPOLOGYNODE_HPP_

#include <Nodes/Node.hpp>
#include <Topology/LinkProperty.hpp>
#include <Util/TimeMeasurement.hpp>
#include <any>
#include <map>
#include <optional>

namespace NES {
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Spatial::DataTypes::Experimental {
class GeoLocation;
class Waypoint;
}// namespace Spatial::DataTypes::Experimental

namespace Spatial::Experimental {
enum class SpatialType;
}// namespace Spatial::Experimental

namespace Spatial::Mobility::Experimental {
class ReconnectSchedule;
using ReconnectSchedulePtr = std::unique_ptr<ReconnectSchedule>;
}// namespace Spatial::Mobility::Experimental

/**
 * @brief This class represents information about a physical node participating in the NES infrastructure
 */
class TopologyNode : public Node {

  public:
    static TopologyNodePtr create(const uint64_t id,
                                  const std::string& ipAddress,
                                  const uint32_t grpcPort,
                                  const uint32_t dataPort,
                                  const uint64_t resources,
                                  const uint64_t memoryCapacity,
                                  const uint64_t mtbfValue,
                                  const uint64_t launchTime,
                                  const uint64_t epochValue,
                                  const uint64_t ingestionRate,
                                  const uint64_t networkCapacity,
                                  std::map<std::string, std::any> properties);

    virtual ~TopologyNode() = default;

    /**
     * @brief method to get the id of the node
     * @return id as a uint64_t
     */
    uint64_t getId() const;

    /**
     * @brief method to get the overall cpu capacity of the node
     * @return uint64_t cpu capacity
     */
    uint64_t getAvailableResources() const;


    /**
     * @brief method to get the overall memory capacity of the node
     * @return uint64_t memory capacity
     */
    uint64_t getMemoryCapacity() const;

    /**
     * @brief method to get the memory capacity of the node
     * @return uint64_t initial memory capacity
     */
    uint64_t getInitialMemoryCapacity() const;

    /**
     * @brief method to reduce the memory capacity of the node
     * @param uint64_t used memory
     */
    void reduceMemoryCapacity(uint64_t usedMemory);

    /**
     * @brief method to reduce the network capacity of the node
     * @param uint64_t used network
     */
    void reduceNetworkCapacity(uint64_t usedNetwork);

    /**
     * @brief method to get the mtbf value of the node
     * @return uint64_t mtbf value
     */
    uint64_t getMTBFValue() const;

    /**
     * @brief calculates propbability of the device failure
     * @param topologyNode
     * @return probability of failure
     */
    double calculateReliability() const;

    /**
     * @brief method to get the launch time of the node
     * @return uint64_t launch time
     */
    uint64_t getLaunchTime() const;

    /**
     * @brief method to get the epoch value
     * @return uint64_t epoch value
     */
    uint64_t getEpochValue() const;

    /**
     * @brief method to get the ingestion rate of the node
     * @return uint64_t ingestion rate
     */
    uint64_t getIngestionRate() const;

    /**
     * @brief method to get the network capacity of the node
     * @return uint64_t network capacity
     */
    uint64_t getNetworkCapacity() const;

    /**
     * @brief method to get the intial network capacity of the node
     * @return uint64_t initial network capacity
     */
    uint64_t getInitialNetworkCapacity() const;

    /**
     * @brief method to reduce the cpu capacity of the node
     * @param uint64_t of the value that has to be subtracted
     */
    void reduceResources(uint64_t usedCapacity);

    /**
     * @brief method to increase CPU capacity
     * @param uint64_t of the vlaue that has to be added
     */
    void increaseResources(uint64_t freedCapacity);

    /**
     * @brief Get ip address of the node
     * @return ip address
     */
    std::string getIpAddress() const;

    /**
     * @brief Get grpc port for the node
     * @return grpc port
     */
    uint32_t getGrpcPort() const;

    /**
     * @brief Get the data port for the node
     * @return data port
     */
    uint32_t getDataPort() const;

    /**
     * @brief Get maintenance flag where 1 represents marked for maintenance
     * @return bool
     */
    bool isUnderMaintenance();

    /**
     * @brief sets maintenance flag where 1 represents marked for maintenance
     * @param flag
     */
    void setForMaintenance(bool flag);

    std::string toString() const override;

    /**
     * @brief Create a shallow copy of the physical node i.e. without copying the parent and child nodes
     * @return
     */
    TopologyNodePtr copy();

    explicit TopologyNode(uint64_t id,
                          std::string ipAddress,
                          uint32_t grpcPort,
                          uint32_t dataPort,
                          uint64_t resources,
                          uint64_t memoryCapacity,
                          uint64_t mtbfValue,
                          uint64_t launchTime,
                          uint64_t epochValue,
                          uint64_t ingestionRate,
                          uint64_t networkCapacity,
                          std::map<std::string, std::any> properties);

    bool containAsParent(NodePtr node) override;

    bool containAsChild(NodePtr node) override;

    /**
     * @brief Add a new property to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void addNodeProperty(const std::string& key, const std::any& value);


    /**
     * @brief Add a new property to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void updateNodeProperty(const std::string& key, const std::any& value);


    /**
     * @brief Check if a Node property exists
     * @param key key of the property
     * @return true if the property with the given key exists
     */
    bool hasNodeProperty(const std::string& key);

    /**
     * @brief Get a the value of a property
     * @param key key of the value to retrieve
     * @return value of the property with the given key
     */
    std::any getNodeProperty(const std::string& key);

    /**
     * @brief Remove a property from the stored properties map
     * @param key key of the property to remove
     * @return true if the removal is successful
     */
    bool removeNodeProperty(const std::string& key);

    /**
     * @brief add a new link property to the stored properties map
     * @param linked topology node to which the property will be associated
     * @param value of the link property
     */
    void addLinkProperty(const TopologyNodePtr& linkedNode, const LinkPropertyPtr& topologyLink);

    /**
     * @brief get a the value of a link property
     * @param linked topology node associated with the link property to retrieve
     * @return value of the link property
     */
    LinkPropertyPtr getLinkProperty(const TopologyNodePtr& linkedNode);

    /**
     * @brief remove a a link property from the stored map
     * @param linked topology node associated with the link property to remove
     * @return true if the removal is successful
     */
    bool removeLinkProperty(const TopologyNodePtr& linkedNode);

    /**
     * Experimental
     * @brief sets the status of the node as running on a mobile device or a fixed location device.
     * To be run right after node creation. Fixed nodes should not become mobile or vice versa at a later point
     * @param spatialType
     */
    void setSpatialType(NES::Spatial::Experimental::SpatialType spatialType);

    /**
     * Experimental
     * @brief check if the node is a running on a mobile device or not
     * @return true if the node is running on a mobile device
     */
    NES::Spatial::Experimental::SpatialType getSpatialNodeType();

  private:
    uint64_t id;
    std::string ipAddress;
    uint32_t grpcPort;
    uint32_t dataPort;
    uint64_t resources;
    uint64_t memoryCapacity;
    uint64_t initialMemoryCapacity;
    uint64_t mtbfValue;
    uint64_t launchTime;
    uint64_t epochValue;
    uint64_t ingestionRate;
    uint64_t initialNetworkCapacity;
    uint64_t networkCapacity;
    uint64_t usedResources;

    /**
     * @brief A field to store a map of node properties
     */
    std::map<std::string, std::any> nodeProperties;

    /**
     * @brief A field to store a map of the id of the linked nodes and its link property
     */
    std::map<uint64_t, LinkPropertyPtr> linkProperties;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_TOPOLOGY_TOPOLOGYNODE_HPP_
