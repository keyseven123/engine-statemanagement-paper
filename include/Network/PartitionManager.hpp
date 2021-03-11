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

#ifndef NES_PARTITIONMANAGER_HPP
#define NES_PARTITIONMANAGER_HPP

#include <Network/NesPartition.hpp>
#include <map>
#include <shared_mutex>
#include <unordered_map>

namespace NES::Network {

class PartitionManager {
  public:
    PartitionManager() = default;

    /**
     * @brief Registers a subpartition in the PartitionManager. If the subpartition does not exist a new entry is
     * added in the partition table, otherwise the counter is incremented.
     * @param the partition
     * @return true if this is  the first time the partition was registered, false otherwise
     */
    bool registerSubpartition(NesPartition partition);

    /**
     * @brief Unregisters a subpartition in the PartitionManager. If the subpartition does not exist or the current
     * counter is 0 an error is thrown.
     * @param the partition
     * @return true if the partition counter got to zero, false otherwise
     */
    bool unregisterSubpartition(NesPartition partition);

    /**
     * @brief Returns the current counter of a given partition. Throws error if not existing.
     * @param the partition
     * @return the counter of the partition
     * @throw  std::out_of_range  If no such data is present.
     */
    uint64_t getSubpartitionCounter(NesPartition partition);

    /**
     * @brief checks if a partition is registered
     * @param the partition
     * @return true if registered, else false
     */
    bool isRegistered(NesPartition partition) const;

    /**
     * @brief clears all registered partitions
     */
    void clear();

  private:
    std::unordered_map<NesPartition, uint64_t> partitionCounter;
    mutable std::shared_mutex partitionCounterMutex;
};
typedef std::shared_ptr<PartitionManager> PartitionManagerPtr;

}// namespace NES::Network

#endif//NES_PARTITIONMANAGER_HPP
