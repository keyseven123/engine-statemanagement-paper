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

#ifndef NES_DYNAMICMEMORYLAYOUT_HPP
#define NES_DYNAMICMEMORYLAYOUT_HPP

#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

typedef uint64_t FIELD_SIZE;

/**
 * @brief This abstract class is the base class for DynamicRowLayout and DynamicColumnLayout.
 * As the base class, it has multiple methods or members that are usefull for both a row and column layout.
 */
class DynamicMemoryLayout {

  public:
    /**
     * @brief Constructor for abstract class DynamicMemoryLayout
     * @param checkBoundaryFieldChecks
     * @param recordSize
     * @param fieldSizes
     */
    DynamicMemoryLayout(bool checkBoundaryFieldChecks, uint64_t recordSize, std::vector<FIELD_SIZE>& fieldSizes);

    /**
     * @param fieldName
     * @return either field index for fieldName or empty optinal
     */
    std::optional<uint64_t> getFieldIndexFromName(std::string fieldName) const;

    virtual DynamicMemoryLayoutPtr copy() const = 0;

    /**
     * @return true if boundaries are actively being checked
     */
    bool isCheckBoundaryFieldChecks() const;

    /**
     * @return number of current records
     */
    uint64_t getRecordSize() const;

    /**
     * @return reference of field sizes vector
     */
    const std::vector<FIELD_SIZE>& getFieldSizes() const;

  protected:
    explicit DynamicMemoryLayout();
    bool checkBoundaryFieldChecks;
    uint64_t recordSize;
    std::vector<FIELD_SIZE> fieldSizes;
    std::map<std::string, uint64_t> nameFieldIndexMap;
};

}// namespace NES::NodeEngine::DynamicMemoryLayout
#endif//NES_DYNAMICMEMORYLAYOUT_HPP
