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

#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Util/Logger.hpp>
#include <memory>

namespace NES::NodeEngine {

/**
 * @brief This is the base class to represent a physical field.
 * It can be a BasicPhysicalField for value types or a ArrayPhyiscalField for arrays.
 */
class PhysicalField : public std::enable_shared_from_this<PhysicalField> {
  public:
    PhysicalField(uint64_t bufferOffset);
    ~PhysicalField();
    /**
     * @brief Casts the PhysicalField to a ValueField of a specific type.
     * @throws IllegalArgumentException if the type of the field dose not match the expected template.
     * @tparam ValueType type of the physical field
     * @return std::shared_ptr<BasicPhysicalField<ValueType>>
     */
    template<class ValueType>
    std::shared_ptr<BasicPhysicalField<ValueType>> asValueField() {
        if (!isFieldOfValueType<ValueType>()) {
            NES_FATAL_ERROR("This field is not of that type " << typeid(ValueType).name());
            throw IllegalArgumentException("This field is not of that type");
        }
        return std::static_pointer_cast<BasicPhysicalField<ValueType>>(this->shared_from_this());
    }

    /**
     * @brief casts the PhysicalField to an ArrayPhysicalField.
     * @throws IllegalArgumentException if the type of the field is not a ArrayPhysicalField.
     * @return std::shared_ptr<ArrayPhysicalField>
     */
    virtual std::shared_ptr<ArrayPhysicalField> asArrayField();

    /**
     * @brief Checks if the type of the BasicPhysicalField corresponds to a specific generic type.
     * @tparam ValueType
     * @return
     */
    template<class ValueType>
    bool isFieldOfValueType() {
        if (std::dynamic_pointer_cast<BasicPhysicalField<ValueType>>(this->shared_from_this())) {
            return true;
        };
        return false;
    }

  protected:
    /**
     * @brief Field offset in the underling memory buffer.
     */
    uint64_t bufferOffset;
};

PhysicalFieldPtr createArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset);

}// namespace NES::NodeEngine
#endif//INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_
