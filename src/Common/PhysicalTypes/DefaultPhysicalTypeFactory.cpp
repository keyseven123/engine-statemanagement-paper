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

#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Util/Logger.hpp>
namespace NES {

DefaultPhysicalTypeFactory::DefaultPhysicalTypeFactory() : PhysicalTypeFactory() {}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(DataTypePtr dataType) {
    if (dataType->isBoolean()) {
        return BasicPhysicalType::create(dataType, BasicPhysicalType::BOOLEAN);
    } else if (dataType->isInteger()) {
        return getPhysicalType(DataType::as<Integer>(dataType));
    } else if (dataType->isFloat()) {
        return getPhysicalType(DataType::as<Float>(dataType));
    } else if (dataType->isArray()) {
        return getPhysicalType(DataType::as<Array>(dataType));
    } else if (dataType->isFixedChar()) {
        return getPhysicalType(DataType::as<FixedChar>(dataType));
    } else if (dataType->isChar()) {
        return getPhysicalType(DataType::as<Char>(dataType));
    }
    NES_THROW_RUNTIME_ERROR("DefaultPhysicalTypeFactory: it was not possible to infer a physical type for: "
                            + dataType->toString());
    return nullptr;
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(IntegerPtr integer) {
    if (integer->getLowerBound() >= 0) {
        if (integer->getBits() <= 8) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_8);
        } else if (integer->getBits() <= 16) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_16);
        } else if (integer->getBits() <= 32) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_32);
        } else if (integer->getBits() <= 64) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_64);
        }
    } else {
        if (integer->getBits() <= 8) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_8);
        } else if (integer->getBits() <= 16) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_16);
        } else if (integer->getBits() <= 32) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_32);
        } else if (integer->getBits() <= 64) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_64);
        }
    }
    NES_THROW_RUNTIME_ERROR("DefaultPhysicalTypeFactory: it was not possible to infer a physical type for: "
                            + integer->toString());
    return nullptr;
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(FixedCharPtr charType) {
    auto componentType = getPhysicalType(DataTypeFactory::createChar());
    return ArrayPhysicalType::create(charType, charType->getLength(), componentType);
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(FloatPtr floatType) {
    if (floatType->getBits() <= 32) {
        return BasicPhysicalType::create(floatType, BasicPhysicalType::FLOAT);
    } else if (floatType->getBits() <= 64) {
        return BasicPhysicalType::create(floatType, BasicPhysicalType::DOUBLE);
    }
    NES_THROW_RUNTIME_ERROR("DefaultPhysicalTypeFactory: it was not possible to infer a physical type for: "
                            + floatType->toString());
    return nullptr;
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(ArrayPtr arrayType) {
    auto componentType = getPhysicalType(arrayType->getComponent());
    return ArrayPhysicalType::create(arrayType, arrayType->getLength(), componentType);
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(CharPtr charType) {
    return BasicPhysicalType::create(charType, BasicPhysicalType::CHAR);
}

}// namespace NES