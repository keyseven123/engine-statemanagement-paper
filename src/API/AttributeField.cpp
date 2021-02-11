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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <sstream>

namespace NES {

AttributeField::AttributeField(const std::string& name, DataTypePtr dataType) : name(name), dataType(dataType) {}

AttributeFieldPtr AttributeField::create(std::string name, DataTypePtr dataType) {
    return std::make_shared<AttributeField>(name, dataType);
}

std::string AttributeField::getName() const { return name; }

void AttributeField::setName(std::string newName) { this->name = newName; }

DataTypePtr AttributeField::getDataType() const { return dataType; }

const std::string AttributeField::toString() const {
    std::stringstream ss;
    ss << name << ":" << dataType->toString();
    return ss.str();
}

bool AttributeField::isEqual(AttributeFieldPtr attr) {
    if (!attr)
        return false;
    return (attr->name == name) && (attr->dataType->isEquals(attr->getDataType()));
}

AttributeFieldPtr AttributeField::copy() { return create(name, dataType); }

}// namespace NES
