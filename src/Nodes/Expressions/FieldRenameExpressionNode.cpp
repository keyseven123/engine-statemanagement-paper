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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/InvalidFieldException.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <utility>

namespace NES {
FieldRenameExpressionNode::FieldRenameExpressionNode(std::string fieldName, std::string newFieldName, DataTypePtr datatype)
    : FieldAccessExpressionNode(datatype, fieldName), newFieldName(newFieldName){};

FieldRenameExpressionNode::FieldRenameExpressionNode(FieldRenameExpressionNode* other)
    : FieldRenameExpressionNode(other->fieldName, other->getFieldName(), other->stamp){};

ExpressionNodePtr FieldRenameExpressionNode::create(std::string fieldName, std::string newFieldName, DataTypePtr datatype) {
    return std::make_shared<FieldRenameExpressionNode>(FieldRenameExpressionNode(fieldName, newFieldName, datatype));
}

bool FieldRenameExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldRenameExpressionNode>()) {
        auto otherFieldRead = rhs->as<FieldRenameExpressionNode>();
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp->isEquals(stamp);
    }
    return false;
}

const std::string FieldRenameExpressionNode::getNewFieldName() { return newFieldName; }

const std::string FieldRenameExpressionNode::toString() const {
    return "FieldRenameExpression(" + fieldName + ": " + stamp->toString() + ")";
}

void FieldRenameExpressionNode::inferStamp(SchemaPtr schema) {
    //Detect if user has provided fully qualified name
    FieldAccessExpressionNode::inferStamp(schema);
    auto fieldAttribute = schema->hasFieldName(fieldName);
    if (!fieldAttribute) {
        throw InvalidFieldException("Original field with name " + fieldName + " does not exists in the schema "
                                    + schema->toString());
    }

    auto newFieldAttribute = schema->hasFieldName(newFieldName);
    if (newFieldAttribute) {
        NES_ERROR("FieldRenameExpressionNode: The new field name" + newFieldName + " already exists in the input schema "
                  + schema->toString() + ". Can't use the name of an existing field.");
        throw InvalidFieldException("New field with name " + newFieldName + " already exists in the schema "
                                    + schema->toString());
    }

    //Check if the new field is already fully qualified if not then make it fully qualified
    if (newFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        newFieldName = fieldName.substr(0, fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName) {
        NES_WARNING("FieldRenameExpressionNode: Both existing and new fields are same: existing: " + fieldName
                    + " new field name: " + newFieldName);
    }

    // assign the stamp of this field access with the type of this field.
    stamp = fieldAttribute->getDataType();
}

ExpressionNodePtr FieldRenameExpressionNode::copy() {
    return std::make_shared<FieldRenameExpressionNode>(FieldRenameExpressionNode(this));
}

}// namespace NES