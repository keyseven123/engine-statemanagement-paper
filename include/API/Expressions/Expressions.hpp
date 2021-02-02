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

#ifndef NES_INCLUDE_API_EXPRESSIONS_HPP
#define NES_INCLUDE_API_EXPRESSIONS_HPP

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief This file contains the user facing api to create expression nodes in a fluent and easy way.
 */

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class FieldAssignmentExpressionNode;
typedef std::shared_ptr<FieldAssignmentExpressionNode> FieldAssignmentExpressionNodePtr;

/**
 * @brief A expression item represents the leaf in an expression tree.
 * It is converted to an constant value expression or a field access expression.
 */
class ExpressionItem {
  public:
    ExpressionItem(int8_t value);
    ExpressionItem(uint8_t value);
    ExpressionItem(int16_t value);
    ExpressionItem(uint16_t value);
    ExpressionItem(int32_t value);
    ExpressionItem(uint32_t value);
    ExpressionItem(int64_t value);
    ExpressionItem(uint64_t value);
    ExpressionItem(float value);
    ExpressionItem(double value);
    ExpressionItem(bool value);
    ExpressionItem(const char* value);
    ExpressionItem(ValueTypePtr value);
    ExpressionItem(ExpressionNodePtr ref);

    FieldAssignmentExpressionNodePtr operator=(ExpressionItem);
    FieldAssignmentExpressionNodePtr operator=(ExpressionNodePtr);

    /**
     * @brief Gets the expression node of this expression item.
     */
    ExpressionNodePtr getExpressionNode();

    /**
     * @brief Rename the expression item
     * @param name : the new name
     * @return the updated expression item
     */
    ExpressionItem rename(std::string name);

  private:
    ExpressionNodePtr expression;
};

/**
 * @brief Attribute(name) allows the user to reference a field in his expression.
 * Attribute("f1") < 10
 * todo rename to field if conflict with legacy code is resolved.
 * @param fieldName
 */
ExpressionItem Attribute(std::string name);

/**
 * @brief Attribute(name, type) allows the user to reference a field, with a specific type in his expression.
 * Field("f1", Int) < 10.
 * todo remove this case if we added type inference at runtime from the operator tree.
 * todo rename to field if conflict with legacy code is resolved.
 * @param fieldName, type
 */
ExpressionItem Attribute(std::string name, BasicType type);

}//end of namespace NES
#endif
