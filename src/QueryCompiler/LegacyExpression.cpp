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

#include <sstream>
#include <string>

#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedCode.hpp>

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <QueryCompiler/LegacyExpression.hpp>
#include <Util/Logger.hpp>
namespace NES {

Predicate::Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right,
                     const std::string& functionCallOverload, bool bracket)
    : op(op), left(left), right(right), bracket(bracket), functionCallOverload(functionCallOverload) {}

Predicate::Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right,
                     bool bracket)
    : op(op), left(left), right(right), bracket(bracket), functionCallOverload("") {}

UserAPIExpressionPtr Predicate::copy() const { return std::make_shared<Predicate>(*this); }

const ExpressionStatmentPtr Predicate::generateCode(GeneratedCodePtr& code, NES::RecordHandlerPtr recordHandler) const {
    if (functionCallOverload.empty()) {
        if (bracket)
            return BinaryOperatorStatement(*(left->generateCode(code, recordHandler)), op,
                                           *(right->generateCode(code, recordHandler)), BRACKETS)
                .copy();
        return BinaryOperatorStatement(*(left->generateCode(code, recordHandler)), op,
                                       *(right->generateCode(code, recordHandler)))
            .copy();
    } else {
        FunctionCallStatement expr = FunctionCallStatement(functionCallOverload);
        expr.addParameter(left->generateCode(code, recordHandler));
        expr.addParameter(right->generateCode(code, recordHandler));
        auto tf = CompilerTypesFactory();
        if (bracket)
            return BinaryOperatorStatement(expr, op,
                                           (ConstantExpressionStatement(tf.createValueType(
                                               DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "0")))),
                                           BRACKETS)
                .copy();
        return BinaryOperatorStatement(expr, op,
                                       (ConstantExpressionStatement(tf.createValueType(
                                           DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "0")))))
            .copy();
    }
}

const ExpressionStatmentPtr PredicateItem::generateCode(GeneratedCodePtr&, NES::RecordHandlerPtr recordHandler) const {
    if (attribute) {
        //checks if the predicate field is contained in the current stream record.
        if (recordHandler->hasAttribute(attribute->getName())) {
            return recordHandler->getAttribute(attribute->getName());
        } else {
            NES_FATAL_ERROR("UserAPIExpression: Could not Retrieve Attribute from record handler!");
        }
    } else if (value) {
        // todo remove if compiler refactored
        auto tf = CompilerTypesFactory();
        return ConstantExpressionStatement(tf.createValueType(value)).copy();
    }
    return nullptr;
}

const std::string Predicate::toString() const {
    std::stringstream stream;
    if (bracket)
        stream << "(";
    stream << left->toString() << " " << ::NES::toCodeExpression(op)->code_ << " " << right->toString() << " ";
    if (bracket)
        stream << ")";
    return stream.str();
}

bool Predicate::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const NES::Predicate&>(_rhs);
        if ((left == nullptr && rhs.left == nullptr) || (left->equals(*rhs.left.get()))) {
            if ((right == nullptr && rhs.right == nullptr) || (right->equals(*rhs.right.get()))) {
                return op == rhs.op && bracket == rhs.bracket && functionCallOverload == rhs.functionCallOverload;
            }
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

PredicateItem::PredicateItem(AttributeFieldPtr attribute) : mutation(PredicateItemMutation::ATTRIBUTE), attribute(attribute) {}
BinaryOperatorType Predicate::getOperatorType() const { return op; }

const UserAPIExpressionPtr Predicate::getLeft() const { return left; }

const UserAPIExpressionPtr Predicate::getRight() const { return right; }

PredicateItem::PredicateItem(ValueTypePtr value) : mutation(PredicateItemMutation::VALUE), value(value) {}

PredicateItem::PredicateItem(int8_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint8_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), std::to_string(val))) {}
PredicateItem::PredicateItem(int16_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint16_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(val))) {}
PredicateItem::PredicateItem(int32_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint32_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(val))) {}
PredicateItem::PredicateItem(int64_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint64_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(val))) {}
PredicateItem::PredicateItem(float val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createFloat(), std::to_string(val))) {}
PredicateItem::PredicateItem(double val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createDouble(), std::to_string(val))) {}
PredicateItem::PredicateItem(bool val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createBoolean(), std::to_string(val))) {}
PredicateItem::PredicateItem(char val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createChar(), std::to_string(val))) {}
PredicateItem::PredicateItem(const char* val)
    : mutation(PredicateItemMutation::VALUE), value(DataTypeFactory::createFixedCharValue(val)) {}

const std::string PredicateItem::toString() const {
    switch (mutation) {
        case PredicateItemMutation::ATTRIBUTE: return attribute->toString();
        case PredicateItemMutation::VALUE: {
            auto tf = CompilerTypesFactory();
            return tf.createValueType(value)->getCodeExpression()->code_;
        }
    }
    return "";
}

bool PredicateItem::isStringType() const { return (getDataTypePtr()->isFixedChar()); }

const DataTypePtr PredicateItem::getDataTypePtr() const {
    if (attribute)
        return attribute->getDataType();
    return value->getType();
};

UserAPIExpressionPtr PredicateItem::copy() const { return std::make_shared<PredicateItem>(*this); }

bool PredicateItem::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const NES::PredicateItem&>(_rhs);
        if ((attribute == nullptr && rhs.attribute == nullptr) || attribute->isEqual(rhs.attribute)) {
            if ((value == nullptr && rhs.value == nullptr) || (value->isEquals(rhs.value))) {
                return mutation == rhs.mutation;
            }
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

const ValueTypePtr& PredicateItem::getValue() const { return value; }

Field::Field(AttributeFieldPtr field) : PredicateItem(field), _name(field->getName()) {}

const PredicatePtr createPredicate(const LegacyExpression& expression) {
    PredicatePtr value = std::dynamic_pointer_cast<Predicate>(expression.copy());
    if (!value) {
        NES_ERROR("UserAPIExpression is not a predicate");
    }
    return value;
}

Predicate operator==(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator!=(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::UNEQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator>(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::GREATER_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator<(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LESS_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator>=(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::GREATER_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator<=(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LESS_THAN_EQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator+(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::PLUS_OP, lhs.copy(), rhs.copy());
}
Predicate operator-(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::MINUS_OP, lhs.copy(), rhs.copy());
}
Predicate operator*(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::MULTIPLY_OP, lhs.copy(), rhs.copy());
}
Predicate operator/(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::DIVISION_OP, lhs.copy(), rhs.copy());
}
Predicate operator%(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::MODULO_OP, lhs.copy(), rhs.copy());
}
Predicate operator&&(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LOGICAL_AND_OP, lhs.copy(), rhs.copy());
}
Predicate operator||(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LOGICAL_OR_OP, lhs.copy(), rhs.copy());
}
Predicate operator&(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_AND_OP, lhs.copy(), rhs.copy());
}
Predicate operator|(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_OR_OP, lhs.copy(), rhs.copy());
}
Predicate operator^(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_XOR_OP, lhs.copy(), rhs.copy());
}
Predicate operator<<(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_LEFT_SHIFT_OP, lhs.copy(), rhs.copy());
}
Predicate operator>>(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_RIGHT_SHIFT_OP, lhs.copy(), rhs.copy());
}

Predicate operator==(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return (lhs == dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator!=(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator!=(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator>(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator<(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>=(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator>=(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<=(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator<=(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator+(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator+(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator-(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator-(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator*(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator*(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator/(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator/(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator%(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator%(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator&&(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator&&(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator||(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator||(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator|(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator|(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator^(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator^(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<<(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator<<(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>>(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator>>(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}

Predicate operator==(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return (dynamic_cast<const LegacyExpression&>(lhs) == rhs);
}
Predicate operator!=(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator!=(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator>(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator>(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator<(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator<(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator>=(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator>=(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator<=(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator<=(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator+(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator+(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator-(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator-(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator*(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator*(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator/(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator/(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator%(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator%(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator&&(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator&&(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator||(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator||(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator|(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator|(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator^(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator^(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator<<(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator<<(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator>>(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator>>(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}

/**
 * Operator overload includes String compare by define a function-call-overload in the code generation process
 * @param lhs
 * @param rhs
 * @return
 */
Predicate operator==(const PredicateItem& lhs, const PredicateItem& rhs) {
    //possible use of memcmp when arraytypes equal with length is equal...
    int checktype = lhs.isStringType();
    checktype += rhs.isStringType();
    if (checktype == 1)
        NES_ERROR("NOT COMPARABLE TYPES");
    if (checktype == 2)
        return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy(), "strcmp", false);
    return (dynamic_cast<const LegacyExpression&>(lhs) == dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator!=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator!=(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>=(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<=(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator+(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator+(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator-(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator-(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator*(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator*(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator/(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator/(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator%(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric())
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator%(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator&&(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator&&(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator||(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator||(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator|(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator|(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator^(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator^(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<<(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<<(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>>(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>>(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
}//end namespace NES
