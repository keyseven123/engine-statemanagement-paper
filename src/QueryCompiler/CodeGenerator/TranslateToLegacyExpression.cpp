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

#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <utility>
namespace NES {

namespace QueryCompilation {
TranslateToLegacyExpressionPtr TranslateToLegacyExpression::create() { return std::make_shared<TranslateToLegacyExpression>(); }

TranslateToLegacyExpression::TranslateToLegacyExpression() {}

/**
 * Translate the expression node into the corresponding user api expression of the legacy api.
 * To this end we first cast the expression node in the right subtype and then translate it.
 */
LegacyExpressionPtr TranslateToLegacyExpression::transformExpression(ExpressionNodePtr expression) {
    if (expression->instanceOf<LogicalExpressionNode>()) {
        // Translate logical expressions to the legacy representation
        return transformLogicalExpressions(expression);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // Translate arithmetical expressions to the legacy representation
        return transformArithmeticalExpressions(expression);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // Translate constant value expression node.
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return PredicateItem(value).copy();
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // Translate field read expression node.
        auto fieldReadExpression = expression->as<FieldAccessExpressionNode>();
        auto fieldName = fieldReadExpression->getFieldName();
        auto stamp = fieldReadExpression->getStamp();
        NES_DEBUG("TranslateToLegacyPhase: Translate FieldAccessExpressionNode: " << expression->toString());
        return Field(AttributeField::create(fieldName, stamp)).copy();
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this expression node: " << expression->toString());
    NES_NOT_IMPLEMENTED();
    ;
}

LegacyExpressionPtr TranslateToLegacyExpression::transformArithmeticalExpressions(ExpressionNodePtr expression) {
    if (expression->instanceOf<AddExpressionNode>()) {
        // Translate add expression node.
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto legacyLeft = transformExpression(addExpressionNode->getLeft());
        auto legacyRight = transformExpression(addExpressionNode->getRight());
        return Predicate(BinaryOperatorType::PLUS_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<SubExpressionNode>()) {
        // Translate sub expression node.
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto legacyLeft = transformExpression(subExpressionNode->getLeft());
        auto legacyRight = transformExpression(subExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MINUS_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // Translate mul expression node.
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto legacyLeft = transformExpression(mulExpressionNode->getLeft());
        auto legacyRight = transformExpression(mulExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MULTIPLY_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // Translate div expression node.
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto legacyLeft = transformExpression(divExpressionNode->getLeft());
        auto legacyRight = transformExpression(divExpressionNode->getRight());
        return Predicate(BinaryOperatorType::DIVISION_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<PowExpressionNode>()) {
        // Translate POWER expression node.
        auto powExpressionNode = expression->as<PowExpressionNode>();
        auto legacyLeft = transformExpression(powExpressionNode->getLeft());
        auto legacyRight = transformExpression(powExpressionNode->getRight());
        return Predicate(BinaryOperatorType::POWER_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<AbsExpressionNode>()) {
        // Translate ABS expression node.
        auto absExpressionNode = expression->as<AbsExpressionNode>();
        absExpressionNode->toString();
        (void) absExpressionNode;
        NES_FATAL_ERROR("TranslateToLegacyPhase: Unary expressions not supported in "
                        "legacy expressions: "
                        << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this arithmetical expression node: "
                    << expression->toString());
    NES_NOT_IMPLEMENTED();
}

LegacyExpressionPtr TranslateToLegacyExpression::transformLogicalExpressions(ExpressionNodePtr expression) {
    if (expression->instanceOf<AndExpressionNode>()) {
        // Translate and expression node.
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_AND_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<OrExpressionNode>()) {
        // Translate or expression node.
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto legacyLeft = transformExpression(orExpressionNode->getLeft());
        auto legacyRight = transformExpression(orExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_OR_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // Translate less expression node.
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto legacyLeft = transformExpression(lessExpressionNode->getLeft());
        auto legacyRight = transformExpression(lessExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THAN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // Translate less equals expression node.
        auto andExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THAN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // Translate greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto legacyLeft = transformExpression(greaterExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THAN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // Translate greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto legacyLeft = transformExpression(greaterEqualsExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterEqualsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THAN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // Translate equals expression node.
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto legacyLeft = transformExpression(equalsExpressionNode->getLeft());
        auto legacyRight = transformExpression(equalsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto const negateExpressionNode = expression->as<NegateExpressionNode>();
        (void) negateExpressionNode;
        NES_FATAL_ERROR("TranslateToLegacyPhase: Unary expressions not supported in "
                        "legacy expressions: "
                        << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this "
                    "logical expression node: "
                    << expression->toString());
    NES_NOT_IMPLEMENTED();
    ;
}
}// namespace QueryCompilation
}// namespace NES