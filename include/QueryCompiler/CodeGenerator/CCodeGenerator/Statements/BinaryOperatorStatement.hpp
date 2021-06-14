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

#pragma once

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES {
namespace QueryCompilation {
const CodeExpressionPtr toCodeExpression(const BinaryOperatorType& type);

class BinaryOperatorStatement : public ExpressionStatment {
  public:
    BinaryOperatorStatement(ExpressionStatmentPtr lhs,
                            const BinaryOperatorType& op,
                            ExpressionStatmentPtr rhs,
                            BracketMode bracket_mode = NO_BRACKETS);

    BinaryOperatorStatement(const ExpressionStatment& lhs,
                            const BinaryOperatorType& op,
                            const ExpressionStatment& rhs,
                            BracketMode bracket_mode = NO_BRACKETS);

    BinaryOperatorStatement
    addRight(const BinaryOperatorType& op, const VarRefStatement& rhs, BracketMode bracket_mode = NO_BRACKETS);

    StatementPtr assignToVariable(const VarRefStatement& lhs);

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    //  BinaryOperatorStatement operator [](const ExpressionStatment &ref){
    //    return BinaryOperatorStatement(*this, ARRAY_REFERENCE_OP, ref);
    //  }

    virtual ~BinaryOperatorStatement();

  private:
    ExpressionStatmentPtr lhs_;
    ExpressionStatmentPtr rhs_;
    BinaryOperatorType op_;
    BracketMode bracket_mode_;
};

/** \brief small utility operator overloads to make code generation simpler and */

BinaryOperatorStatement assign(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator==(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator!=(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator<(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator<=(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator>(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator>=(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator+(const ExpressionStatment&, const ExpressionStatment& rhs);

BinaryOperatorStatement operator-(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator*(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator/(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator%(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator&&(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator||(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator&(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator|(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator^(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator<<(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator>>(const ExpressionStatment& lhs, const ExpressionStatment& rhs);
}// namespace QueryCompilation
}// namespace NES
