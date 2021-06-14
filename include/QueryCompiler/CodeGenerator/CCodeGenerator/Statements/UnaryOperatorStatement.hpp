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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES {
namespace QueryCompilation {
const CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type);

class UnaryOperatorStatement : public ExpressionStatment {
  public:
    UnaryOperatorStatement(const ExpressionStatment& expr, const UnaryOperatorType& op, BracketMode bracket_mode = NO_BRACKETS);

    StatementType getStamentType() const override;

    const CodeExpressionPtr getCode() const override;

    const ExpressionStatmentPtr copy() const override;

    ~UnaryOperatorStatement() override;

  private:
    ExpressionStatmentPtr expr_;
    UnaryOperatorType op_;
    BracketMode bracket_mode_;
};

UnaryOperatorStatement operator&(const ExpressionStatment& ref);

UnaryOperatorStatement operator*(const ExpressionStatment& ref);

UnaryOperatorStatement operator++(const ExpressionStatment& ref);

UnaryOperatorStatement operator--(const ExpressionStatment& ref);

UnaryOperatorStatement operator~(const ExpressionStatment& ref);

UnaryOperatorStatement operator!(const ExpressionStatment& ref);

UnaryOperatorStatement sizeOf(const ExpressionStatment& ref);
}// namespace QueryCompilation
}// namespace NES
