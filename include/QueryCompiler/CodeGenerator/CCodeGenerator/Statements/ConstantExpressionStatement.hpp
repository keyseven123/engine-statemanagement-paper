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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES {
namespace QueryCompilation {

class ConstantExpressionStatement : public ExpressionStatment {
  public:
    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    ConstantExpressionStatement(GeneratableValueTypePtr val);

    virtual ~ConstantExpressionStatement();

  private:
    GeneratableValueTypePtr constantValue;
};

typedef ConstantExpressionStatement Constant;
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
