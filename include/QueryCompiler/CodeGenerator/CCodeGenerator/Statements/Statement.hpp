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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <memory>
#include <string>
#include <utility>

namespace NES {
namespace QueryCompilation {
enum StatementType {
    RETURN_STMT,
    IF_STMT,
    IF_ELSE_STMT,
    FOR_LOOP_STMT,
    FUNC_CALL_STMT,
    VAR_REF_STMT,
    VAR_DEC_STMT,
    CONSTANT_VALUE_EXPR_STMT,
    BINARY_OP_STMT,
    UNARY_OP_STMT,
    COMPOUND_STMT
};

enum BracketMode { NO_BRACKETS, BRACKETS };

class Statement;
typedef std::shared_ptr<Statement> StatementPtr;

class Statement {
  public:
    /**
     * @brief method to get the statement type
     * @return StatementType
     */
    virtual StatementType getStamentType() const = 0;

    /**
     * @brief method to get the code as code expressions
     * @return CodeExpressionPtr
     */
    virtual const CodeExpressionPtr getCode() const = 0;

    /**
     * @brief method to create a copy of this statement
     * @return copy of statement
     */
    virtual const StatementPtr createCopy() const = 0;

    /** @brief virtual copy constructor */
    virtual ~Statement();
};

class AssignmentStatement {
  public:
    VariableDeclaration lhs_tuple_var;
    VariableDeclaration lhs_field_var;
    VariableDeclaration lhs_index_var;
    VariableDeclaration rhs_tuple_var;
    VariableDeclaration rhs_field_var;
    VariableDeclaration rhs_index_var;
};
}// namespace QueryCompilation
}// namespace NES
