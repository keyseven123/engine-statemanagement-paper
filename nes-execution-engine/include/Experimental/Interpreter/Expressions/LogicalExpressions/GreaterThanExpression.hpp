/*
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
#include <Nautilus/Interface/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_GREATERTHANEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_GREATERTHANEXPRESSION_HPP_

namespace NES::ExecutionEngine::Experimental::Interpreter {

class GreaterThanExpression : public Expression {
  private:
    ExpressionPtr leftSubExpression;
    ExpressionPtr rightSubExpression;

  public:
    GreaterThanExpression(ExpressionPtr leftSubExpression, ExpressionPtr rightSubExpression);
    Value<> execute(Record& record) override;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_GREATERTHANEXPRESSION_HPP_