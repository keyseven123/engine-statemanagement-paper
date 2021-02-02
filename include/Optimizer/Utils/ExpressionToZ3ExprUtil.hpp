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

#ifndef NES_OPTIMIZE_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP
#define NES_OPTIMIZE_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP

#include <memory>

namespace z3 {
class expr;
typedef std::shared_ptr<expr> ExprPtr;

class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {
class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;
}// namespace NES

namespace NES::Optimizer {

class Z3ExprAndFieldMap;
typedef std::shared_ptr<Z3ExprAndFieldMap> Z3ExprAndFieldMapPtr;
/**
 * @brief This class is responsible for taking input as a logical expression and generating an equivalent Z3 expression.
 */
class ExpressionToZ3ExprUtil {

  public:
    /**
     * @brief Convert input expression into an equivalent Z3 expressions
     * @param expression: the input expression
     * @param context: Z3 context
     * @return returns Z3 expression and field map
     */
    static Z3ExprAndFieldMapPtr createForExpression(ExpressionNodePtr expression, z3::ContextPtr context);

  private:
    /**
     * @brief Convert input Logical expression into an equivalent Z3 expression
     * @param expression: the input logical expression
     * @param context: the Z3 context
     * @return returns Z3 expression and field map
     */
    static Z3ExprAndFieldMapPtr createForLogicalExpressions(ExpressionNodePtr expression, z3::ContextPtr context);

    /**
     * @brief Convert input arithmetic expression into an equivalent Z3 expression
     * @param expression: the input arithmetic expression
     * @param context: the Z3 context
     * @return returns Z3 expression and field map
     */
    static Z3ExprAndFieldMapPtr createForArithmeticalExpressions(ExpressionNodePtr expression, z3::ContextPtr context);
};
}// namespace NES::Optimizer

#endif//NES_OPTIMIZE_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP
