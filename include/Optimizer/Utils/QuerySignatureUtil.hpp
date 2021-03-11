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

#ifndef NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP
#define NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP

#include <Operators/OperatorForwardDeclaration.hpp>
#include <memory>

namespace z3 {

class expr;
typedef std::shared_ptr<expr> ExprPtr;

class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

}// namespace NES

namespace NES::Optimizer {

class QuerySignature;
typedef std::shared_ptr<QuerySignature> QuerySignaturePtr;

/**
 * @brief This class is responsible for creating the Query Plan Signature for the input operator.
 * The query plan is composed of the input operator and all its upstream child operators.
 */
class QuerySignatureUtil {
  public:
    /**
     * @brief Convert input operator into an equivalent logical expression
     * @param context: the context of Z3
     * @param operatorNode: the input operator
     * @return the object representing signature created by the operator and its children
     */
    static QuerySignaturePtr createQuerySignatureForOperator(z3::ContextPtr context, OperatorNodePtr operatorNode);

  private:
    /**
     * @brief Compute a query signature for Source operator
     * @param context: z3 context
     * @param sourceOperator: the source operator
     * @return Signature based on source operator
     */
    static QuerySignaturePtr createQuerySignatureForSource(z3::ContextPtr context, SourceLogicalOperatorNodePtr sourceOperator);

    /**
     * @brief Compute a query signature for Project operator
     * @param projectOperator: the project operator
     * @return Signature based on project operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForProject(ProjectionLogicalOperatorNodePtr projectOperator);

    /**
     * @brief Compute a query signature for Map operator
     * @param context: z3 context
     * @param mapOperator: the map operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForMap(z3::ContextPtr context, MapLogicalOperatorNodePtr mapOperator);

    /**
     * @brief Compute a query signature for Filter operator
     * @param context: z3 context
     * @param filterOperator: the Filter operator
     * @return Signature based on filter operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForFilter(z3::ContextPtr context, FilterLogicalOperatorNodePtr filterOperator);

    /**
     * @brief Compute a query signature for window operator
     * @param context: z3 context
     * @param windowOperator: the window operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForWindow(z3::ContextPtr context, WindowLogicalOperatorNodePtr windowOperator);

    /**
     * @brief compute a signature for join operator
     * @param context: z3 context
     * @param joinOperator: the join operator
     * @return Signature based on join operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForJoin(z3::ContextPtr context, JoinLogicalOperatorNodePtr joinOperator);

    /**
     * @brief compute a signature for watermark operator
     * @param context: z3 context
     * @param watermarkAssignerOperator: the watermark operator
     * @return Signature based on watermark operator and its child signature
     */
    static QuerySignaturePtr createQuerySignatureForWatermark(z3::ContextPtr context,
                                                              WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerOperator);

    /**
     * @brief Compute a signature for Union operator
     * @param context: z3 context
     * @param unionOperator: the union operator
     * @return Signature based on union and its child signatures
     */
    static QuerySignaturePtr createQuerySignatureForUnion(z3::ContextPtr context, UnionLogicalOperatorNodePtr unionOperator);
};
}// namespace NES::Optimizer

#endif//NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP
