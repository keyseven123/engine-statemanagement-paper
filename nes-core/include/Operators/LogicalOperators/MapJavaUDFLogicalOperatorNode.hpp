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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_MAPJAVAUDFLOGICALOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_MAPJAVAUDFLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/JavaUDFLogicalOperator.hpp>

namespace NES {

/**
 * Logical operator node for a map operation which uses a Java UDF.
 *
 * The operation completely replaces the stream tuple based on the result of the Java UDF method. Therefore, the output schema is
 * determined by the UDF method signature.
 */
class MapJavaUDFLogicalOperatorNode : public JavaUDFLogicalOperator {
  public:
    /**
     * Construct a MapUdfLogicalOperatorNode.
     * @param javaUdfDescriptor The descriptor of the Java UDF used in the map operation.
     * @param id The ID of the operator.
     */
    MapJavaUDFLogicalOperatorNode(const Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor, OperatorId id);

    /**
     * @see Node#toString
     */
    std::string toString() const override;

    /**
     * @see OperatorNode#copy
     */
    OperatorNodePtr copy() override;

    /**
     * @see Node#equal
     *
     * Two MapUdfLogicalOperatorNode are equal when the wrapped JavaUDFDescriptor are equal.
     */
    [[nodiscard]] bool equal(const NodePtr& other) const override;

    /**
     * @see Node#isIdentical
     */
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_MAPJAVAUDFLOGICALOPERATORNODE_HPP_
