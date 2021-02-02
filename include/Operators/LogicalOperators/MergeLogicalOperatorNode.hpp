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

#ifndef Merge_LOGICAL_OPERATOR_NODE_HPP
#define Merge_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief Merge operator, which contains an expression as a predicate.
 */
class MergeLogicalOperatorNode : public BinaryOperatorNode {
  public:
    explicit MergeLogicalOperatorNode(OperatorId id);
    ~MergeLogicalOperatorNode() = default;

    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    bool equal(const NodePtr rhs) const override;
    std::string getStringBasedSignature() override;
};
}// namespace NES
#endif// Merge_LOGICAL_OPERATOR_NODE_HPP