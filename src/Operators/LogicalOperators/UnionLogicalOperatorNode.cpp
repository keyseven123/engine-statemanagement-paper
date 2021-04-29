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

#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

UnionLogicalOperatorNode::UnionLogicalOperatorNode(OperatorId id) : OperatorNode(id), LogicalBinaryOperatorNode(id) {}

bool UnionLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<UnionLogicalOperatorNode>()->getId() == id;
}

const std::string UnionLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "unionWith(" << id << ")";
    return ss.str();
}

bool UnionLogicalOperatorNode::inferSchema() {
    if (!LogicalBinaryOperatorNode::inferSchema()) {
        return false;
    }

    leftInputSchema->clear();
    rightInputSchema->clear();
    if (distinctSchemas.size() == 1) {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[0]);
    } else {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[1]);
    }

    if (!leftInputSchema->hasEqualTypes(rightInputSchema)) {
        NES_ERROR("Found Schema mismatch for left and right schema types. Left schema " + leftInputSchema->toString()
                  + " and Right schema " + rightInputSchema->toString());
        throw TypeInferenceException("Found Schema mismatch for left and right schema types. Left schema "
                                     + leftInputSchema->toString() + " and Right schema " + rightInputSchema->toString());
    }

    //Copy the schema of left input
    outputSchema->clear();
    outputSchema->copyFields(leftInputSchema);
    return true;
}

OperatorNodePtr UnionLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createUnionOperator(id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setStringSignature(stringSignature);
    return copy;
}

bool UnionLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<UnionLogicalOperatorNode>()) {
        return true;
    }
    return false;
}
void UnionLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("UnionLogicalOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "UnionLogicalOperatorNode: Union should have 2 children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "UNION(";
    signatureStream << children[0]->as<LogicalOperatorNode>()->getStringSignature() + ").";
    signatureStream << children[1]->as<LogicalOperatorNode>()->getStringSignature();
    setStringSignature(signatureStream.str());
}

}// namespace NES