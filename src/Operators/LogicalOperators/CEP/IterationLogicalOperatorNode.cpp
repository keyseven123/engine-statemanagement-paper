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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

IterationLogicalOperatorNode::IterationLogicalOperatorNode(uint64_t minIterations, uint64_t maxIterations, uint64_t id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), minIterations(minIterations), maxIterations(maxIterations) {}

uint64_t IterationLogicalOperatorNode::getMinIterations() { return minIterations; }

uint64_t IterationLogicalOperatorNode::getMaxIterations() { return maxIterations; }

bool IterationLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<IterationLogicalOperatorNode>()->getId() == id;
}

bool IterationLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<IterationLogicalOperatorNode>()) {
        auto iteration = rhs->as<IterationLogicalOperatorNode>();
        return (minIterations == iteration->minIterations && maxIterations == iteration->maxIterations);
    }
    return false;
};

const std::string IterationLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Iteration(" << id << ", minimum iteration=" << minIterations << ", maximum iteration=" << maxIterations << ")";
    return ss.str();
}

bool IterationLogicalOperatorNode::inferSchema() {
    // infer the default input and output schema
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }
    return true;
}

OperatorNodePtr IterationLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createCEPIterationOperator(minIterations, maxIterations, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

void IterationLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("IterationLogicalOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty(), "IterationLogicalOperatorNode: Iteration should have children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    signatureStream << "Iteration(" << minIterations << ", " << maxIterations << ")."
                    << children[0]->as<LogicalOperatorNode>()->getStringSignature();
    setStringSignature(signatureStream.str());
}
}// namespace NES
