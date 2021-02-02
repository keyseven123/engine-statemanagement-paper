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
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <z3++.h>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor, OperatorId id)
    : sourceDescriptor(sourceDescriptor), UnaryOperatorNode(id) {}

bool SourceLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SourceLogicalOperatorNode>()->getId() == id;
}

bool SourceLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceOperator = rhs->as<SourceLogicalOperatorNode>();
        return sourceOperator->getSourceDescriptor()->equal(sourceDescriptor);
    }
    return false;
}

const std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << id << ")";
    return ss.str();
}

std::string SourceLogicalOperatorNode::getStringBasedSignature() { return "SOURCE(" + sourceDescriptor->getStreamName() + ")"; }

SourceDescriptorPtr SourceLogicalOperatorNode::getSourceDescriptor() { return sourceDescriptor; }

bool SourceLogicalOperatorNode::inferSchema() {
    inputSchema = sourceDescriptor->getSchema();
    outputSchema = sourceDescriptor->getSchema();
    return true;
}

void SourceLogicalOperatorNode::setSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    this->sourceDescriptor = sourceDescriptor;
}

void SourceLogicalOperatorNode::setProjectSchema(SchemaPtr schema) { projectSchema = schema; }

OperatorNodePtr SourceLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createSourceOperator(sourceDescriptor, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setSignature(signature);
    if (copy->instanceOf<SourceLogicalOperatorNode>()) {
        copy->as<SourceLogicalOperatorNode>()->setProjectSchema(projectSchema);
    }

    return copy;
}
}// namespace NES
