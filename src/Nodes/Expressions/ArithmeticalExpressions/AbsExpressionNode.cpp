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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <cmath>

namespace NES {

AbsExpressionNode::AbsExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

AbsExpressionNode::AbsExpressionNode(AbsExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr AbsExpressionNode::create(const ExpressionNodePtr child) {
    auto absNode = std::make_shared<AbsExpressionNode>(child->getStamp());
    absNode->setChild(child);
    return absNode;
}

void AbsExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    // increase lower bound to 0
    if (this->stamp->isFloat()) {
        auto stamp = DataType::as<Float>(this->stamp);
        auto newLowerBound = fmax(0, stamp->lowerBound);
        this->stamp = DataTypeFactory::createFloat(stamp->getBits(), newLowerBound, stamp->upperBound);
    } else if (this->stamp->isInteger()) {
        auto stamp = DataType::as<Integer>(this->stamp);
        auto newLowerBound = fmax(0, stamp->lowerBound);
        this->stamp = DataTypeFactory::createInteger(stamp->getBits(), newLowerBound, stamp->upperBound);
    }

    NES_TRACE("AbsExpressionNode: increased the lower bound of stamp to 0: " << toString());
}

bool AbsExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<AbsExpressionNode>()) {
        auto otherAbsNode = rhs->as<AbsExpressionNode>();
        return child()->equal(otherAbsNode->child());
    }
    return false;
}

const std::string AbsExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ABS(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr AbsExpressionNode::copy() { return std::make_shared<AbsExpressionNode>(AbsExpressionNode(this)); }

}// namespace NES