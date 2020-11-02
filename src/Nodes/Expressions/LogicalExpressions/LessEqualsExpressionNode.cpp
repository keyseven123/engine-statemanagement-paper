
#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
namespace NES {
LessEqualsExpressionNode::LessEqualsExpressionNode() : LogicalBinaryExpressionNode(){};

LessEqualsExpressionNode::LessEqualsExpressionNode(LessEqualsExpressionNode* other) : LogicalBinaryExpressionNode(other) {}

ExpressionNodePtr LessEqualsExpressionNode::create(const ExpressionNodePtr left,
                                                   const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessEqualsExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessEqualsExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<LessEqualsExpressionNode>()) {
        auto other = rhs->as<LessEqualsExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

const std::string LessEqualsExpressionNode::toString() const {
    return "LessThenNode(" + stamp->toString() + ")";
}
ExpressionNodePtr LessEqualsExpressionNode::copy() {
    return std::make_shared<LessEqualsExpressionNode>(LessEqualsExpressionNode(this));
}

}// namespace NES