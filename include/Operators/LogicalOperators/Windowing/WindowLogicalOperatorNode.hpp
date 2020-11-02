#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>

namespace NES {

class WindowLogicalOperatorNode : public WindowOperatorNode {
  public:
    WindowLogicalOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
    virtual bool inferSchema();
};

}// namespace NES
#endif// WINDOW_LOGICAL_OPERATOR_NODE_HPP
