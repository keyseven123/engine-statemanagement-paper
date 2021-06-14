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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <memory>

namespace NES::Windowing {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class WindowOperatorHandler;
typedef std::shared_ptr<WindowOperatorHandler> WindowOperatorHandlerPtr;

}// namespace NES::Windowing

namespace NES::Join {
class LogicalJoinDefinition;
typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;

class JoinOperatorHandler;
typedef std::shared_ptr<JoinOperatorHandler> JoinOperatorHandlerPtr;
}// namespace NES::Join
namespace NES {

class LogicalOperatorNode;
typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class UnaryOperatorNode;
typedef std::shared_ptr<UnaryOperatorNode> UnaryOperatorNodePtr;

class LogicalUnaryOperatorNode;
typedef std::shared_ptr<LogicalUnaryOperatorNode> LogicalUnaryOperatorNodePtr;

class BinaryOperatorNode;
typedef std::shared_ptr<BinaryOperatorNode> BinaryOperatorNodePtr;

class LogicalBinaryOperatorNode;
typedef std::shared_ptr<LogicalBinaryOperatorNode> LogicalBinaryOperatorNodePtr;

class ExchangeOperatorNode;
typedef std::shared_ptr<ExchangeOperatorNode> ExchangeOperatorNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class WindowOperatorNode;
typedef std::shared_ptr<WindowOperatorNode> WindowOperatorNodePtr;

class JoinLogicalOperatorNode;
typedef std::shared_ptr<JoinLogicalOperatorNode> JoinLogicalOperatorNodePtr;

class FieldAssignmentExpressionNode;
typedef std::shared_ptr<FieldAssignmentExpressionNode> FieldAssignmentExpressionNodePtr;

class ConstantValueExpressionNode;
typedef std::shared_ptr<ConstantValueExpressionNode> ConstantValueExpressionNodePtr;

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

class BroadcastLogicalOperatorNode;
typedef std::shared_ptr<BroadcastLogicalOperatorNode> BroadcastLogicalOperatorNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class WatermarkAssignerLogicalOperatorNode;
typedef std::shared_ptr<WatermarkAssignerLogicalOperatorNode> WatermarkAssignerLogicalOperatorNodePtr;

class CentralWindowOperator;
typedef std::shared_ptr<CentralWindowOperator> CentralWindowOperatorPtr;

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class BroadcastLogicalOperatorNode;
typedef std::shared_ptr<BroadcastLogicalOperatorNode> BroadcastLogicalOperatorNodePtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_
