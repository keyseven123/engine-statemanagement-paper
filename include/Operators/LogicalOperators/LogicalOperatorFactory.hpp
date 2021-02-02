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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorId.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
class ExpressionItem;

class LogicalOperatorFactory {
  public:
    template<typename Operator, typename... Arguments>
    static std::shared_ptr<Operator> create(Arguments&&... args) {
        return std::shared_ptr<Operator>(std::forward<Arguments>(args)...);
    }

    /**
     * @brief Create a new logical filter operator.
     * @param predicate: the filter predicate is represented as an expression node, which has to return true.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr createFilterOperator(const ExpressionNodePtr predicate,
                                                     OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new stream rename operator.
     * @param new stream name
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr createRenameStreamOperator(const std::string newStreamName,
                                                           OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
    * @brief Create a new logical projection operator.
    * @param expression list
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return LogicalOperatorNodePtr
    */
    static UnaryOperatorNodePtr createProjectionOperator(std::vector<ExpressionItem> expressions,
                                                         OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new sink operator with a specific sink descriptor.
     * @param sinkDescriptor the SinkDescriptor.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static UnaryOperatorNodePtr createSinkOperator(const SinkDescriptorPtr sinkDescriptor,
                                                   OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new map operator with a field assignment expression as a map expression.
     * @param mapExpression the FieldAssignmentExpressionNode.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr createMapOperator(const FieldAssignmentExpressionNodePtr mapExpression,
                                                  OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new source operator with source descriptor.
     * @param sourceDescriptor the SourceDescriptorPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr createSourceOperator(const SourceDescriptorPtr sourceDescriptor,
                                                     OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
    * @brief Create a specialized watermark assigner operator.
    * @param watermarkStrategy strategy to be used to assign the watermark
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return LogicalOperatorNodePtr
    */
    static UnaryOperatorNodePtr
    createWatermarkAssignerOperator(const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor,
                                    OperatorId id = UtilityFunctions::getNextOperatorId());
    /**
     * @brief Create a new window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr createWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                     OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized central window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr
    createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                           OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized slice creation window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr
    createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                           OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized slice merging window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr
    createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                          OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized window computation window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static UnaryOperatorNodePtr
    createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                               OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized merge operator.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static BinaryOperatorNodePtr createMergeOperator(OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
    * @brief Create a specialized join operator.
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return BinaryOperatorNodePtr
    */
    static BinaryOperatorNodePtr createJoinOperator(Join::LogicalJoinDefinitionPtr joinDefinition,
                                                    OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a broadcast operator.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return ExchangeOperatorNodePtr
     */
    static ExchangeOperatorNodePtr createBroadcastOperator(OperatorId id = UtilityFunctions::getNextOperatorId());
};

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
