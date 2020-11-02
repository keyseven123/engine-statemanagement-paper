#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorId.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

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
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createFilterOperator(const ExpressionNodePtr predicate, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new sink operator with a specific sink descriptor.
     * @param sinkDescriptor the SinkDescriptor.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSinkOperator(const SinkDescriptorPtr sinkDescriptor, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new map operator with a field assignment expression as a map expression.
     * @param mapExpression the FieldAssignmentExpressionNode.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createMapOperator(const FieldAssignmentExpressionNodePtr mapExpression, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new source operator with source descriptor.
     * @param sourceDescriptor the SourceDescriptorPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSourceOperator(const SourceDescriptorPtr sourceDescriptor, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a new window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized central window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized slice creation window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized slice merging window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized window computation window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a specialized merge operator.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createMergeOperator(OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
     * @brief Create a broadcast operator.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createBroadcastOperator(OperatorId id = UtilityFunctions::getNextOperatorId());
};

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
