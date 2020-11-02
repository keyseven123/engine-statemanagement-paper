#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMAPOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMAPOPERATOR_HPP_

#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableMapOperator : public MapLogicalOperatorNode, public GeneratableOperator {
  public:
    /**
     * @brief Create sharable instance of GeneratableMapOperator
     * @param mapLogicalOperator: the map logical operator
     * @param id: the operator id if not provided then next available operator id is used.
     * @return instance of GeneratableMapOperator
     */
    static GeneratableMapOperatorPtr create(MapLogicalOperatorNodePtr mapLogicalOperator, OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
    * @brief Produce function, which calls the child produce function and brakes pipelines if necessary.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief Consume function, which generates code for the processing and calls the parent consume function.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief To string method for the operator.
    * @return string
    */
    const std::string toString() const override;

  private:
    GeneratableMapOperator(FieldAssignmentExpressionNodePtr mapExpression, OperatorId id);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMAPOPERATOR_HPP_
