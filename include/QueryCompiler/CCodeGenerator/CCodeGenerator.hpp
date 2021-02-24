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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/TypeCastExprStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>

namespace NES {

/**
 * @brief A code generator that generates C++ code optimized for X86 architectures.
 */
class CCodeGenerator : public CodeGenerator {

  public:
    CCodeGenerator();
    static CodeGeneratorPtr create();

    /**
     * @brief Code generation for a scan, which depends on a particular input schema.
     * @param schema The input schema, in which we receive the input buffer.
     * @param schema The out schema, in which we forward to the next operator
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForScan(SchemaPtr inputSchema, SchemaPtr outputSchema, PipelineContextPtr context) override;

    /**
     * @brief Code generation for a projection, which depends on a particular input schema.
     * @param projectExpressions The projection expression nodes.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForProjection(std::vector<ExpressionNodePtr> projectExpressions, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a filter operator, which depends on a particular filter predicate.
    * @param predicate The filter predicate, which selects input records.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForFilter(PredicatePtr pred, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a map operator, which depends on a particular map predicate.
    * @param field The field, which we want to manipulate with the map predicate.
    * @param predicate The map predicate.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForMap(AttributeFieldPtr field, UserAPIExpressionPtr pred, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a emit, which depends on a particular output schema.
    * @param schema The output schema.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForEmit(SchemaPtr sinkSchema, PipelineContextPtr context) override;

    /**
     * @brief Code generation for a watermark assigner operator.
     * @param watermarkStrategy strategy used for watermark assignment.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForWatermarkAssigner(Windowing::WatermarkStrategyPtr watermarkStrategy, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    uint64_t generateWindowSetup(Windowing::LogicalWindowDefinitionPtr window, SchemaPtr windowOutputSchema,
                                 PipelineContextPtr context, uint64_t id) override;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForCompleteWindow(Windowing::LogicalWindowDefinitionPtr window,
                                       GeneratableWindowAggregationPtr generatableWindowAggregation, PipelineContextPtr context,
                                       uint64_t operatorHandlerIndex) override;

    /**
    * @brief Code generation for a slice creation operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForSlicingWindow(Windowing::LogicalWindowDefinitionPtr window,
                                      GeneratableWindowAggregationPtr generatableWindowAggregation, PipelineContextPtr context,
                                      uint64_t windowOperatorIndex) override;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForCombiningWindow(Windowing::LogicalWindowDefinitionPtr window,
                                        GeneratableWindowAggregationPtr generatableWindowAggregation, PipelineContextPtr context,
                                        uint64_t windowOperatorIndex) override;

    /**
    * @brief Code generation the setup method for join operators, which depends on a particular join definition.
    * @param join The join definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    uint64_t generateJoinSetup(Join::LogicalJoinDefinitionPtr join, PipelineContextPtr context, uint64_t id) override;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef, PipelineContextPtr context,
                             uint64_t operatorHandlerIndex) override;

    /**
     * @brief Performs the actual compilation the generated code pipeline.
     * @param code generated code.
     * @return ExecutablePipelinePtr returns the compiled and executable pipeline.
     */
    NodeEngine::Execution::ExecutablePipelineStagePtr compile(PipelineContextPtr context) override;

    std::string generateCode(PipelineContextPtr context) override;

    ~CCodeGenerator() override;

  private:
    CompilerPtr compiler;

  private:
    BinaryOperatorStatement getBuffer(VariableDeclaration tupleBufferVariable);
    VariableDeclaration getWindowOperatorHandler(PipelineContextPtr context, VariableDeclaration tupleBufferVariable,
                                                 uint64_t index);
    BinaryOperatorStatement getWatermark(VariableDeclaration tupleBufferVariable);
    BinaryOperatorStatement getOriginId(VariableDeclaration tupleBufferVariable);

    TypeCastExprStatement getTypedBuffer(VariableDeclaration tupleBufferVariable, StructDeclaration structDeclaration);
    BinaryOperatorStatement getBufferSize(VariableDeclaration tupleBufferVariable);
    BinaryOperatorStatement setNumberOfTuples(VariableDeclaration tupleBufferVariable, VariableDeclaration inputBufferVariable);
    BinaryOperatorStatement setWatermark(VariableDeclaration tupleBufferVariable, VariableDeclaration inputBufferVariable);
    BinaryOperatorStatement setOriginId(VariableDeclaration tupleBufferVariable, VariableDeclaration inputBufferVariable);

    BinaryOperatorStatement allocateTupleBuffer(VariableDeclaration pipelineContext);
    BinaryOperatorStatement emitTupleBuffer(VariableDeclaration pipelineContext, VariableDeclaration tupleBufferVariable,
                                            VariableDeclaration workerContextVariable);
    void generateTupleBufferSpaceCheck(PipelineContextPtr context, VariableDeclaration varDeclResultTuple,
                                       StructDeclaration structDeclarationResultTuple);

    StructDeclaration getStructDeclarationFromSchema(std::string structName, SchemaPtr schema);

    BinaryOperatorStatement getAggregationWindowHandler(VariableDeclaration pipelineContextVariable, DataTypePtr keyType,
                                                        DataTypePtr inputType, DataTypePtr partialAggregateType,
                                                        DataTypePtr finalAggregateType);

    BinaryOperatorStatement getJoinWindowHandler(VariableDeclaration pipelineContextVariable, DataTypePtr KeyType);

    BinaryOperatorStatement getStateVariable(VariableDeclaration);

    BinaryOperatorStatement getLeftJoinState(VariableDeclaration windowHandlerVariable);
    BinaryOperatorStatement getRightJoinState(VariableDeclaration windowHandlerVariable);

    BinaryOperatorStatement getWindowManager(VariableDeclaration);

    void generateCodeForWatermarkUpdaterWindow(PipelineContextPtr context, VariableDeclaration handler);
    void generateCodeForWatermarkUpdaterJoin(PipelineContextPtr context, VariableDeclaration handler, bool leftSide);

    StructDeclaration getStructDeclarationFromWindow(std::string structName);

    VariableDeclaration getJoinOperatorHandler(PipelineContextPtr context, VariableDeclaration tupleBufferVariable,
                                               uint64_t joinOperatorIndex);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
