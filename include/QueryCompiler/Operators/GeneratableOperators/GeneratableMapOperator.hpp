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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEMAP_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEMAP_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Generates a map operator, which manipulates a tuple attribute acroding to an expression.
 */
class GeneratableMapOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable map operator, which applies a map expression on a specific input record.
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param mapExpression the map expression
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         FieldAssignmentExpressionNodePtr mapExpression);

    /**
     * @brief Creates a new generatable map operator, which applies a map expression on a specific input record.
     * @param id operator id
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param mapExpression the map expression
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         FieldAssignmentExpressionNodePtr mapExpression);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableMapOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                           FieldAssignmentExpressionNodePtr mapExpression);
    const FieldAssignmentExpressionNodePtr mapExpression;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEMAP_HPP_
