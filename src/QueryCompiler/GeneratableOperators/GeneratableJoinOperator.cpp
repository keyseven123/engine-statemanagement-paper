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
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableJoinOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>

namespace NES {

void GeneratableJoinOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinOperatorHandler = Join::JoinOperatorHandler::create(joinDefinition, context->getResultSchema());

    auto newPipelineContext1 = PipelineContext::create();
    newPipelineContext1->arity = PipelineContext::BinaryLeft;
    NES_ASSERT(0 == newPipelineContext1->registerOperatorHandler(joinOperatorHandler), "invalid operator handler index");
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);

    auto newPipelineContext2 = PipelineContext::create();
    newPipelineContext2->arity = PipelineContext::BinaryRight;
    NES_ASSERT(0 == newPipelineContext2->registerOperatorHandler(joinOperatorHandler), "invalid operator handler index");
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableJoinOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinOperatorHandlerIndex = codegen->generateJoinSetup(joinDefinition, context);
    codegen->generateCodeForJoin(joinDefinition, context, joinOperatorHandlerIndex);
}

GeneratableJoinOperatorPtr GeneratableJoinOperator::create(JoinLogicalOperatorNodePtr logicalJoinOperator, OperatorId id) {
    return std::make_shared<GeneratableJoinOperator>(
        GeneratableJoinOperator(logicalJoinOperator->getLeftInputSchema(), logicalJoinOperator->getRightInputSchema(),
                                logicalJoinOperator->getOutputSchema(), logicalJoinOperator->getJoinDefinition(), id));
}

GeneratableJoinOperator::GeneratableJoinOperator(SchemaPtr leftSchema, SchemaPtr rightSchema, SchemaPtr outSchema,
                                                 Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id)
    : JoinLogicalOperatorNode(joinDefinition, id), joinDefinition(joinDefinition) {

    NES_ASSERT(leftSchema, "invalid left schema");
    NES_ASSERT(rightSchema, "invalid right schema");
    NES_ASSERT(outSchema, "invalid out schema");
    setLeftInputSchema(leftSchema);
    setRightInputSchema(rightSchema);
    setOutputSchema(outSchema);
}

const std::string GeneratableJoinOperator::toString() const {
    std::stringstream ss;
    ss << "JOIN_(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES