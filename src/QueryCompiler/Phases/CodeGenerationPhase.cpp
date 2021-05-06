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
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/CodeGenerationPhase.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {
namespace QueryCompilation {

CodeGenerationPhasePtr CodeGenerationPhase::create() { return std::make_shared<CodeGenerationPhase>(); }

PipelineQueryPlanPtr CodeGenerationPhase::apply(PipelineQueryPlanPtr queryPlan) {
    NES_DEBUG("Generate code for query plan " << queryPlan->getQueryId() << " - " << queryPlan->getQuerySubPlanId());
    for (auto pipeline : queryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return queryPlan;
}

OperatorPipelinePtr CodeGenerationPhase::apply(OperatorPipelinePtr pipeline) {
    NES_DEBUG("Generate code for pipeline " << pipeline->getPipelineId());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    auto pipelineRoots = pipeline->getQueryPlan()->getRootOperators();
    NES_ASSERT(pipelineRoots.size() == 1, "A pipeline should have a single root operator.");
    auto rootOperator = pipelineRoots[0];
    generate(rootOperator, [&codeGenerator, &context](GeneratableOperators::GeneratableOperatorPtr operatorNode) {
        operatorNode->generateOpen(codeGenerator, context);
    });

    generate(rootOperator, [&codeGenerator, &context](GeneratableOperators::GeneratableOperatorPtr operatorNode) {
        operatorNode->generateExecute(codeGenerator, context);
    });

    generate(rootOperator, [&codeGenerator, &context](GeneratableOperators::GeneratableOperatorPtr operatorNode) {
        operatorNode->generateClose(codeGenerator, context);
    });
    auto pipelineStage = codeGenerator->compile(context);

    // we replace the current pipeline operators with an executable operator.
    // this allows us to keep the pipeline structure.
    auto operatorHandlers = context->getOperatorHandlers();
    auto executableOperator = ExecutableOperator::create(pipelineStage, operatorHandlers);
    pipeline->getQueryPlan()->replaceRootOperator(rootOperator, executableOperator);
    return pipeline;
}

void CodeGenerationPhase::generate(OperatorNodePtr rootOperator,
                                   std::function<void(GeneratableOperators::GeneratableOperatorPtr operatorNode)> applyFunction) {
    auto iterator = DepthFirstNodeIterator(rootOperator);
    for (auto node : iterator) {
        if (!node->instanceOf<GeneratableOperators::GeneratableOperator>()) {
            throw QueryCompilationException("Operator should be of type GeneratableOperator but it is a " + node->toString());
        }
        auto generatableOperator = node->as<GeneratableOperators::GeneratableOperator>();
        applyFunction(generatableOperator);
    }
}

}// namespace QueryCompilation
}// namespace NES
