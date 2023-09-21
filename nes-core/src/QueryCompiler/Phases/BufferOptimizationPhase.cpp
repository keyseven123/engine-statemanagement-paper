/*
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
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Phases/BufferOptimizationPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation {

BufferOptimizationPhasePtr
BufferOptimizationPhase::BufferOptimizationPhase::create(QueryCompilerOptions::OutputBufferOptimizationLevel level) {
    return std::make_shared<BufferOptimizationPhase>(level);
}

BufferOptimizationPhase::BufferOptimizationPhase(QueryCompilerOptions::OutputBufferOptimizationLevel level) : level(level) {}

PipelineQueryPlanPtr BufferOptimizationPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

bool BufferOptimizationPhase::isReadOnlyInput(OperatorPipelinePtr pipeline) {
    // We define the input of a pipeline as read only if it is shared with another pipeline.
    // To this end, we check if one of our parents has more than one child.
    for (const auto& parent : pipeline->getPredecessors()) {
        if (parent->getSuccessors().size() > 1) {
            // the parent has more than one successor. So our input is read only.
            return true;
        }
    }
    return false;
}

OperatorPipelinePtr BufferOptimizationPhase::apply(OperatorPipelinePtr) {
    NES_NOT_IMPLEMENTED();
    /*
    if (level == QueryCompilerOptions::OutputBufferOptimizationLevel::NO) {
        NES_DEBUG("BufferOptimizationPhase: No optimization requested or applied.");
        return operatorPipeline;
    }

    // If we can't modify the input we can't optimize the buffer access.
    if (isReadOnlyInput(operatorPipeline)) {
        NES_DEBUG("BufferOptimizationPhase: No optimization is possible as input is read only.");
        return operatorPipeline;
    }

    NES_DEBUG("BufferOptimizationPhase: Scanning pipeline for optimization potential.");
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();

    SchemaPtr inputSchema = nullptr;
    SchemaPtr outputSchema = nullptr;
    std::shared_ptr<GeneratableOperators::GeneratableBufferEmit> emitNode = nullptr;
    bool filterOperatorFound = false;
    bool filterOnly = true;
    // TODO add checks when further operators are introduced that change the number of result tuples

    for (const auto& node : nodes) {
        if (node->instanceOf<GeneratableOperators::GeneratableBufferScan>()) {
            auto scanNode = node->as<GeneratableOperators::GeneratableBufferScan>();
            inputSchema = scanNode->getInputSchema();
        } else if (node->instanceOf<GeneratableOperators::GeneratableBufferEmit>()) {
            emitNode = node->as<GeneratableOperators::GeneratableBufferEmit>();
            outputSchema = emitNode->getOutputSchema();
        } else if (node->instanceOf<GeneratableOperators::GeneratableFilterOperator>()
                   || node->instanceOf<GeneratableOperators::GeneratableFilterOperatorPredicated>()) {
            filterOperatorFound = true;
        } else if (node->instanceOf<GeneratableOperators::GeneratableCEPIterationOperator>()) {
            return operatorPipeline;
        } else {
            filterOnly = false;
        }
    }

    if (inputSchema == nullptr) {
        NES_DEBUG("BufferOptimizationPhase: No Scan operator found in pipeline. No optimization can be applied.");
        return operatorPipeline;
    }
    if (emitNode == nullptr || outputSchema == nullptr) {
        NES_DEBUG("BufferOptimizationPhase: No Emit operator found in pipeline. No optimization can be applied.");
        return operatorPipeline;
    }

    if (inputSchema->getLayoutType() != Schema::MemoryLayoutType::ROW_LAYOUT
        || outputSchema->getLayoutType() != Schema::MemoryLayoutType::ROW_LAYOUT) {
        NES_DEBUG("BufferOptimizationPhase: Currently buffer optimization is only possible if the input and output schema are "
                  "using a ROW layout.");
        return operatorPipeline;
    }

    // If we have only a filter query, we use a record copy instead of a field copy
    if (inputSchema->equals(outputSchema) && filterOperatorFound && filterOnly) {
        emitNode->setOutputBufferAssignmentStrategy(OutputBufferAssignmentStrategy::RECORD_COPY);
        NES_DEBUG("BufferOptimizationPhase: Use Record Copy");
    } else {
        emitNode->setOutputBufferAssignmentStrategy(OutputBufferAssignmentStrategy::FIELD_COPY);
        NES_DEBUG("BufferOptimizationPhase: Use Field Copy");
    }

    // Check if necessary conditions are fulfilled and set the desired strategy in the emit operator:
    if (inputSchema->equals(outputSchema) && !filterOperatorFound
        && (level == QueryCompilerOptions::OutputBufferOptimizationLevel::ONLY_INPLACE_OPERATIONS_NO_FALLBACK
            || level == QueryCompilerOptions::OutputBufferOptimizationLevel::ALL)) {
        // The highest level of optimization - just modifying the input buffer in place and passing it to the next pipeline
        // - can be applied as there are no filter statements etc.
        emitNode->setOutputBufferAllocationStrategy(OutputBufferAllocationStrategy::ONLY_INPLACE_OPERATIONS);
        NES_DEBUG("BufferOptimizationPhase: Assign ONLY_INPLACE_OPERATIONS optimization strategy to pipeline.");
        return operatorPipeline;
    }

    if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes()
        && (level == QueryCompilerOptions::OutputBufferOptimizationLevel::REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK
            || level == QueryCompilerOptions::OutputBufferOptimizationLevel::ALL)) {
        // The optimizations "reuse input buffer as output buffer" and "omit size check" can be applied.
        emitNode->setOutputBufferAllocationStrategy(OutputBufferAllocationStrategy::REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK);
        NES_DEBUG(
            "BufferOptimizationPhase: Assign REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK optimization strategy to pipeline.");
        return operatorPipeline;
    }
    if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes()
        && (level == QueryCompilerOptions::OutputBufferOptimizationLevel::REUSE_INPUT_BUFFER_NO_FALLBACK
            || level == QueryCompilerOptions::OutputBufferOptimizationLevel::ALL)) {
        // The optimization  "reuse input buffer as output buffer" can be applied.
        emitNode->setOutputBufferAllocationStrategy(OutputBufferAllocationStrategy::REUSE_INPUT_BUFFER);
        NES_DEBUG("BufferOptimizationPhase: Assign REUSE_INPUT_BUFFER optimization strategy to pipeline.");
        return operatorPipeline;
    }
    if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes()
        && (level == QueryCompilerOptions::OutputBufferOptimizationLevel::OMIT_OVERFLOW_CHECK_NO_FALLBACK
            || level == QueryCompilerOptions::OutputBufferOptimizationLevel::ALL)) {
        // The optimization "omit size check" can be applied.
        emitNode->setOutputBufferAllocationStrategy(OutputBufferAllocationStrategy::OMIT_OVERFLOW_CHECK);
        NES_DEBUG("BufferOptimizationPhase: Assign OMIT_OVERFLOW_CHECK optimization strategy to pipeline.");
        return operatorPipeline;
    }

    // level != NO, but still no optimization can be applied
    NES_DEBUG("BufferOptimizationPhase: Optimization was requested, but no optimization was applied.");

    return operatorPipeline;*/
}

}// namespace NES::QueryCompilation