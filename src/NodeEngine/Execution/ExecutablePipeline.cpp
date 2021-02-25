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

#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/Execution/OperatorHandler.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::NodeEngine::Execution {

ExecutablePipeline::ExecutablePipeline(uint32_t pipelineStageId, QuerySubPlanId qepId,
                                       ExecutablePipelineStagePtr executablePipelineStage,
                                       PipelineExecutionContextPtr pipelineExecutionContext, ExecutablePipelinePtr nextPipeline,
                                       SchemaPtr inputSchema, SchemaPtr outputSchema, bool reconfiguration)
    : pipelineStageId(pipelineStageId), qepId(qepId), executablePipelineStage(std::move(executablePipelineStage)),
      nextPipeline(std::move(nextPipeline)), pipelineContext(std::move(pipelineExecutionContext)), inputSchema(inputSchema),
      outputSchema(outputSchema), reconfiguration(reconfiguration) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext, "Wrong pipeline stage argument");
}

bool ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineStageId);
    uint32_t ret = !executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
    return ret;
}

bool ExecutablePipeline::setup(QueryManagerPtr, BufferManagerPtr) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start() {
    for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->start(pipelineContext);
    }
    executablePipelineStage->start(*pipelineContext.get());
    return true;
}

bool ExecutablePipeline::stop() {
    for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->stop(pipelineContext);
    }
    return executablePipelineStage->stop(*pipelineContext.get()) == 0;
}

ExecutablePipelinePtr ExecutablePipeline::getNextPipeline() { return nextPipeline; }

uint32_t ExecutablePipeline::getPipeStageId() { return pipelineStageId; }

QuerySubPlanId ExecutablePipeline::getQepParentId() const { return qepId; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

PipelineStageArity ExecutablePipeline::getArity() { return executablePipelineStage->getArity(); }

ExecutablePipelinePtr ExecutablePipeline::create(uint32_t pipelineStageId, const QuerySubPlanId querySubPlanId,
                                                 ExecutablePipelineStagePtr executablePipelineStage,
                                                 PipelineExecutionContextPtr pipelineContext, ExecutablePipelinePtr nextPipeline,
                                                 SchemaPtr inputSchema, SchemaPtr outputSchema, bool reconfiguration) {
    NES_ASSERT2_FMT(executablePipelineStage != nullptr,
                    "Executable pipelinestage is null for " << pipelineStageId
                                                            << "within the following query sub plan: " << querySubPlanId);
    NES_ASSERT2_FMT(pipelineContext != nullptr,
                    "Pipeline context is null for " << pipelineStageId
                                                    << "within the following query sub plan: " << querySubPlanId);
    if (!reconfiguration) {
        NES_ASSERT2_FMT(inputSchema != nullptr,
                        "Input schema is null for " << pipelineStageId
                                                    << "within the following query sub plan: " << querySubPlanId);
        NES_ASSERT2_FMT(outputSchema != nullptr,
                        "Output schema is null for " << pipelineStageId
                                                     << "within the following query sub plan: " << querySubPlanId);
    }
    return std::make_shared<ExecutablePipeline>(pipelineStageId, querySubPlanId, executablePipelineStage, pipelineContext,
                                                nextPipeline, inputSchema, outputSchema, reconfiguration);
}

std::string ExecutablePipeline::getCodeAsString() {
    std::stringstream ss;
    ss << executablePipelineStage->getCodeAsString();
    return ss.str();
}

ExecutablePipeline::~ExecutablePipeline() { NES_DEBUG("~ExecutablePipeline(" + std::to_string(pipelineStageId) + ")"); }

const SchemaPtr& ExecutablePipeline::getInputSchema() const { return inputSchema; }

const SchemaPtr& ExecutablePipeline::getOutputSchema() const { return outputSchema; }

}// namespace NES::NodeEngine::Execution