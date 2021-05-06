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

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/ExecutionResult.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::NodeEngine {

Task::Task(Execution::ExecutablePipelinePtr pipeline, TupleBuffer& buffer) : pipeline(std::move(pipeline)), buf(buffer) {
    id = UtilityFunctions::getNextTaskId();
}

Task::Task() : pipeline(nullptr), buf(), id(-1) {}

ExecutionResult Task::operator()(WorkerContextRef workerContext) { return pipeline->execute(buf, workerContext); }

uint64_t Task::getNumberOfTuples() { return buf.getNumberOfTuples(); }

bool Task::isWatermarkOnly() { return buf.getNumberOfTuples() == 0; }

Execution::ExecutablePipelinePtr Task::getPipeline() { return pipeline; }

TupleBuffer& Task::getBufferRef() { return buf; }

bool Task::operator!() const { return pipeline == nullptr; }

Task::operator bool() const { return pipeline != nullptr; }
uint64_t Task::getId() { return id; }

std::string Task::toString() {
    std::stringstream ss;
    ss << "Task: id=" << id;
    ss << " execute pipelineId=" << pipeline->getPipeStageId() << " qepParentId=" << pipeline->getQepParentId()
       << " nextPipelineId=" << pipeline->getNextPipeline();
    ss << " inputBuffer=" << buf.getBuffer() << " inputTuples=" << buf.getNumberOfTuples()
       << " bufferSize=" << buf.getBufferSize() << " watermark=" << buf.getWatermark() << " originID=" << buf.getOriginId();
    return ss.str();
}

}// namespace NES::NodeEngine
