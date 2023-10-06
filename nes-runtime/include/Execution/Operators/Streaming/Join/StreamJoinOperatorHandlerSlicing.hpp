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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERSLICING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERSLICING_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/Join/OperatorHandlerInterfaces/JoinOperatorHandlerInterfaceSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

class StreamJoinOperatorHandlerSlicing : public virtual JoinOperatorHandlerInterfaceSlicing, public virtual StreamJoinOperatorHandler {
  public:

    ~StreamJoinOperatorHandlerSlicing()  override = default;
    StreamSlicePtr getSliceByTimestampOrCreateIt(uint64_t timestamp) override;
    StreamSlice* getCurrentWindowOrCreate() override;
    std::vector<std::pair<StreamSlicePtr, StreamSlicePtr>> getSlicesLeftRightForWindow(uint64_t windowId) override;
    void triggerSlices(TriggerableWindows& triggerableWindows, PipelineExecutionContext* pipelineCtx) override;
    std::vector<WindowInfo> getAllWindowsForSlice(StreamSlice& slice) override;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERSLICING_HPP_
