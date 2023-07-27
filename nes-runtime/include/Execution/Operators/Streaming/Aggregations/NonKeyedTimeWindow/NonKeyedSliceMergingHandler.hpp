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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {
struct SliceMergeTask;
class State;
class NonKeyedSlice;
using GlobalSlicePtr = std::unique_ptr<NonKeyedSlice>;
class NonKeyedSliceStaging;

/**
 * @brief The GlobalSliceMergingHandler merges thread local pre-aggregated slices for global
 * tumbling and sliding window aggregations.
 */
class NonKeyedSliceMergingHandler : public Runtime::Execution::OperatorHandler,
                                    public ::NES::detail::virtual_enable_shared_from_this<NonKeyedSliceMergingHandler, false> {
  public:
    /**
     * @brief Constructor for the GlobalSliceMergingHandler
     * @param windowDefinition
     */
    NonKeyedSliceMergingHandler(std::shared_ptr<NonKeyedSliceStaging> globalSliceStaging);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Get a reference to the slice staging.
     * @note This should be only called from the generated code.
     * @return KeyedSliceStaging
     */
    inline NonKeyedSliceStaging& getSliceStaging() { return *sliceStaging.get(); }

    /**
     * @brief Gets a weak pointer to the slice staging
     * @return std::weak_ptr<KeyedSliceStaging>
     */
    std::weak_ptr<NonKeyedSliceStaging> getSliceStagingPtr();

    /**
     * @brief Creates a new global slice for a specific slice merge task
     * @param sliceMergeTask SliceMergeTask
     * @return GlobalSlicePtr
     */
    GlobalSlicePtr createGlobalSlice(SliceMergeTask* sliceMergeTask);
    const State* getDefaultState() const;

    ~NonKeyedSliceMergingHandler();

  private:
    uint64_t entrySize;
    std::shared_ptr<NonKeyedSliceStaging> sliceStaging;
    std::unique_ptr<State> defaultState;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGHANDLER_HPP_
