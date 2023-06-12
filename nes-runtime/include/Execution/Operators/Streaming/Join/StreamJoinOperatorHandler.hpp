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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_H_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_H_
#include <API/Schema.hpp>
#include <Common/Identifiers.hpp>
#include <Execution/Operators/Streaming/Join/StreamWindow.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <list>
#include <map>
#include <optional>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This operator is the general join operator handler withh basic functionality
 */
class StreamJoinOperatorHandler : public OperatorHandler {
  public:
    enum class JoinType : uint8_t { HASH_JOIN, NESTED_LOOP_JOIN };

    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param windowSize
     * @param joinSchemaLeft
     * @param joinSchemaRight
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param origins
     * @param joinType
     */
    StreamJoinOperatorHandler(const SchemaPtr& joinSchemaLeft,
                              const SchemaPtr& joinSchemaRight,
                              const std::string& joinFieldNameLeft,
                              const std::string& joinFieldNameRight,
                              const std::vector<OriginId>& origins,
                              size_t windowSize,
                              const JoinType joinType);

    virtual ~StreamJoinOperatorHandler() = default;

    /**
     * @brief Starts the operator handler
     * @param pipelineExecutionContext
     * @param stateManager
     * @param localStateVariableId
     */
    virtual void
    start(PipelineExecutionContextPtr pipelineExecutionContext, StateManagerPtr stateManager, uint32_t localStateVariableId) = 0;

    /**
     * @brief Stops the operator handler
     * @param terminationType
     * @param pipelineExecutionContext
     */
    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) = 0;

    void setup(uint64_t newNumberOfWorkerThreads);

    /**
     * @brief Deletes a window
     * @param windowIdentifier
     */
    void deleteWindow(uint64_t windowIdentifier);

    /**
     * @brief Retrieves the number of tuples for a stream (left or right) and a window
     * @param windowIdentifier
     * @param isLeftSide
     * @return Number of tuples or -1 if no window exists for the window identifier
     */
    uint64_t getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide);

    /**
     * @brief Retrieves the pointer to the first tuple for a stream (left or right) and a window
     * @param windowIdentifier
     * @param isLeftSide
     * @return Pointer
     */
    uint8_t* getFirstTuple(uint64_t windowIdentifier, bool isLeftSide);

    /**
     * @brief Retrieves the schema for the left or right stream
     * @param isLeftSide
     * @return SchemaPtr
     */
    SchemaPtr getSchema(bool isLeftSide);

    /**
     * @brief Retrieves the start and end of a window
     * @param windowIdentifier
     * @return Pair<windowStart, windowEnd>
     */
    std::pair<uint64_t, uint64_t> getWindowStartEnd(uint64_t windowIdentifier);

    /**
     * @brief Checks if any window can be triggered and all triggerable window identifiers are being returned in a vector.
     * This method updates the watermarkProcessor and should be thread-safe
     * @param watermarkTs
     * @param sequenceNumber
     * @param originId
     * @param isLeft
     * @return Vector<uint64_t> containing windows that can be triggered
     */
    std::vector<uint64_t> checkWindowsTrigger(uint64_t watermarkTs, uint64_t sequenceNumber, OriginId originId);

    /**
     * @brief method to trigger the finished windows
     * @param windowIdentifiersToBeTriggered
     * @param workerCtx
     * @param pipelineCtx
     */
    virtual void triggerWindows(std::vector<uint64_t> windowIdentifiersToBeTriggered,
                                WorkerContext* workerCtx,
                                PipelineExecutionContext* pipelineCtx) = 0;

    /**
     * @brief Getter for the left join schema
     * @return left join schema
     */
    SchemaPtr getJoinSchemaLeft() const;

    /**
     * @brief Getter for the right join schema
     * @return right join schema
     */
    SchemaPtr getJoinSchemaRight() const;

    /**
     * @brief Getter for the field name of the left join schmea
     * @return string of the join field name left
     */
    const std::string& getJoinFieldNameLeft() const;

    /**
     * @brief Getter for the field name of the right join schmea
     * @return string of the join field name right
     */
    const std::string& getJoinFieldNameRight() const;

    /**
     * @brief Retrieves the window by a window identifier. If no window exists for the windowIdentifier, the optional has no value.
     * @param windowIdentifier
     * @return optional
     */
    std::optional<StreamWindowPtr> getWindowByWindowIdentifier(uint64_t windowIdentifier);

    /**
     * @brief Retrieves the window by a window timestamp. If no window exists for the timestamp, the optional has no value.
     * @param timestamp
     * @return
     */
    StreamWindowPtr getWindowByTimestampOrCreateIt(uint64_t timestamp);

    /**
     * Return the current watermark
     * @return
     */
    uint64_t getLastWatermark();

    /**
     * @brief Creates a new window that corresponds to this timestamp
     * @param timestamp
     */
    StreamWindowPtr createNewWindow(uint64_t timestamp);

    /**
     * @brief get the number of windows
     * @return
     */
    uint64_t getNumberOfWindows();

    /**
     * @brief update the watermark for a particular worker
     * @param watermark
     * @param workerId
     */
    void updateWatermarkForWorker(uint64_t watermark, uint64_t workerId);

    /**
     * @brief Get the minimal watermark among all worker
     * @return
     */
    uint64_t getMinWatermarkForWorker();

    /**
     * @brief Add the id of the operator this handler is responsilbe for
     * @param operatorId
     */
    void addOperatorId(OperatorId operatorId);

  protected:
    size_t numberOfWorkerThreads = 1;
    std::list<StreamWindowPtr> windows;
    SliceAssigner sliceAssigner;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    SchemaPtr joinSchemaLeft;
    SchemaPtr joinSchemaRight;
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
    JoinType joinType;
    std::atomic<bool> alreadySetup{false};
    std::map<uint64_t, uint64_t> workerIdToWatermarkMap;
    OperatorId operatorId;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_H_
