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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

StreamHashJoinProbe::StreamHashJoinProbe(uint64_t handlerIndex,
                                         SchemaPtr joinSchemaLeft,
                                         SchemaPtr joinSchemaRight,
                                         SchemaPtr joinSchemaOutput,
                                         std::string joinFieldNameLeft,
                                         std::string joinFieldNameRight)
    : handlerIndex(handlerIndex), joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight),
      joinSchemaOutput(joinSchemaOutput), joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight) {}

/**
 * @brief Checks if two fields are similar
 * @param fieldPtr1
 * @param fieldPtr2
 * @param sizeOfField
 * @return true if both fields are equal, false otherwise
 */
bool compareField(uint8_t* fieldPtr1, uint8_t* fieldPtr2, size_t sizeOfField) {
    return memcmp(fieldPtr1, fieldPtr2, sizeOfField) == 0;
}

/**
 * @brief Returns a pointer to the field of the record (recordBase)
 * @param recordBase
 * @param joinSchema
 * @param fieldName
 * @return pointer to the field
 */
uint8_t* getField(uint8_t* recordBase, Schema* joinSchema, const std::string& fieldName) {
    uint8_t* pointer = recordBase;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : joinSchema->fields) {
        if (field->getName() == fieldName) {
            break;
        }
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        pointer += fieldType->size();
    }
    return pointer;
}

uint64_t getWindowStartProxyForHashJoin(void* ptrOpHandler, uint64_t windowIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    return opHandler->getWindowStartEnd(windowIdentifier).first;
}

uint64_t getWindowEndProxyForHashJoin(void* ptrOpHandler, uint64_t windowIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    return opHandler->getWindowStartEnd(windowIdentifier).second;
}

uint64_t getPartitionId(void* windowIdentifierPtr) {
    NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "windowIdentifierPtr should not be null");
    auto identifierStruct = static_cast<JoinPartitionIdTWindowIdentifier*>(windowIdentifierPtr);
    return identifierStruct->partitionId;
}

uint64_t getWindowIdentifier(void* windowIdentifierPtr) {
    NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "windowIdentifierPtr should not be null");
    auto identifierStruct = static_cast<JoinPartitionIdTWindowIdentifier*>(windowIdentifierPtr);
    return identifierStruct->windowIdentifier;
}

void* getHashWindow(void* ptrOpHandler, uint64_t windowIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    auto streamWindow = opHandler->getWindowByWindowIdentifier(windowIdentifier).value();
    auto hashWindow = static_cast<StreamHashJoinWindow*>(streamWindow.get());
    return hashWindow;
}

void* getTupleFromBucketAtPosProxyForHashJoin(void* hashWindowPtr,
                                              bool isLeftSide,
                                              uint64_t bucketPos,
                                              uint64_t pageNo,
                                              uint64_t recordPos) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    auto hashWindow = static_cast<StreamHashJoinWindow*>(hashWindowPtr);

    return hashWindow->getMergingHashTable(isLeftSide).getTupleFromBucketAtPos(bucketPos, pageNo, recordPos);
}

uint64_t getSequenceNumberProxyForHashJoin(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    return opHandler->getNextSequenceNumber();
}
uint64_t getOriginIdProxyForHashJoin(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    return opHandler->getOutputOriginId();
}

uint64_t getNumberOfPagesProxyForHashJoin(void* hashWindowPtr, bool isLeftSide, uint64_t bucketPos) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    auto hashWindow = static_cast<StreamHashJoinWindow*>(hashWindowPtr);

    return hashWindow->getMergingHashTable(isLeftSide).getNumPages(bucketPos);
}

uint64_t getNumberOfTuplesForPage(void* hashWindowPtr, bool isLeftSide, uint64_t bucketPos, uint64_t pageNo) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    auto hashWindow = static_cast<StreamHashJoinWindow*>(hashWindowPtr);
    return hashWindow->getMergingHashTable(isLeftSide).getNumberOfTuplesForPage(bucketPos, pageNo);
}

void markPartitionFinishProxyForHashJoin(void* hashWindowPtr, void* ptrOpHandler, uint64_t windowIdentifier) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "ptrOpHandler should not be null");

    auto hashWindow = static_cast<StreamHashJoinWindow*>(hashWindowPtr);
    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);

    if (hashWindow->markPartitionAsFinished()) {
        opHandler->deleteWindow(windowIdentifier);
    }
}

void StreamHashJoinProbe::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    child->open(ctx, recordBuffer);

    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto joinPartitionIdTWindowIdentifier = recordBuffer.getBuffer();
    auto partitionId = Nautilus::FunctionCall("getPartitionId", getPartitionId, joinPartitionIdTWindowIdentifier);
    auto windowIdentifier = Nautilus::FunctionCall("getWindowIdentifier", getWindowIdentifier, joinPartitionIdTWindowIdentifier);

    Value<Any> windowStart = Nautilus::FunctionCall("getWindowStartProxyForHashJoin",
                                                    getWindowStartProxyForHashJoin,
                                                    operatorHandlerMemRef,
                                                    windowIdentifier);
    Value<Any> windowEnd = Nautilus::FunctionCall("getWindowEndProxyForHashJoin",
                                                  getWindowEndProxyForHashJoin,
                                                  operatorHandlerMemRef,
                                                  windowIdentifier);

    ctx.setWatermarkTs(windowEnd.as<UInt64>());
    auto sequenceNumber =
        Nautilus::FunctionCall("getSequenceNumberProxyForHashJoin", getSequenceNumberProxyForHashJoin, operatorHandlerMemRef);
    ctx.setSequenceNumber(sequenceNumber);
    auto originId = Nautilus::FunctionCall("getOriginIdProxyForHashJoin", getOriginIdProxyForHashJoin, operatorHandlerMemRef);
    ctx.setOrigin(originId);

    auto leftMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, joinSchemaLeft);
    auto rightMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, joinSchemaRight);

    auto hashWindowRef = Nautilus::FunctionCall("getHashWindow", getHashWindow, operatorHandlerMemRef, windowIdentifier);

    auto numberOfPagesLeft = Nautilus::FunctionCall("getNumberOfPagesProxyForHashJoin",
                                                    getNumberOfPagesProxyForHashJoin,
                                                    hashWindowRef,
                                                    Value<Boolean>(/*isLeftSide*/ true),
                                                    partitionId);

    auto numberOfPagesRight = Nautilus::FunctionCall("getNumberOfPagesProxyForHashJoin",
                                                     getNumberOfPagesProxyForHashJoin,
                                                     hashWindowRef,
                                                     Value<Boolean>(/*isLeftSide*/ false),
                                                     partitionId);

    //first check if one of the page is 0 then we can return
    if (numberOfPagesLeft == 0 || numberOfPagesRight == 0) {
        Nautilus::FunctionCall("markPartitionFinishProxyForHashJoin",
                               markPartitionFinishProxyForHashJoin,
                               hashWindowRef,
                               operatorHandlerMemRef,
                               windowIdentifier);
        NES_TRACE("Mark partition for done numberOfPagesLeft={} numberOfPagesRight={}",
                  numberOfPagesLeft->toString(),
                  numberOfPagesRight->toString());
        return;
    }

    //for every left page
    for (Value<UInt64> leftPageNo((uint64_t) 0); leftPageNo < numberOfPagesLeft; leftPageNo = leftPageNo + 1) {
        auto numberOfTuplesLeft = Nautilus::FunctionCall("getNumberOfTuplesForPage",
                                                         getNumberOfTuplesForPage,
                                                         hashWindowRef,
                                                         Value<Boolean>(/*isLeftSide*/ true),
                                                         partitionId,
                                                         leftPageNo);
        //for every key in left page
        for (Value<UInt64> leftKeyPos((uint64_t) 0); leftKeyPos < numberOfTuplesLeft; leftKeyPos = leftKeyPos + 1) {
            auto leftRecordRef = Nautilus::FunctionCall("getTupleFromBucketAtPosProxyForHashJoin",
                                                        getTupleFromBucketAtPosProxyForHashJoin,
                                                        hashWindowRef,
                                                        Value<Boolean>(/*isLeftSide*/ true),
                                                        partitionId,
                                                        leftPageNo,
                                                        leftKeyPos);

            Value<UInt64> zeroValue = (uint64_t) 0;
            auto leftRecord = leftMemProvider->read({}, leftRecordRef, zeroValue);

            //TODO we should write an iterator in nautilus over the pages to make it more elegant
            //for every right page
            for (Value<UInt64> rightPageNo((uint64_t) 0); rightPageNo < numberOfPagesRight; rightPageNo = rightPageNo + 1) {
                auto numberOfTuplesRight = Nautilus::FunctionCall("getNumberOfTuplesForPage",
                                                                  getNumberOfTuplesForPage,
                                                                  hashWindowRef,
                                                                  Value<Boolean>(/*isLeftSide*/ false),
                                                                  partitionId,
                                                                  rightPageNo);
                if (numberOfTuplesRight == 0) {
                    continue;
                }

                //TODO: introduce Bloomfilter here #3909
                //                if (!rhsPage->bloomFilterCheck(lhsKeyPtr, sizeOfLeftKey)) {
                //                    continue;
                //                }

                //for every key in right page
                for (Value<UInt64> rightKeyPos((uint64_t) 0); rightKeyPos < numberOfTuplesRight; rightKeyPos = rightKeyPos + 1) {
                    auto rightRecordRef = Nautilus::FunctionCall("getTupleFromBucketAtPosProxyForHashJoin",
                                                                 getTupleFromBucketAtPosProxyForHashJoin,
                                                                 hashWindowRef,
                                                                 Value<Boolean>(/*isLeftSide*/ false),
                                                                 partitionId,
                                                                 rightPageNo,
                                                                 rightKeyPos);

                    auto rightRecord = rightMemProvider->read({}, rightRecordRef, zeroValue);
                    if (leftRecord.read(joinFieldNameLeft) == rightRecord.read(joinFieldNameRight)) {
                        Record joinedRecord;

                        // TODO replace this with a more useful version
                        joinedRecord.write(joinSchemaOutput->get(0)->getName(), windowStart);
                        joinedRecord.write(joinSchemaOutput->get(1)->getName(), windowEnd);
                        joinedRecord.write(joinSchemaOutput->get(2)->getName(), leftRecord.read(joinFieldNameLeft));

                        /* Writing the leftSchema fields, expect the join schema to have the fields in the same order then
                     * the left schema */
                        for (auto& field : joinSchemaLeft->fields) {
                            joinedRecord.write(field->getName(), leftRecord.read(field->getName()));
                        }

                        /* Writing the rightSchema fields, expect the join schema to have the fields in the same order then
                     * the right schema */
                        for (auto& field : joinSchemaRight->fields) {
                            joinedRecord.write(field->getName(), rightRecord.read(field->getName()));
                        }

                        NES_DEBUG(" write record={}", joinedRecord.toString());
                        NES_DEBUG(" write left record={}", leftRecord.toString());
                        NES_DEBUG(" write right record={}", rightRecord.toString());
                        // Calling the child operator for this joinedRecord
                        child->execute(ctx, joinedRecord);

                    }//end of key compare
                }    //end of for every right key
            }        //end of for every right page
        }            //end of for every left key
    }                //end of for every left page

    if (withDeletion) {
        Nautilus::FunctionCall("markPartitionFinishProxyForHashJoin",
                               markPartitionFinishProxyForHashJoin,
                               hashWindowRef,
                               operatorHandlerMemRef,
                               windowIdentifier);
    }
}
}//namespace NES::Runtime::Execution::Operators