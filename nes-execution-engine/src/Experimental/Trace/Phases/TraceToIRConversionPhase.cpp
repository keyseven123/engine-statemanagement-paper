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

#include "Common/DataTypes/BasicTypes.hpp"
#include <Experimental/NESIR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/AndOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/OrOperation.hpp>
#include <Experimental/NESIR/Operations/Loop/LoopInfo.hpp>
#include <Experimental/NESIR/Operations/Loop/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>

namespace NES::ExecutionEngine::Experimental::Trace {

std::shared_ptr<IR::NESIR> TraceToIRConversionPhase::apply(std::shared_ptr<ExecutionTrace> trace) {
    auto phaseContext = IRConversionContext(std::move(trace));
    return phaseContext.process();
};

std::shared_ptr<IR::NESIR> TraceToIRConversionPhase::IRConversionContext::process() {
    auto& rootBlock = trace->getBlocks().front();
    auto rootIrBlock = processBlock(0, rootBlock);

    auto& returnOperation = trace->getBlock(trace->returnRef->blockId).operations.back();
    auto returnType = std::get<ValueRef>(returnOperation.result).type;
    auto functionOperation =
        std::make_shared<IR::Operations::FunctionOperation>("execute",
                                                            /*argumentTypes*/ std::vector<IR::Operations::PrimitiveStamp>{},
                                                            /*arguments*/ std::vector<std::string>{},
                                                            returnType);
    functionOperation->addFunctionBasicBlock(rootIrBlock);
    ir->addRootOperation(functionOperation);
    ir->setIsSCF(isSCF);
    return ir;
}

IR::BasicBlockPtr TraceToIRConversionPhase::IRConversionContext::processBlock(int32_t scope, Block& block) {
    // create new frame and block
    ValueFrame blockFrame;
    std::vector<std::shared_ptr<IR::Operations::BasicBlockArgument>> blockArguments;
    for (auto& arg : block.arguments) {
        auto argumentIdentifier = createValueIdentifier(arg);
        auto blockArgument = std::make_shared<IR::Operations::BasicBlockArgument>(argumentIdentifier, arg.type);
        blockArguments.emplace_back(blockArgument);
        blockFrame.setValue(argumentIdentifier, blockArgument);
    }

    IR::BasicBlockPtr irBasicBlock = std::make_shared<IR::BasicBlock>(std::to_string(block.blockId),
                                                                      scope,
                                                                      /*operations*/ std::vector<IR::Operations::OperationPtr>{},
                                                                      /*arguments*/ blockArguments);
    blockMap[block.blockId] = irBasicBlock;
    for (auto& operation : block.operations) {
        processOperation(scope, blockFrame, block, irBasicBlock, operation);
    }
    return irBasicBlock;
}

void TraceToIRConversionPhase::IRConversionContext::processOperation(int32_t scope,
                                                                     ValueFrame& frame,
                                                                     Block& currentBlock,
                                                                     IR::BasicBlockPtr& currentIrBlock,
                                                                     Operation& operation) {

    switch (operation.op) {
        case ADD: {
            processAdd(scope, frame, currentIrBlock, operation);
            return;
        };
        case SUB: {
            processSub(scope, frame, currentIrBlock, operation);
            return;
        };
        case DIV: {
            processDiv(scope, frame, currentIrBlock, operation);
            return;
        };
        case MUL: {
            processMul(scope, frame, currentIrBlock, operation);
            return;
        };
        case EQUALS: {
            processEquals(scope, frame, currentIrBlock, operation);
            return;
        };
        case LESS_THAN: {
            processLessThan(scope, frame, currentIrBlock, operation);
            return;
        };
        case NEGATE: {
            processNegate(scope, frame, currentIrBlock, operation);
            return;
        };
        case AND: {
            processAnd(scope, frame, currentIrBlock, operation);
            return;
        };
        case OR: {
            processOr(scope, frame, currentIrBlock, operation);
            return;
        };
        case CMP: {
            processCMP(scope, frame, currentBlock, currentIrBlock, operation);
            return;
        };
        case JMP: {
            processJMP(scope, frame, currentIrBlock, operation);
            return;
        };
        case CONST: {
            auto valueRef = get<ConstantValue>(operation.input[0]);
            // TODO check data type
            auto intValue = std::static_pointer_cast<Interpreter::Integer>(valueRef.value);
            auto resultIdentifier = createValueIdentifier(operation.result);
            auto constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, intValue->value, 64);
            currentIrBlock->addOperation(constOperation);
            frame.setValue(resultIdentifier, constOperation);
            return;
        };
        case ASSIGN: break;
        case RETURN: {
            if (std::get<ValueRef>(operation.result).type == IR::Operations::VOID) {
                auto operation = std::make_shared<IR::Operations::ReturnOperation>();
                currentIrBlock->addOperation(operation);
            } else {
                auto returnValue = frame.getValue(createValueIdentifier(operation.input[0]));
                auto operation = std::make_shared<IR::Operations::ReturnOperation>(returnValue);
                currentIrBlock->addOperation(operation);
            }

            return;
        };
        case LOAD: {
            processLoad(scope, frame, currentIrBlock, operation);
            return;
        };
        case STORE: {
            processStore(scope, frame, currentIrBlock, operation);
            return;
        };
        case CALL: processCall(scope, frame, currentIrBlock, operation); return;
    }
    //  NES_NOT_IMPLEMENTED();
}

void TraceToIRConversionPhase::IRConversionContext::processJMP(int32_t scope,
                                                               ValueFrame& frame,
                                                               IR::BasicBlockPtr& block,
                                                               Operation& operation) {
    std::cout << "current block " << operation << std::endl;
    auto blockRef = get<BlockRef>(operation.input[0]);
    IR::Operations::BasicBlockInvocation blockInvocation;
    createBlockArguments(frame, blockInvocation, blockRef);

    if (blockMap.contains(blockRef.block)) {
        block->addNextBlock(blockMap[blockRef.block], blockInvocation.getArguments());
        return;
    }
    auto targetBlock = trace->getBlock(blockRef.block);
    // Problem:
    // trueCaseBlock = trace->getBlock(get<BlockRef>(operation.input[0]))
    // targetBlock   = get<BlockRef>(operation.input[0])

    // check if we jump to a loop head:
    if (targetBlock.operations.back().op == CMP) {
        auto trueCaseBlockRef = get<BlockRef>(operation.input[0]);
        if (isBlockInLoop(targetBlock.blockId, UINT32_MAX)) {
            NES_DEBUG("1. found loop");
            auto loopOperator = std::make_shared<IR::Operations::LoopOperation>(IR::Operations::LoopOperation::LoopType::ForLoop);
            loopOperator->setLoopInfo(std::make_shared<IR::Operations::DefaultLoopInfo>());
            auto loopHeadBlock = processBlock(scope + 1, trace->getBlock(blockRef.block));
            loopOperator->getLoopHeadBlock().setBlock(loopHeadBlock);
            for (auto& arg : blockRef.arguments) {
                auto arcIdentifier = createValueIdentifier(arg);
                auto argument = frame.getValue(arcIdentifier);
                loopOperator->getLoopHeadBlock().addArgument(argument);
            }
            blockMap[blockRef.block] = loopHeadBlock;
            block->addOperation(loopOperator);
            return;
        }
    }

    auto resultTargetBlock = processBlock(scope - 1, trace->getBlock(blockRef.block));
    blockMap[blockRef.block] = resultTargetBlock;
    block->addNextBlock(resultTargetBlock, blockInvocation.getArguments());
}

void TraceToIRConversionPhase::IRConversionContext::processCMP(int32_t scope,
                                                               ValueFrame& frame,
                                                               Block&,
                                                               IR::BasicBlockPtr& currentIrBlock,
                                                               Operation& operation) {

    auto valueRef = get<ValueRef>(operation.result);
    auto trueCaseBlockRef = get<BlockRef>(operation.input[0]);
    auto falseCaseBlockRef = get<BlockRef>(operation.input[1]);

    //  if (isBlockInLoop(scope, currentBlock.blockId, trueCaseBlockRef.block)) {
    //     NES_DEBUG("1. found loop");
    //} else if (isBlockInLoop(scope, currentBlock.blockId, falseCaseBlockRef.block)) {
    //    NES_DEBUG("2. found loop");
    //} else {
    auto booleanValue = frame.getValue(createValueIdentifier(valueRef));
    auto ifOperation = std::make_shared<IR::Operations::IfOperation>(booleanValue);
    auto trueCaseBlock = processBlock(scope + 1, trace->getBlock(trueCaseBlockRef.block));

    ifOperation->getTrueBlockInvocation().setBlock(trueCaseBlock);
    createBlockArguments(frame, ifOperation->getTrueBlockInvocation(), trueCaseBlockRef);

    auto falseCaseBlock = processBlock(scope + 1, trace->getBlock(falseCaseBlockRef.block));
    ifOperation->getFalseBlockInvocation().setBlock(falseCaseBlock);
    createBlockArguments(frame, ifOperation->getFalseBlockInvocation(), falseCaseBlockRef);
    currentIrBlock->addOperation(ifOperation);

    // find control-flow merge block for if
    auto controlFlowMergeBlockTrueCase = findControlFlowMerge(trueCaseBlock, scope);
    auto controlFlowMergeBlockFalseCase = findControlFlowMerge(falseCaseBlock, scope);
    //isCF = !isCF && (controlFlowMergeBlockTrueCase->getIdentifier() != controlFlowMergeBlockFalseCase->getIdentifier());
    if(controlFlowMergeBlockFalseCase) {
        if(controlFlowMergeBlockTrueCase->getIdentifier() != controlFlowMergeBlockFalseCase->getIdentifier()) {
            isSCF = false;
        }
    }
    NES_DEBUG("Found control-flow merge at: " << controlFlowMergeBlockTrueCase->getIdentifier());
    ifOperation->setMergeBlock(controlFlowMergeBlockTrueCase);
}

IR::BasicBlockPtr TraceToIRConversionPhase::IRConversionContext::findControlFlowMerge(IR::BasicBlockPtr currentBlock,
                                                                                      int32_t targetScope) {
    if (currentBlock->getScopeLevel() <= targetScope) {
        return currentBlock;
    }
    if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
        return findControlFlowMerge(branchOp->getNextBlockInvocation().getBlock(), targetScope);
    }
    if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::IfOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
        auto resultFromTrueBranch = findControlFlowMerge(branchOp->getTrueBlockInvocation().getBlock(), targetScope);
        if (resultFromTrueBranch) {
            return resultFromTrueBranch;
        }
        auto resultFromFalseBranch = findControlFlowMerge(branchOp->getFalseBlockInvocation().getBlock(), targetScope);
        if (resultFromFalseBranch) {
            return resultFromFalseBranch;
        }
    }
    if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::ReturnOp) {
        return nullptr;
    }
    NES_NOT_IMPLEMENTED();
}

std::vector<std::string> TraceToIRConversionPhase::IRConversionContext::createBlockArguments(BlockRef val) {
    std::vector<std::string> blockArgumentIdentifiers;
    for (auto& arg : val.arguments) {
        blockArgumentIdentifiers.emplace_back(createValueIdentifier(arg));
    }
    return blockArgumentIdentifiers;
}

void TraceToIRConversionPhase::IRConversionContext::createBlockArguments(ValueFrame& frame,
                                                                         IR::Operations::BasicBlockInvocation& blockInvocation,
                                                                         BlockRef val) {
    for (auto& arg : val.arguments) {
        auto valueIdentifier = createValueIdentifier(arg);
        blockInvocation.addArgument(frame.getValue(valueIdentifier));
    }
}

std::string TraceToIRConversionPhase::IRConversionContext::createValueIdentifier(InputVariant val) {
    if (holds_alternative<ValueRef>(val)) {
        auto valueRef = std::get<ValueRef>(val);
        return std::to_string(valueRef.blockId) + "_" + std::to_string(valueRef.operationId);
    } else
        return "";
}

void TraceToIRConversionPhase::IRConversionContext::processAdd(int32_t,
                                                               ValueFrame& frame,
                                                               IR::BasicBlockPtr& currentBlock,
                                                               Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto addOperation = std::make_shared<IR::Operations::AddOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, addOperation);
    currentBlock->addOperation(addOperation);
}

void TraceToIRConversionPhase::IRConversionContext::processSub(int32_t,
                                                               ValueFrame& frame,
                                                               IR::BasicBlockPtr& currentBlock,
                                                               Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto subOperation = std::make_shared<IR::Operations::SubOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, subOperation);
    currentBlock->addOperation(subOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processMul(int32_t,
                                                               ValueFrame& frame,
                                                               IR::BasicBlockPtr& currentBlock,
                                                               Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto mulOperation = std::make_shared<IR::Operations::MulOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, mulOperation);
    currentBlock->addOperation(mulOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processDiv(int32_t,
                                                               ValueFrame& frame,
                                                               IR::BasicBlockPtr& currentBlock,
                                                               Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto divOperation = std::make_shared<IR::Operations::DivOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, divOperation);
    currentBlock->addOperation(divOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processNegate(int32_t,
                                                                  ValueFrame& frame,
                                                                  IR::BasicBlockPtr& currentBlock,
                                                                  Operation& operation) {
    auto input = frame.getValue(createValueIdentifier(operation.input[0]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto negateOperation = std::make_shared<IR::Operations::NegateOperation>(resultIdentifier, input);
    frame.setValue(resultIdentifier, negateOperation);
    currentBlock->addOperation(negateOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processLessThan(int32_t,
                                                                    ValueFrame& frame,
                                                                    IR::BasicBlockPtr& currentBlock,
                                                                    Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto compareOperation =
        std::make_shared<IR::Operations::CompareOperation>(resultIdentifier,
                                                           leftInput,
                                                           rightInput,
                                                           IR::Operations::CompareOperation::Comparator::ISLT);
    frame.setValue(resultIdentifier, compareOperation);
    currentBlock->addOperation(compareOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processEquals(int32_t,
                                                                  ValueFrame& frame,
                                                                  IR::BasicBlockPtr& currentBlock,
                                                                  Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto compareOperation = std::make_shared<IR::Operations::CompareOperation>(resultIdentifier,
                                                                               leftInput,
                                                                               rightInput,
                                                                               IR::Operations::CompareOperation::Comparator::IEQ);
    frame.setValue(resultIdentifier, compareOperation);
    currentBlock->addOperation(compareOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processAnd(int32_t,
                                                               ValueFrame& frame,
                                                               IR::BasicBlockPtr& currentBlock,
                                                               Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto andOperation = std::make_shared<IR::Operations::AndOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, andOperation);
    currentBlock->addOperation(andOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processOr(int32_t,
                                                              ValueFrame& frame,
                                                              IR::BasicBlockPtr& currentBlock,
                                                              Operation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation = std::make_shared<IR::Operations::OrOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processLoad(int32_t,
                                                                ValueFrame& frame,
                                                                IR::BasicBlockPtr& currentBlock,
                                                                Operation& operation) {
    // TODO add load data type
    //auto constOperation = std::make_shared<IR::Operations::LoadOperation>(createValueIdentifier(operation.result),
    //                                                                      createValueIdentifier(operation.input[0]),
    //                                                                      IR::Operations::Operation::BasicType::VOID);
    //currentBlock->addOperation(constOperation);
    auto address = frame.getValue(createValueIdentifier(operation.input[0]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto resultType = std::get<ValueRef>(operation.result).type;
    auto loadOperation = std::make_shared<IR::Operations::LoadOperation>(resultIdentifier, address, resultType);
    frame.setValue(resultIdentifier, loadOperation);
    currentBlock->addOperation(loadOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processStore(int32_t,
                                                                 ValueFrame& frame,
                                                                 IR::BasicBlockPtr& currentBlock,
                                                                 Operation& operation) {
    auto address = frame.getValue(createValueIdentifier(operation.input[1]));
    auto value = frame.getValue(createValueIdentifier(operation.input[0]));
    auto storeOperation = std::make_shared<IR::Operations::StoreOperation>(address, value);
    currentBlock->addOperation(storeOperation);
}

void TraceToIRConversionPhase::IRConversionContext::processCall(int32_t,
                                                                ValueFrame& frame,
                                                                IR::BasicBlockPtr& currentBlock,
                                                                Operation& operation) {

    auto inputArguments = std::vector<IR::Operations::OperationWPtr>{};
    auto functionCallTarget = std::get<FunctionCallTarget>(operation.input[0]);

    for (uint32_t i = 1; i < operation.input.size(); i++) {
        auto input = frame.getValue(createValueIdentifier(operation.input[i]));
        inputArguments.emplace_back(input);
    }

    auto resultType = std::holds_alternative<None>(operation.result) ? IR::Operations::PrimitiveStamp::VOID
                                                                     : std::get<ValueRef>(operation.result).type;
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto proxyCallOperation =
        std::make_shared<IR::Operations::ProxyCallOperation>(IR::Operations::ProxyCallOperation::ProxyCallType::Other,
                                                             functionCallTarget.mangledName,
                                                             functionCallTarget.functionPtr,
                                                             resultIdentifier,
                                                             inputArguments,
                                                             resultType);
    if (resultType != IR::Operations::VOID) {
        frame.setValue(resultIdentifier, proxyCallOperation);
    }
    currentBlock->addOperation(proxyCallOperation);
}

bool TraceToIRConversionPhase::IRConversionContext::isBlockInLoop(uint32_t parentBlockId,
                                                                  uint32_t currentBlockId) {
    if (currentBlockId == parentBlockId) {
        return true;
    }
    if(currentBlockId == UINT32_MAX) {
        currentBlockId = parentBlockId;
    }
    auto currentBlock = trace->getBlock(currentBlockId);
    auto& terminationOp = currentBlock.operations.back();
    if (terminationOp.op == CMP) {
        auto trueCaseBlockRef = get<BlockRef>(terminationOp.input[0]);
        auto falseCaseBlockRef = get<BlockRef>(terminationOp.input[1]);
        return isBlockInLoop(parentBlockId, trueCaseBlockRef.block)
            || isBlockInLoop(parentBlockId, falseCaseBlockRef.block);
    } else if (terminationOp.op == JMP) {
        auto target = get<BlockRef>(terminationOp.input[0]);
        return isBlockInLoop(parentBlockId, target.block);
    }
    return false;
}

}// namespace NES::ExecutionEngine::Experimental::Trace