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

#include <API/Query.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/Loop/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <memory>
#include <mlir/IR/AsmState.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Transforms/DialectConversion.h>
#include <unordered_map>

using namespace std;
using namespace NES::ExecutionEngine::Experimental::MLIR;
using namespace NES::ExecutionEngine::Experimental::IR;
using namespace NES::ExecutionEngine::Experimental::IR::Operations;

namespace NES {
class MLIR_LOOP_GenerationTest : public testing::Test {
  public:
    NES::ExecutionEngine::Experimental::MLIR::MLIRUtility* mlirUtility;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIR_LOOP_GenerationTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
    }

    void SetUp() override { mlirUtility = new NES::ExecutionEngine::Experimental::MLIR::MLIRUtility("", false); }
    static void TearDownTestCase() { NES_INFO("MLIR_LOOP_GenerationTest test class TearDownTestCase."); }
};

template<typename... Args>
BasicBlockPtr
createBB(std::string identifier, int level, std::vector<std::shared_ptr<Operations::BasicBlockArgument>> arguments) {
    return std::make_shared<BasicBlock>(identifier, level, std::vector<OperationPtr>{}, arguments);
}

/**
 * @brief Test MLIR Generation for Add Float Operation
 *
 * def (int x)
 *  for(int i =0; i< 10;i++){
 *      x = x +1
 *  }
 *  return x;
 */
TEST_F(MLIR_LOOP_GenerationTest, testIfEquals) {
    auto nesIR = std::make_shared<NESIR>();

    auto x = std::make_shared<BasicBlockArgument>("x", PrimitiveStamp::INT64);
    auto rootBasicBlock = createBB("executeBodyBB", 0, {x});

    auto stepSize = std::make_shared<ConstIntOperation>("stepSize", 1, 64);
    rootBasicBlock->addOperation(stepSize);
    auto lowerBound = std::make_shared<ConstIntOperation>("lowerBound", 0, 64);
    rootBasicBlock->addOperation(lowerBound);
    auto upperBound = std::make_shared<ConstIntOperation>("upperBound", 10, 64);
    rootBasicBlock->addOperation(upperBound);
    auto loopOperation = std::make_shared<LoopOperation>(LoopOperation::ForLoop, std::vector<std::string>{"int64Const1", "x"});
    auto loopInfo = std::make_shared<CountedLoopInfo>();
    loopInfo->stepSize = stepSize;
    loopInfo->lowerBound = lowerBound;
    loopInfo->upperBound = upperBound;
    loopInfo->iteratorArguments.emplace_back(x);
    loopOperation->setLoopInfo(loopInfo);
    rootBasicBlock->addOperation(loopOperation);
    // loop header
    {
        // loop body block
        {
            auto i2 = std::make_shared<BasicBlockArgument>("i2", PrimitiveStamp::INT64);
            auto x2 = std::make_shared<BasicBlockArgument>("x2", PrimitiveStamp::INT64);
            auto loopBodyBlock = createBB("loopHeader", 2, {x2});
            loopInfo->loopBodyBlock = loopBodyBlock;
            auto oneConst = std::make_shared<ConstIntOperation>("oneConst", 1, 64);
            loopBodyBlock->addOperation(oneConst);
            auto addResult = std::make_shared<AddOperation>("add", x2, oneConst);
            loopBodyBlock->addOperation(addResult);
            loopBodyBlock->addNextBlock(rootBasicBlock, {addResult});
        }

        // loop end control-flow-merge block
        {
            auto x3 = std::make_shared<BasicBlockArgument>("x3", PrimitiveStamp::INT64);
            auto loopEndControlFlowMergeBlock = createBB("loopEnd", 0, {x3});
            loopInfo->loopEndBlock = loopEndControlFlowMergeBlock;
            auto returnOperation = std::make_shared<ReturnOperation>(x3);
            loopEndControlFlowMergeBlock->addOperation(returnOperation);
        }
    }

    std::vector<PrimitiveStamp> executeArgTypes = {PrimitiveStamp::INT64};
    std::vector<std::string> executeArgNames = {"x"};
    auto functionOp = std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, PrimitiveStamp::INT64);
    nesIR->addRootOperation(functionOp)->addFunctionBasicBlock(rootBasicBlock);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int (*)(int)) engine->lookup("execute").get();
    ASSERT_EQ(function(10), 20);
    ASSERT_EQ(function(42), 52);
}

/**
 * @brief Test MLIR Generation for Add Float Operation
 *
 * def (int x)
 *  for(int i =0; i< 10;i++){
 *      x = x +1
 *  }
 *  return x;
 */
TEST_F(MLIR_LOOP_GenerationTest, testDefaultLoop) {
    auto nesIR = std::make_shared<NESIR>();

    auto x = std::make_shared<BasicBlockArgument>("x", PrimitiveStamp::INT64);
    auto rootBasicBlock = createBB("executeBodyBB", 0, {x});

    auto lowerBound = std::make_shared<ConstIntOperation>("lowerBound", 0, 64);
    rootBasicBlock->addOperation(lowerBound);
    auto loopOperation = std::make_shared<LoopOperation>(LoopOperation::ForLoop, std::vector<std::string>{"lowerBound", "x"});
    auto loopInfo = std::make_shared<DefaultLoopInfo>();
    loopOperation->setLoopInfo(loopInfo);
    rootBasicBlock->addOperation(loopOperation);
    // loop header
    {
        // loop head
        {
            auto i2 = std::make_shared<BasicBlockArgument>("i2", PrimitiveStamp::INT64);
            auto x2 = std::make_shared<BasicBlockArgument>("x2", PrimitiveStamp::INT64);
            auto loopHeaderBlock = createBB("loopHeader", 2, {i2, x2});
            loopOperation->getLoopHeadBlock().setBlock(loopHeaderBlock);
            auto upperBound = std::make_shared<ConstIntOperation>("upperBound", 10, 64);
            loopHeaderBlock->addOperation(upperBound);
            auto compOp = std::make_shared<CompareOperation>("comp", i2, upperBound, CompareOperation::Comparator::ISLT);
            loopHeaderBlock->addOperation(compOp);
            auto ifOp = std::make_shared<IfOperation>(compOp);
            loopHeaderBlock->addOperation(ifOp);

            // loop body block
            {
                auto i3 = std::make_shared<BasicBlockArgument>("i3", PrimitiveStamp::INT64);
                auto x3 = std::make_shared<BasicBlockArgument>("x3", PrimitiveStamp::INT64);
                auto loopBodyBlock = createBB("loopBodyBlock", 2, {i3, x3});
                ifOp->getTrueBlockInvocation().setBlock(loopBodyBlock);
                ifOp->getTrueBlockInvocation().addArgument(i2);
                ifOp->getTrueBlockInvocation().addArgument(x2);
                auto oneConst = std::make_shared<ConstIntOperation>("oneConst", 1, 64);
                loopBodyBlock->addOperation(oneConst);
                auto addResult = std::make_shared<AddOperation>("add", x3, oneConst);
                loopBodyBlock->addOperation(addResult);
                auto addI = std::make_shared<AddOperation>("addI", i3, oneConst);
                loopBodyBlock->addOperation(addI);
                loopBodyBlock->addNextBlock(loopHeaderBlock, {addI, addResult});
            }

            // loop end control-flow-merge block
            {
                auto x3 = std::make_shared<BasicBlockArgument>("x3", PrimitiveStamp::INT64);
                auto loopEndControlFlowMergeBlock = createBB("loopEnd", 0, {x3});
                ifOp->getFalseBlockInvocation().setBlock(loopEndControlFlowMergeBlock);
                ifOp->getFalseBlockInvocation().addArgument(x2);
                auto returnOperation = std::make_shared<ReturnOperation>(x3);
                loopEndControlFlowMergeBlock->addOperation(returnOperation);
            }
        }
    }

    std::vector<PrimitiveStamp> executeArgTypes = {PrimitiveStamp::INT64};
    std::vector<std::string> executeArgNames = {"x"};
    auto functionOp = std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, PrimitiveStamp::INT64);
    nesIR->addRootOperation(functionOp)->addFunctionBasicBlock(rootBasicBlock);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int (*)(int)) engine->lookup("execute").get();
    ASSERT_EQ(function(10), 20);
    ASSERT_EQ(function(42), 52);
}

}// namespace NES