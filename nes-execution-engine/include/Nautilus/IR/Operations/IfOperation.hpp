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

#ifndef NES_IFOPERATION_HPP
#define NES_IFOPERATION_HPP

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
class IfOperation : public Operation {
  public:
    IfOperation(OperationPtr booleanValue);
    ~IfOperation() override = default;

    OperationPtr getValue();

    BasicBlockInvocation& getTrueBlockInvocation();
    BasicBlockInvocation& getFalseBlockInvocation();
    BasicBlockPtr getMergeBlock();
    void setMergeBlock(BasicBlockPtr mergeBlock);
    bool hasFalseCase();

    std::string toString() override;

  private:
    OperationWPtr booleanValue;
    BasicBlockInvocation trueBlockInvocation;
    BasicBlockInvocation falseBlockInvocation;
    std::weak_ptr<BasicBlock> mergeBlock;
};
}// namespace NES
#endif//NES_IFOPERATION_HPP
