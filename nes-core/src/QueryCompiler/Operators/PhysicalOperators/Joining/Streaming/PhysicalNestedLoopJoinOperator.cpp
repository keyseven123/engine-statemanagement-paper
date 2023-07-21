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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalNestedLoopJoinOperator::PhysicalNestedLoopJoinOperator(
    Runtime::Execution::Operators::NLJOperatorHandlerPtr operatorHandler,
    OperatorId id)
    : operatorHandler(std::move(operatorHandler)) {
    this->operatorHandler->addOperatorId(id);
}

Runtime::Execution::Operators::NLJOperatorHandlerPtr PhysicalNestedLoopJoinOperator::getOperatorHandler() const {
    return operatorHandler;
}
}// namespace NES::QueryCompilation::PhysicalOperators