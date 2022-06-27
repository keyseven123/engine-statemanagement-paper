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

#include "Experimental/NESIR/Operations/Operation.hpp"
#include <Experimental/NESIR/Operations/StoreOperation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

StoreOperation::StoreOperation(OperationPtr value, OperationPtr address)
    : Operation(OperationType::StoreOp, VOID), value(value), address(address) {}

OperationPtr StoreOperation::getValue() { return value.lock(); }
OperationPtr StoreOperation::getAddress() { return address.lock(); }

std::string StoreOperation::toString() {
    return "StoreOperation(" + getValue()->toString() + ", " + getAddress()->toString() + ")";
}
}// namespace NES::ExecutionEngine::Experimental::IR::Operations
