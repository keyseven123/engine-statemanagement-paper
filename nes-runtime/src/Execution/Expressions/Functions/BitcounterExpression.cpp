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

#include <Execution/Expressions/Functions/BitcounterExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

BitcounterExpression::BitcounterExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& SubExpression)
    : SubExpression(SubExpression) {}

double bitcounter(double number) { return std::log2(number) + 1; }

Value<> BitcounterExpression::execute(NES::Nautilus::Record& record) const {
    Value leftValue = SubExpression->execute(record);
    Value<> result = FunctionCall<>("bitcounter", bitcounter, leftValue.as<Double>());
    return result;
}

}// namespace NES::Runtime::Execution::Expressions