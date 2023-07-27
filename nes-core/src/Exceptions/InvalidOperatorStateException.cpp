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

#include <Exceptions/InvalidOperatorStateException.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>

namespace NES::Exceptions {

InvalidOperatorStateException::InvalidOperatorStateException(NES::OperatorId operatorId,
                                                             const std::vector<OperatorState>& expectedState,
                                                             NES::OperatorState actualState)
    : RequestExecutionException("Invalid operator state") {

    std::stringstream expectedStatusString;
    for (const auto& state : expectedState) {
        expectedStatusString << std::string(magic_enum::enum_name(state)) << " ";
    }
    message = "InvalidOperatorStateException: Operator with id" + std::to_string(operatorId)
        + "Expected operator state to be in [" + expectedStatusString.str() + "] but found to be in "
        + std::string(magic_enum::enum_name(actualState));
}

const char* InvalidOperatorStateException::what() const noexcept { return message.c_str(); }

}// namespace NES::Exceptions
