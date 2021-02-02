/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Exceptions/NesRuntimeException.hpp>

namespace NES {

NesRuntimeException::NesRuntimeException(const std::string& msg, std::string&& stacktrace) : errorMessage(msg) {
    errorMessage.append(":: callstack:\n");
    errorMessage.append(std::move(stacktrace));
}

NesRuntimeException::NesRuntimeException(const std::string& msg, const std::string& stacktrace) : errorMessage(msg) {
    errorMessage.append(":: callstack:\n");
    errorMessage.append(stacktrace);
}

const char* NesRuntimeException::what() const throw() { return errorMessage.c_str(); }

}// namespace NES