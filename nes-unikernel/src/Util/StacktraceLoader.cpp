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
#ifdef USE_LIB_DWARF
#include <Util/Backward/backward.hpp>
#endif
#include <Util/Logger/Logger.hpp>
#include <Util/StacktraceLoader.hpp>

#define CALLSTACK_MAX_SIZE 32

namespace NES {
/**
 * @brief This methods collects the call stacks and prints is
 */
std::string collectStacktrace() {
#ifdef USE_LIB_DWARF
    backward::StackTrace stackTrace;
    backward::Printer printer;
    stackTrace.load_here(CALLSTACK_MAX_SIZE);
    printer.object = true;
    printer.color_mode = backward::ColorMode::always;
    std::stringbuf buffer;
    std::ostream os(&buffer);
    printer.print(stackTrace, os);
    return buffer.str();
#endif
    return "Stacktrace not available";
}

/**
 * @brief This methods collects the call stacks and prints is
 */
std::string collectAndPrintStacktrace() {
    auto stacktrace = collectStacktrace();
    NES_ERROR("{}", stacktrace);
    return stacktrace;
}

void nesErrorHandler() { collectAndPrintStacktrace(); }

}// namespace NES
