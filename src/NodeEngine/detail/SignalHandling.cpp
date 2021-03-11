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
#include <NodeEngine/ErrorListener.hpp>
#include <NodeEngine/internal/backtrace.hpp>
#include <Util/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <csignal>
#include <memory>
#include <mutex>

namespace NES::NodeEngine {

/// this mutex protected the globalErrorListeners vector
static std::mutex globalErrorListenerMutex;
/// this vector contains system-wide error listeners, e.g., NodeEngine and CoordinatorEngine
static std::vector<std::shared_ptr<ErrorListener>> globalErrorListeners;

/**
 * @brief calls to this function will create a NesRuntimeException that is passed to all system-wide error listeners
 * @param buffer the message of the exception
 * @param stacktrace the stacktrace of where the error was raised
 */
void invokeErrorHandlers(const std::string buffer, std::string&& stacktrace) {
    NES_DEBUG("invokeErrorHandlers with buffer=" << buffer << " trace=" << stacktrace);
    std::unique_lock lock(globalErrorListenerMutex);
    auto exception = std::make_shared<NesRuntimeException>(buffer, stacktrace);
    for (auto& listener : globalErrorListeners) {
        listener->onFatalException(exception, stacktrace);
    }
    std::exit(1);
}

/**
 * @brief make an error listener system-wide
 * @param listener the error listener to make system-wide
 */
void installGlobalErrorListener(std::shared_ptr<ErrorListener> listener) {
    NES_DEBUG("installGlobalErrorListener");
    std::unique_lock lock(globalErrorListenerMutex);
    if (listener) {
        globalErrorListeners.emplace_back(listener);
    }
}

namespace detail {
static backward::SignalHandling sh;

/// called when a signal is intercepted
void nesErrorHandler(int signal) {
    auto stacktrace = collectAndPrintStacktrace();
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener->onFatalError(signal, stacktrace);
        }
    }
    std::exit(1);
}

/// called when std::terminate() is invoked
void nesTerminateHandler() {
    auto stacktrace = collectAndPrintStacktrace();
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try {
        if (unknown) {
            std::rethrow_exception(unknown);
        } else {
            // normal termination
            return;
        }
    } catch (const std::exception& e) {// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    } catch (...) {// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener->onFatalException(currentException, stacktrace);
        }
    }
    std::exit(1);
}

/// called when an exception is not caught in our code
void nesUnexpectedException() {
    auto stacktrace = collectAndPrintStacktrace();
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try {
        if (unknown) {
            std::rethrow_exception(unknown);
        } else {
            throw std::runtime_error("Unknown invalid exception caught");
        }
    } catch (const std::exception& e) {// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    } catch (...) {// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener->onFatalException(currentException, stacktrace);
        }
    }
    std::exit(1);
}

struct ErrorHandlerLoader {
  public:
    explicit ErrorHandlerLoader() {
        std::set_terminate(nesTerminateHandler);
        std::set_unexpected(nesUnexpectedException);
        std::signal(SIGABRT, nesErrorHandler);
        std::signal(SIGSEGV, nesErrorHandler);
        std::signal(SIGBUS, nesErrorHandler);
    }
};
static ErrorHandlerLoader loader;

}// namespace detail
}// namespace NES::NodeEngine
