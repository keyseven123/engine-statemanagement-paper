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

#ifndef INCLUDE_UTIL_LOGGER_HPP_
#define INCLUDE_UTIL_LOGGER_HPP_
// TRACE < DEBUG < INFO < WARN < ERROR < FATAL

#include <Exceptions/NesRuntimeException.hpp>
#include <NodeEngine/ErrorListener.hpp>
#include <Util/StacktraceLoader.hpp>
#include <iostream>
#include <log4cxx/consoleappender.h>
#include <log4cxx/fileappender.h>
#include <log4cxx/logger.h>
#include <log4cxx/patternlayout.h>
#include <mutex>

using namespace log4cxx;
using namespace log4cxx::helpers;

namespace NES {

enum DebugLevel { LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE };

static std::string getDebugLevelAsString(DebugLevel level) {
    if (level == LOG_NONE) {
        return "LOG_NONE";
    } else if (level == LOG_WARNING) {
        return "LOG_WARNING";
    } else if (level == LOG_DEBUG) {
        return "LOG_DEBUG";
    } else if (level == LOG_INFO) {
        return "LOG_INFO";
    } else if (level == LOG_TRACE) {
        return "LOG_TRACE";
    } else {
        return "UNKNOWN";
    }
}

static DebugLevel getStringAsDebugLevel(std::string level) {
    if (level == "LOG_NONE") {
        return LOG_NONE;
    } else if (level == "LOG_WARNING") {
        return LOG_WARNING;
    } else if (level == "LOG_DEBUG") {
        return LOG_DEBUG;
    } else if (level == "LOG_INFO") {
        return LOG_INFO;
    } else if (level == "LOG_TRACE") {
        return LOG_TRACE;
    } else {
        throw std::runtime_error("Logger: Debug level unknown: " + level);
    }
}

static log4cxx::LoggerPtr NESLogger(log4cxx::Logger::getLogger("NES"));
}// namespace NES
// LoggerPtr logger(Logger::getLogger("NES"));

// TRACE < DEBUG < INFO < WARN < ERROR < FATAL
namespace NES {
namespace detail {

/**
 * @brief This class is necessary to silence the compiler regarding unused variables
 */
struct LoggingBlackHole {
    template<typename T>
    LoggingBlackHole& operator<<(const T&) {
        return *this;
    }
};

}// namespace detail
}// namespace NES
#ifdef NES_LOGGING_TRACE_LEVEL
#define NES_TRACE(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_TRACE(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#define NES_DEBUG(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_DEBUG(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#define NES_INFO(TEXT)                                                                                                           \
    do {                                                                                                                         \
        LOG4CXX_INFO(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_WARNING(TEXT)                                                                                                        \
    do {                                                                                                                         \
        LOG4CXX_WARN(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#endif
#ifdef NES_LOGGING_DEBUG_LEVEL
#define NES_TRACE(TEXT)                                                                                                          \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_DEBUG(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_DEBUG(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#define NES_INFO(TEXT)                                                                                                           \
    do {                                                                                                                         \
        LOG4CXX_INFO(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_WARNING(TEXT)                                                                                                        \
    do {                                                                                                                         \
        LOG4CXX_WARN(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#endif
#ifdef NES_LOGGING_INFO_LEVEL
#define NES_TRACE(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_DEBUG(TEXT)                                                                                                          \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_INFO(TEXT)                                                                                                           \
    do {                                                                                                                         \
        LOG4CXX_INFO(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_WARNING(TEXT)                                                                                                        \
    do {                                                                                                                         \
        LOG4CXX_WARN(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#endif
#ifdef NES_LOGGING_WARNING_LEVEL
#define NES_TRACE(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_DEBUG(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_INFO(TEXT)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_WARNING(TEXT)                                                                                                        \
    do {                                                                                                                         \
        LOG4CXX_WARN(NES::NESLogger, TEXT);                                                                                      \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#endif
#ifdef NES_LOGGING_ERROR_LEVEL
#define NES_TRACE(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_DEBUG(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_INFO(...)                                                                                                            \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_WARNING(...)                                                                                                         \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#endif
#ifdef NES_LOGGING_FATAL_ERROR_LEVEL
#define NES_TRACE(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_DEBUG(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
        ;                                                                                                                        \
    } while (0)
#define NES_INFO(...)                                                                                                            \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_WARNING(...)                                                                                                         \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, TEXT);                                                                                     \
    } while (0)
#endif
#ifdef NES_LOGGING_NO_LEVEL
#define NES_TRACE(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_DEBUG(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_INFO(...)                                                                                                            \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_WARNING(...)                                                                                                         \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#define NES_ERROR(...)                                                                                                           \
    do {                                                                                                                         \
        NES::detail::LoggingBlackHole bh;                                                                                        \
        ((void) (bh << __VA_ARGS__));                                                                                            \
    } while (0)
#endif

namespace NES {
namespace NodeEngine {
void invokeErrorHandlers(std::string buffer, std::string&& stacktrace);
}
}// namespace NES

/// I am aware that we do not like __ before variable names but here we need them
/// to avoid name collions, e.g., __buffer, __stacktrace
/// that should not be a problem because of the scope, however, better be safe than sorry :P
#ifdef NES_DEBUG_MODE
//Note Verify is only evaluated in Debug but not in Release
#define NES_VERIFY(CONDITION, TEXT)                                                                                              \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            LOG4CXX_ERROR(NES::NESLogger, "NES Fatal Error on " #CONDITION << " message: " << TEXT);                             \
            {                                                                                                                    \
                auto __stacktrace = collectAndPrintStacktrace();                                                                 \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << "Failed assertion on " #CONDITION;                                                                       \
                __os << " error message: " << TEXT;                                                                              \
                NES::NodeEngine::invokeErrorHandlers(__buffer.str(), std::move(__stacktrace));                                   \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)
#else
#define NES_VERIFY(CONDITION, TEXT) ((void) 0)
#endif

#define NES_ASSERT(CONDITION, TEXT)                                                                                              \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            LOG4CXX_ERROR(NES::NESLogger, "NES Fatal Error on " #CONDITION << " message: " << TEXT);                             \
            {                                                                                                                    \
                auto __stacktrace = collectAndPrintStacktrace();                                                                 \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << "Failed assertion on " #CONDITION;                                                                       \
                __os << " error message: " << TEXT;                                                                              \
                NES::NodeEngine::invokeErrorHandlers(__buffer.str(), std::move(__stacktrace));                                   \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)

#define NES_ASSERT2_FMT(CONDITION, ...)                                                                                          \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            LOG4CXX_ERROR(NES::NESLogger, "NES Fatal Error on " #CONDITION << " message: " << __VA_ARGS__);                      \
            {                                                                                                                    \
                auto __stacktrace = collectAndPrintStacktrace();                                                                 \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << "Failed assertion on " #CONDITION;                                                                       \
                __os << " error message: " << __VA_ARGS__;                                                                       \
                NES::NodeEngine::invokeErrorHandlers(__buffer.str(), std::move(__stacktrace));                                   \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)

#define NES_THROW_RUNTIME_ERROR(...)                                                                                             \
    do {                                                                                                                         \
        auto __stacktrace = collectAndPrintStacktrace();                                                                         \
        std::stringbuf __buffer;                                                                                                 \
        std::ostream __os(&__buffer);                                                                                            \
        __os << __VA_ARGS__;                                                                                                     \
        LOG4CXX_ERROR(NES::NESLogger, __VA_ARGS__);                                                                              \
        throw NesRuntimeException(__buffer.str(), std::move(__stacktrace));                                                      \
    } while (0)

#define NES_FATAL_ERROR(...)                                                                                                     \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NES::NESLogger, __VA_ARGS__);                                                                              \
    } while (0)

namespace NES {
static void setupLogging(std::string logFileName, DebugLevel level) {
    std::cout << "Logger: SETUP_LOGGING" << std::endl;
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c: %l [%M] %X{threadName} [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, logFileName);
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));
#ifdef NES_LOGGING_NO_LEVEL
    NESLogger->setLevel(log4cxx::Level::getOff());
#else
    // set log level
    switch (level) {
        case LOG_NONE: {
            NESLogger->setLevel(log4cxx::Level::getOff());
            break;
        }
        case LOG_WARNING: {
            NESLogger->setLevel(log4cxx::Level::getWarn());
            break;
        }
        case LOG_DEBUG: {
            NESLogger->setLevel(log4cxx::Level::getDebug());
            break;
        }
        case LOG_INFO: {
            NESLogger->setLevel(log4cxx::Level::getInfo());
            break;
        }
        case LOG_TRACE: {
            NESLogger->setLevel(log4cxx::Level::getTrace());
            break;
        }
        default: {
            NES_FATAL_ERROR("setupLogging: log level not supported " << getDebugLevelAsString(level));
        }
    }
#endif
    NESLogger->addAppender(file);
    NESLogger->addAppender(console);
}

static void setLogLevel(DebugLevel level) {
    // set log level
#ifdef NES_LOGGING_NO_LEVEL
    NESLogger->setLevel(log4cxx::Level::getOff());
#else
    // set log level
    switch (level) {
        case LOG_NONE: {
            NESLogger->setLevel(log4cxx::Level::getOff());
            break;
        }
        case LOG_WARNING: {
            NESLogger->setLevel(log4cxx::Level::getWarn());
            break;
        }
        case LOG_DEBUG: {
            NESLogger->setLevel(log4cxx::Level::getDebug());
            break;
        }
        case LOG_INFO: {
            NESLogger->setLevel(log4cxx::Level::getInfo());
            break;
        }
        case LOG_TRACE: {
            NESLogger->setLevel(log4cxx::Level::getTrace());
            break;
        }
        default: {
            NES_FATAL_ERROR("setupLogging: log level not supported " << getDebugLevelAsString(level));
        }
    }
#endif
}

#define NES_NOT_IMPLEMENTED()                                                                                                    \
    do {                                                                                                                         \
        NES_ERROR("Function Not Implemented!");                                                                                  \
        throw Exception("not implemented");                                                                                      \
    } while (0)

}// namespace NES

#endif /* INCLUDE_UTIL_LOGGER_HPP_ */
