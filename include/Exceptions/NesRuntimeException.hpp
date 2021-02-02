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

#ifndef NES_SRC_EXCEPTIONS_NESLOGICEXCEPTION_HPP_
#define NES_SRC_EXCEPTIONS_NESLOGICEXCEPTION_HPP_

#include <exception>
#include <stdexcept>
#include <string>

namespace NES {

/**
 * @brief Exception to be used to report errors and stacktraces
 * This is meant to be used for NES-related errors, wrap std exceptions with their own stacktrace, etc..
 */
class NesRuntimeException : virtual public std::exception {

  protected:
    std::string errorMessage;///< Error message

  public:
    /** Constructor
     *  @param msg The error message
     *  @param stacktrace Error stacktrace
     */
    explicit NesRuntimeException(const std::string& msg, std::string&& stacktrace);

    /** Constructor
    *  @param msg The error message
    *  @param stacktrace Error stacktrace
    */
    explicit NesRuntimeException(const std::string& msg, const std::string& stacktrace);

    /** Destructor.
     *  Virtual to allow for subclassing.
     */
    virtual ~NesRuntimeException() throw() {}

    /** Returns a pointer to the (constant) error description.
     *  @return A pointer to a const char*. The underlying memory
     *  is in possession of the Except object. Callers must
     *  not attempt to free the memory.
     */
    virtual const char* what() const throw();
};

}// namespace NES

#endif//NES_SRC_EXCEPTIONS_NESLOGICEXCEPTION_HPP_
