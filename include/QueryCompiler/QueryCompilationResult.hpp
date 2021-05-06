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
#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_

#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <optional>

namespace NES {
namespace QueryCompilation {

/**
 * @brief Provides the query compilation results.
 * Query compilation can succeed, in this case the result contains a ExecutableQueryPlan pointer.
 * If query compilation fails, the result contains the error and hasError() return true.
 */
class QueryCompilationResult {
  public:
    static QueryCompilationResultPtr create(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan);
    static QueryCompilationResultPtr create(std::exception_ptr exception);
    /**
     * @brief Returns the query execution plan if hasError() == false.
     * @throws QueryCompilationException if hasError() == true.
     * @return NewExecutableQueryPlanPtr
     */
    NodeEngine::Execution::NewExecutableQueryPlanPtr getExecutableQueryPlan();

    /**
     * @brief Indicates if the query compilation succeeded.
     * @return true if the request has an error
     */
    bool hasError();

    /**
     * @brief Returns the exception
     * @return std::exception_ptr
     */
    std::exception_ptr getError();

  private:
    QueryCompilationResult(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan);
    QueryCompilationResult(std::exception_ptr exception);
    std::optional<NodeEngine::Execution::NewExecutableQueryPlanPtr> executableQueryPlan;
    std::optional<std::exception_ptr> exception;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_
