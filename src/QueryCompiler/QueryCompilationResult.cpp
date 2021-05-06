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
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <exception>

namespace NES {
namespace QueryCompilation {

QueryCompilationResult::QueryCompilationResult(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan)
    : executableQueryPlan(executableQueryPlan) {}
QueryCompilationResult::QueryCompilationResult(std::exception_ptr exception) : exception(exception) {}

QueryCompilationResultPtr QueryCompilationResult::create(NodeEngine::Execution::NewExecutableQueryPlanPtr qep) {
    return std::make_shared<QueryCompilationResult>(QueryCompilationResult(qep));
}
QueryCompilationResultPtr QueryCompilationResult::create(std::exception_ptr exception) {
    return std::make_shared<QueryCompilationResult>(QueryCompilationResult(exception));
}

NodeEngine::Execution::NewExecutableQueryPlanPtr QueryCompilationResult::getExecutableQueryPlan() {
    if (hasError()) {
        std::rethrow_exception(exception.value());
    }
    return executableQueryPlan.value();
}

bool QueryCompilationResult::hasError() { return exception.has_value(); }

std::exception_ptr QueryCompilationResult::getError() { return exception.value(); }

}// namespace QueryCompilation
}// namespace NES