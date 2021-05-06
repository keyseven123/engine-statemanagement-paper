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
#include <QueryCompiler/QueryCompilationRequest.hpp>

namespace NES {
namespace QueryCompilation {

QueryCompilationRequest::Builder::Builder(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine)
    : request(QueryCompilationRequest::create(queryPlan, nodeEngine)) {}

QueryCompilationRequest::Builder& QueryCompilationRequest::Builder::debug() {
    request->debug = true;
    return *this;
}

QueryCompilationRequest::Builder& QueryCompilationRequest::Builder::optimze() {
    request->debug = true;
    return *this;
}

QueryCompilationRequest::Builder& QueryCompilationRequest::Builder::dump() {
    request->dumpQueryPlans = true;
    return *this;
}

QueryCompilationRequestPtr QueryCompilationRequest::Builder::build() { return request; }

QueryCompilationRequestPtr QueryCompilationRequest::create(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine) {
    return std::make_shared<QueryCompilationRequest>(QueryCompilationRequest(queryPlan, nodeEngine));
}

QueryCompilationRequest::QueryCompilationRequest(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine)
    : queryPlan(queryPlan), nodeEngine(nodeEngine), debug(false), optimize(false), dumpQueryPlans(false) {}

bool QueryCompilationRequest::isDebugEnabled() { return debug; }

bool QueryCompilationRequest::isOptimizeEnabled() { return optimize; }

bool QueryCompilationRequest::isDumpEnabled() { return dumpQueryPlans; }

QueryPlanPtr QueryCompilationRequest::getQueryPlan() { return queryPlan; }

NodeEngine::NodeEnginePtr QueryCompilationRequest::getNodeEngine() { return nodeEngine; }

}// namespace QueryCompilation
}// namespace NES