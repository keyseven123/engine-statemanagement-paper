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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Phases/Z3SignatureInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

Z3SignatureInferencePhase::Z3SignatureInferencePhase(z3::ContextPtr context) : context(context) {
    NES_DEBUG("Z3SignatureInferencePhase()");
}

Z3SignatureInferencePhase::~Z3SignatureInferencePhase() { NES_DEBUG("~Z3SignatureInferencePhase()"); }

Z3SignatureInferencePhasePtr Z3SignatureInferencePhase::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureInferencePhase>(Z3SignatureInferencePhase(context));
}

void Z3SignatureInferencePhase::execute(QueryPlanPtr queryPlan) {
    NES_INFO("Z3SignatureInferencePhase: computing Z3 expressions for the query " << queryPlan->getQueryId());
    auto sinkOperators = queryPlan->getRootOperators();
    for (auto& sinkOperator : sinkOperators) {
        sinkOperator->as<LogicalOperatorNode>()->inferSignature(context);
    }
}

z3::ContextPtr Z3SignatureInferencePhase::getContext() const { return context; }

}// namespace NES::Optimizer
