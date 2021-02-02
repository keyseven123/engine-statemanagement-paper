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

#ifndef NES_SIGNATUREINFERENCEPHASE_HPP
#define NES_SIGNATUREINFERENCEPHASE_HPP

#include <memory>

namespace z3 {
class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {
class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;
}// namespace NES

namespace NES::Optimizer {

class SignatureInferencePhase;
typedef std::shared_ptr<SignatureInferencePhase> Z3ExpressionInferencePhasePtr;

/**
 * @brief This class is responsible for computing the Z3 expression for all operators within a query
 */
class SignatureInferencePhase {

  public:
    /**
     * @brief Create instance of Z3ExpressionInferencePhase class
     * @return shared instance of the Z3ExpressionInferencePhase
     */
    static Z3ExpressionInferencePhasePtr create(z3::ContextPtr context);

    /**
     * @brief this method will compute the Z3 expression for all operators of the input query plan
     * @param queryPlan: the input query plan
     */
    void execute(QueryPlanPtr queryPlan);

    /**
     * @brief Get shared instance of z3 context
     * @return context
     */
    z3::ContextPtr getContext() const;

    ~SignatureInferencePhase();

  private:
    explicit SignatureInferencePhase(z3::ContextPtr context);
    z3::ContextPtr context;
};
}// namespace NES::Optimizer

#endif//NES_SIGNATUREINFERENCEPHASE_HPP
