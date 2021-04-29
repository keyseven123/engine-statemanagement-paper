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
typedef std::shared_ptr<SignatureInferencePhase> SignatureInferencePhasePtr;

/**
 * @brief This class is responsible for computing the Z3 expression for all operators within a query
 */
class SignatureInferencePhase {

  public:
    /**
     * @brief Create instance of SignatureInferencePhase class
     * @param context : the z3 context
     * @param computeStringSignature : bool flag indicating that string based signature need to be computed
     * @return pointer to the signature inference phase
     */
    static SignatureInferencePhasePtr create(z3::ContextPtr context, bool computeStringSignature = false);

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
    /**
     * @brief Create instance of SignatureInferencePhase class
     * @param context : the z3 context
     * @param computeStringSignature : bool flag indicating that string based signature need to be computed
     */
    explicit SignatureInferencePhase(z3::ContextPtr context, bool computeStringSignature = false);
    z3::ContextPtr context;
    bool computeStringSignature;
};
}// namespace NES::Optimizer

#endif//NES_SIGNATUREINFERENCEPHASE_HPP
