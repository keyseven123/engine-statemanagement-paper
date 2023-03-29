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

#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

SignatureContainmentUtilPtr SignatureContainmentUtil::create(const z3::ContextPtr& context) {
    return std::make_shared<SignatureContainmentUtil>(context);
}

SignatureContainmentUtil::SignatureContainmentUtil(const z3::ContextPtr& context) : counter(0) {
    this->context = context;
    this->solver = std::make_unique<z3::solver>(*this->context);
}

ContainmentType SignatureContainmentUtil::checkContainment(const QuerySignaturePtr& leftSignature,
                                                           const QuerySignaturePtr& rightSignature) {
    NES_TRACE("SignatureContainmentUtil::checkContainment: Checking for containment.");
    try {
        auto otherConditions = rightSignature->getConditions();
        auto conditions = leftSignature->getConditions();
        if (!conditions || !otherConditions) {
            NES_WARNING("SignatureContainmentUtil::checkContainment: Can't compare equality between null signatures");
            return NO_CONTAINMENT;
        }
        z3::expr_vector leftQueryProjectionConditions(*context);
        z3::expr_vector rightQueryProjectionConditions(*context);
        //create conditions for projection containment
        createProjectionCondition(leftSignature, leftQueryProjectionConditions);
        createProjectionCondition(rightSignature, rightQueryProjectionConditions);
        NES_TRACE("SignatureContainmentUtil::checkContainment: Add map conditions and check.");
        NES_TRACE("SignatureContainmentUtil::checkContainment: content of left sig expression vectors: "
                  << leftQueryProjectionConditions.to_string());
        NES_TRACE("SignatureContainmentUtil::checkContainment: content of right sig expression vectors: "
                  << rightQueryProjectionConditions.to_string());
        //create First Order Logic (FOL) for projection containment to check if right sig contained by left sig
        //The rest of this method checks for containment as follows:
        //  check if right sig ⊆ left sig for projections (including map conditions), i.e. if ((!cond2 && cond1) == unsat) <=> right sig ⊆ left sig,
        // since we're checking for projection containment, the negation is on the side of the contained condition,
        // e.g. right sig ⊆ left sig <=> ((!(attr1==true && attr2==45) && (attr1==true && attr2==45, attr3==true)) == unsat)
        //      true: check left sig ⊆ right sig for projections (including map conditions) --> if true, we have equal projections,
        //      i.e. can only check other containment relationships if sources, projections, and maps are equal
        //          true: check filter containment --> see checkFilterContainment() for details
        //          false: return SIG_TWO_CONTAINED
        //      false: check left sig ⊆ right sig for projections (including map conditions)
        //          true: return SIG_ONE_CONTAINED
        //          false: return NO_CONTAINMENT
        //check if right sig contained by left sig for projection
        if (conditionsUnsatisfied(rightQueryProjectionConditions, leftQueryProjectionConditions)) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Sig1 contains Sig2.");
            //check for equality in projection, we can only check other containment relationships if sources, projections and maps are equal
            if (conditionsUnsatisfied(leftQueryProjectionConditions, rightQueryProjectionConditions)) {
                NES_TRACE("SignatureContainmentUtil::checkContainment: Equal sources, equal projection.");
                //todo:  #3494 add window containment
                //check for filter containment
                return checkFilterContainment(leftSignature, rightSignature);
            }
            return RIGHT_SIG_CONTAINED;
        } else {
            //check if new query contained by SQP for projection containment identification
            //since we require equal sources to check for further containment relationships, we cannot check for filter nor window containment here
            if (conditionsUnsatisfied(leftQueryProjectionConditions, rightQueryProjectionConditions)) {
                NES_TRACE("SignatureContainmentUtil::checkContainment: Sig2 contains Sig1.");
                return LEFT_SIG_CONTAINED;
            }
        }
    } catch (...) {
        auto exception = std::current_exception();
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            NES_ERROR("SignatureContainmentUtil: Exception occurred while performing containment check among "
                      "queryIdAndCatalogEntryMapping "
                      << e.what());
        }
    }
    return NO_CONTAINMENT;
}

ContainmentType SignatureContainmentUtil::checkFilterContainment(const QuerySignaturePtr& leftSignature,
                                                                 const QuerySignaturePtr& rightSignature) {
    NES_TRACE("SignatureContainmentUtil::checkContainment: Create new condition vectors.");
    z3::expr_vector leftQueryFilterConditions(*context);
    z3::expr_vector rightQueryFilterConditions(*context);
    NES_TRACE("SignatureContainmentUtil::checkContainment: Add filter conditions.");
    leftQueryFilterConditions.push_back(to_expr(*context, *leftSignature->getConditions()));
    rightQueryFilterConditions.push_back(to_expr(*context, *rightSignature->getConditions()));
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of left sig expression vectors: "
              << leftQueryFilterConditions.to_string());
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of right sig expression vectors: "
              << rightQueryFilterConditions.to_string());
    //The rest of the method checks for filter containment as follows:
    //check if right sig ⊆ left sig for filters, i.e. if ((right cond && !left condition) == unsat) <=> right sig ⊆ left sig,
    //since we're checking for projection containment, the negation is on the side of the contained condition,
    //e.g. right sig ⊆ left sig <=> (((attr<5 && attr2==6) && !(attr1<=10 && attr2==45)) == unsat)
    //      true: check if left sig ⊆ right sig
    //          true: return EQUALITY
    //          false: return LEFT_SIG_CONTAINED
    //      false: check if left sig ⊆ right sig
    //          true: return RIGHT_SIG_CONTAINED
    //          false: return NO_CONTAINMENT
    if (conditionsUnsatisfied(leftQueryFilterConditions, rightQueryFilterConditions)) {
        if (conditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Equal filters.");
            return EQUALITY;
        }
        NES_TRACE("SignatureContainmentUtil::checkContainment: left sig contains right sig for filters.");
        return RIGHT_SIG_CONTAINED;
    } else {
        if (conditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: right sig contains left sig for filters.");
            return LEFT_SIG_CONTAINED;
        }
    }
    return NO_CONTAINMENT;
}

void SignatureContainmentUtil::createProjectionCondition(const QuerySignaturePtr& signature,
                                                         z3::expr_vector& projectionCondition) {
    //todo: #3495 think about order for projection
    //check projection containment
    // if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
    // else we indicate that the attribute is involved in the projection as attributeStingName == true
    // all FOL are added to the projectionCondition vector
    for (auto schemaFieldToExpressionMap : signature->getSchemaFieldToExprMaps()) {
        for (auto [attributeName, z3Expression] : schemaFieldToExpressionMap) {
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: strings: " << attributeName);
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: z3 expressions: " << z3Expression->to_string());
            z3::ExprPtr expr = std::make_shared<z3::expr>(context->bool_const((attributeName.c_str())));
            if (z3Expression->to_string() != attributeName) {
                if (z3Expression->is_int()) {
                    expr = std::make_shared<z3::expr>(context->int_const(attributeName.c_str()));
                } else if (z3Expression->is_fpa()) {
                    expr = std::make_shared<z3::expr>(context->fpa_const<64>(attributeName.c_str()));
                } else if (z3Expression->is_string_value()) {
                    expr = std::make_shared<z3::expr>(context->string_const(attributeName.c_str()));
                }
                projectionCondition.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *z3Expression)));
            } else {
                z3::ExprPtr columnIsUsed = std::make_shared<z3::expr>(context->bool_val(true));
                projectionCondition.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *columnIsUsed)));
            }
        }
    }
}

bool SignatureContainmentUtil::conditionsUnsatisfied(const z3::expr_vector& negatedCondition, const z3::expr_vector& condition) {
    solver->push();
    solver->add(!z3::mk_and(negatedCondition).simplify());
    solver->push();
    solver->add(z3::mk_and(condition).simplify());
    NES_TRACE("SignatureContainmentUtil::conditionsUnsatisfied: Check unsat: " << solver->check());
    bool conditionUnsatisfied = false;
    if (solver->check() == z3::unsat) {
        conditionUnsatisfied = true;
    }
    solver->pop(NUMBER_OF_CONDITIONS_TO_POP_FROM_SOLVER);
    counter++;
    if (counter >= RESET_SOLVER_THRESHOLD) {
        resetSolver();
    }
    return conditionUnsatisfied;
}

void SignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
}

}// namespace NES::Optimizer
