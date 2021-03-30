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

#ifndef NES_BASEQUERYMERGERRULE_HPP
#define NES_BASEQUERYMERGERRULE_HPP

#include <memory>

namespace NES {
class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;
}// namespace NES

namespace NES::Optimizer {
class BaseQueryMergerRule {

  public:
    /**
     * @brief apply the rule on Global Query Plan
     * @param globalQueryPlan: the global query plan
     */
    virtual bool apply(GlobalQueryPlanPtr globalQueryPlan) = 0;
};
typedef std::shared_ptr<BaseQueryMergerRule> BaseQueryMergerRulePtr;
}// namespace NES::Optimizer
#endif//NES_BASEQUERYMERGERRULE_HPP
