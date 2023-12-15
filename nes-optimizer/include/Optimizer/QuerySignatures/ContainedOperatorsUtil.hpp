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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYSIGNATURES_CONTAINEDOPERATORSUTIL_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYSIGNATURES_CONTAINEDOPERATORSUTIL_HPP_

#include <memory>
#include <z3++.h>
namespace NES {

class LogicalOperator;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperator>;

class LogicalFilterOperator;
using FilterLogicalOperatorNodePtr = std::shared_ptr<LogicalFilterOperator>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Optimizer {

class ContainedOperatorsUtil {
  public:
    /**
     * @brief extracts the contained window operator together with its watermark operator
     * @param containedOperator operator that we identified as contained
     * @param containedWindowIndex index of the contained window operator
     * @return contained window operator and its watermark operator
     */
    static std::vector<LogicalOperatorNodePtr> createContainedWindowOperator(const LogicalOperatorNodePtr& containedOperator,
                                                                             const LogicalOperatorNodePtr& containerOperator);
    /**
     * @brief extracts the contained projection operator, i.e. extracts the most downstream projection operator from the contained upstream operator chain
     * @param containedOperator operator that we identified as contained
     * @return contained projection operator
     */
    static LogicalOperatorNodePtr createContainedProjectionOperator(const LogicalOperatorNodePtr& containedOperator);
    /**
     * @brief extracts all upstream filter operators from the contained operator chain
     * @param container the current operator from the container query
     * @param containee the current operator from the contained query
     * @return all filter upstream filter operations from the contained query
     */
    static LogicalOperatorNodePtr createContainedFilterOperators(const LogicalOperatorNodePtr& container,
                                                                 const LogicalOperatorNodePtr& containee);
    /**
     * @brief Validate if the map assignment field is used in the filter predicate of the operator
     * @param filterOperator : filter operator whose predicate need to be checked
     * @param fieldName :  name of the field to be checked
     * @return true if field use in the filter predicate else false
     */
    static bool isMapTransformationAppliedToPredicate(FilterLogicalOperatorNodePtr const& filterOperator,
                                                      const std::vector<std::string>& fieldNames,
                                                      const SchemaPtr& containerOutputSchema);
    /**
     * @brief checks if we can safely extract the contained operator chain from the container operator chain, i.e.
     * if the container chain has multiple parent relationships, we end up with wrong query results if we extract the contained chain
     * therefore this method returns true, if the container chain has only one parent relationship, false otherwise
     * @param containedOperator the operator for which we identified a containment relationship
     * @param extractedContainedOperator the most upstream operator from the extracted contained operator chain
     * @return true, if the container chain has only one parent relationship, false otherwise
     */
    static bool checkDownstreamOperatorChainForSingleParent(const LogicalOperatorNodePtr& containedOperator,
                                                            const LogicalOperatorNodePtr& extractedContainedOperator);
    /**
     * @brief checks if we can safely extract the contained operator chain from the container operator chain, i.e.
     * if the container chain has multiple parent relationships, we end up with wrong query results if we extract the contained chain
     * additionally, we cannot extract filter operations if a map transformation was applied to the filter's predicate
     * therefore this method returns true, if the container chain has only one parent relationship and no map function transforms
     * any upstream attribute with a filter predicate, false otherwise
     * @param containedOperator the operator for which we identified a containment relationship
     * @param extractedContainedOperator the most upstream operator from the extracted contained operator chain
     * @return true, if the container chain has only one parent relationship and no map function transforms any of the filter's
     * predicates, false otherwise
     */
    static bool
    checkDownstreamOperatorChainForSingleParentAndMapOperator(const LogicalOperatorNodePtr& containedOperator,
                                                              const LogicalOperatorNodePtr& extractedContainedOperator,
                                                              std::vector<std::string>& mapAttributeNames);
};

}// namespace Optimizer
}// namespace NES

#endif  // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYSIGNATURES_CONTAINEDOPERATORSUTIL_HPP_
