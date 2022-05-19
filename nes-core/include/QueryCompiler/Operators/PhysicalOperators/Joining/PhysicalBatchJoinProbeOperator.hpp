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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_JOINING_PHYSICAL_BATCH_JOIN_PROBE_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_JOINING_PHYSICAL_BATCH_JOIN_PROBE_OPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {
/**
 * @brief Physical operator for the join sink.
 * This operator queries the operator state and computes final join results.
 */
class PhysicalBatchJoinProbeOperator : public PhysicalBatchJoinOperator, public PhysicalUnaryOperator {
  public:
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchemaProbe,
                                      const SchemaPtr& outputSchema,
                                      const Join::BatchJoinOperatorHandlerPtr& operatorHandler);
    static PhysicalOperatorPtr create(SchemaPtr inputSchemaProbe,
                                      SchemaPtr outputSchema,
                                      Join::BatchJoinOperatorHandlerPtr operatorHandler);
    PhysicalBatchJoinProbeOperator(OperatorId id,
                             SchemaPtr inputSchemaProbe,
                             SchemaPtr outputSchema,
                             Join::BatchJoinOperatorHandlerPtr operatorHandler);
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_JOINING_PHYSICAL_BATCH_JOIN_PROBE_OPERATOR_HPP_
