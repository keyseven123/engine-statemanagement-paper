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

#ifndef NES_NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALRESERVOIRSAMPLEBUILDOPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALRESERVOIRSAMPLEBUILDOPERATOR_HPP_

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalSynopsisBuildOperator.hpp>


namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical operator for building a reservoir sample.
 */
class PhysicalReservoirSampleBuildOperator : public PhysicalSynopsisBuildOperator, public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:

    static PhysicalOperatorPtr create(const OperatorId id,
                                      const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const uint64_t sampleSize,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy);

    static PhysicalOperatorPtr create(const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const uint64_t sampleSize,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy);

    OperatorPtr copy() override;
    uint64_t getSampleSize() const;
    std::string toString() const override;

  private:
    PhysicalReservoirSampleBuildOperator(const OperatorId id,
                                         const StatisticId statisticId,
                                         const SchemaPtr& inputSchema,
                                         const SchemaPtr& outputSchema,
                                         const uint64_t sampleSize,
                                         const Statistic::StatisticMetricHash metricHash,
                                         const Windowing::WindowTypePtr windowType,
                                         const Statistic::SendingPolicyPtr sendingPolicy);
    const uint64_t sampleSize;
};

}// namespace NES::QueryCompilation::PhysicalOperators
#endif//NES_NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALRESERVOIRSAMPLEBUILDOPERATOR_HPP_
