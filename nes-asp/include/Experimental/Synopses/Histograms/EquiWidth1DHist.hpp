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

#ifndef NES_EQUIWIDTH1DHIST_HPP
#define NES_EQUIWIDTH1DHIST_HPP

#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>

namespace NES::ASP {

/**
 * @brief Implementation of a 1-D equi-width histogram
 */
class EquiWidth1DHist : public AbstractSynopsis {
public:
    /**
     * @brief This class acts as a simple storage container for the local state, which contains the memref to the bins
     */
    class LocalBinsOperatorState : public Runtime::Execution::Operators::OperatorState {
      public:
        explicit LocalBinsOperatorState(const Nautilus::Interface::Fixed2DArrayRef& binsRef) : bins(binsRef) {}

        Nautilus::Interface::Fixed2DArrayRef bins;
    };

    /**
     * @brief Constructor for an equi-width-1d histogram
     * @param aggregationConfig
     * @param entrySize
     * @param minValue
     * @param maxValue
     * @param numberOfBins
     * @param lowerBinBoundString
     * @param upperBinBoundString
     * @param readBinDimension
     */
    EquiWidth1DHist(Parsing::SynopsisAggregationConfig& aggregationConfig, const uint64_t entrySize,
                    const int64_t minValue, const int64_t maxValue, const uint64_t numberOfBins,
                    const std::string& lowerBinBoundString, const std::string& upperBinBoundString,
                    std::unique_ptr<Runtime::Execution::Expressions::ReadFieldExpression> readBinDimension);

    /**
     * @brief Adds the record to the histogram, by first calculating the position and then calling the aggregation function
     * @param handlerIndex
     * @param ctx
     * @param record
     * @param pState
     */
    void addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record,
                       Runtime::Execution::Operators::OperatorState *pState) override;

    /**
     * @brief Performs the lower operation on all bins and writes each bin into a record
     * @param handlerIndex
     * @param ctx
     * @param bufferManager
     * @return Vector of TupleBuffers, which contain the records of the bins
     */
    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                                     Runtime::BufferManagerPtr bufferManager) override;

    /**
     * @brief Sets the histogram up, by calling the setup method of the operator handler, as well as resetting the
     * values in the bins with the aggregation function
     * @param handlerIndex
     * @param ctx
     */
    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) override;

    /**
     * @brief Allows the histogram to store something in the state of the synopsis operator, for example a memref to bins
     * @param handlerIndex
     * @param op
     * @param ctx
     * @param buffer
     */
    void storeLocalOperatorState(uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
                                 Runtime::Execution::ExecutionContext &ctx,
                                 Runtime::Execution::RecordBuffer buffer) override;

private:
    const int64_t minValue;
    const int64_t maxValue;
    const uint64_t numberOfBins;
    const uint64_t binWidth;
    const uint64_t entrySize;

    const std::string lowerBinBoundString;
    const std::string upperBinBoundString;
    std::unique_ptr<Runtime::Execution::Expressions::ReadFieldExpression> readBinDimension;
};
} // namespace NES::ASP

#endif //NES_EQUIWIDTH1DHIST_HPP
