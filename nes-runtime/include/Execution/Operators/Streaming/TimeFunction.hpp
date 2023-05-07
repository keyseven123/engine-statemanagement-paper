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
#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_

#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief A time function, infers the timestamp of an record.
 * For ingestion time, this is determined by the creation ts in the buffer.
 * For event time, this is infered by a field in the record.
 */
class TimeFunction {
  public:
    virtual void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) = 0;
    virtual Nautilus::Value<UInt64> getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) = 0;
    virtual ~TimeFunction() = default;
};

/**
 * @brief Time function for event time windows
 */
class EventTimeFunction final : public TimeFunction {
  public:
    explicit EventTimeFunction(Expressions::ExpressionPtr timestampExpression);
    void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) override;
    Nautilus::Value<UInt64> getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) override;

  private:
    Expressions::ExpressionPtr timestampExpression;
};

/**
 * @brief Time function for ingestion time windows
 */
class IngestionTimeFunction final : public TimeFunction {
  public:
    void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) override;
    Nautilus::Value<UInt64> getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) override;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_
