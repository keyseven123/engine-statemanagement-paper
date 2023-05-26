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
#ifndef NES_NLJSINK_HPP
#define NES_NLJSINK_HPP
#include <Execution/Operators/Operator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the second and final phase for a nested loop join. We join the tuples via two nested loops and
 * calling the child with the newly created joined records.
 */
class NLJSink : public Operator {
  public:
    /**
     * @brief Constructor for a NLJSink
     * @param operatorHandlerIndex
     * @param leftSchema
     * @param rightSchema
     * @param joinSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     */
    explicit NLJSink(uint64_t operatorHandlerIndex,
                     SchemaPtr leftSchema,
                     SchemaPtr rightSchema,
                     SchemaPtr joinSchema,
                     std::string joinFieldNameLeft,
                     std::string joinFieldNameRight);

    /**
     * @brief Receives a record buffer containing a window identifier for a window that is ready to be joined
     * @param ctx
     * @param recordBuffer
     */
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    uint64_t operatorHandlerIndex;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    SchemaPtr joinSchema;

    // TODO ask Philipp, if we can get this string from the NLJOperatorHandler
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NLJSINK_HPP
