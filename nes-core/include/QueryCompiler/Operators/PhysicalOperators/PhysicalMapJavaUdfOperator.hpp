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

#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPJAVAUDFOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPJAVAUDFOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace Catalogs::UDF {
class JavaUdfDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUdfDescriptor>;

}// namespace Catalogs::UDF
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Map Java Udf operator.
 */
class PhysicalMapJavaUdfOperator : public PhysicalUnaryOperator {
  public:
    PhysicalMapJavaUdfOperator(OperatorId id,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema,
                               Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor);
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Catalogs::UDF::JavaUdfDescriptorPtr& javaUdfDescriptor);
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor);
    std::string toString() const override;
    OperatorNodePtr copy() override;

    /**
     * @brief Returns the java udf descriptor of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    Catalogs::UDF::JavaUdfDescriptorPtr getJavaUdfDescriptor();

  protected:
    const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPJAVAUDFOPERATOR_HPP_