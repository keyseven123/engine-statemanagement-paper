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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalSinkOperator::PhysicalSinkOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                           SinkDescriptorPtr sinkDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), sinkDescriptor(sinkDescriptor) {}

PhysicalOperatorPtr PhysicalSinkOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                                 SinkDescriptorPtr sinkDescriptor) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, sinkDescriptor);
}

PhysicalOperatorPtr PhysicalSinkOperator::create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                                 SinkDescriptorPtr sinkDescriptor) {
    return std::make_shared<PhysicalSinkOperator>(id, inputSchema, outputSchema, sinkDescriptor);
}

const std::string PhysicalSinkOperator::toString() const { return "PhysicalSinkOperator"; }

OperatorNodePtr PhysicalSinkOperator::copy() { return create(id, inputSchema, outputSchema, sinkDescriptor); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
