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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalDemultiplexOperator::create(OperatorId id, SchemaPtr inputSchema) {
    return std::make_shared<PhysicalDemultiplexOperator>(id, inputSchema);
}
PhysicalOperatorPtr PhysicalDemultiplexOperator::create(SchemaPtr inputSchema) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema);
}

PhysicalDemultiplexOperator::PhysicalDemultiplexOperator(OperatorId id, SchemaPtr inputSchema)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, inputSchema) {}

const std::string PhysicalDemultiplexOperator::toString() const { return "PhysicalDemultiplexOperator"; }
OperatorNodePtr PhysicalDemultiplexOperator::copy() { return create(id, inputSchema); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES