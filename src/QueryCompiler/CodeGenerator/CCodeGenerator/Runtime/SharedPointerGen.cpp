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
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Runtime/SharedPointerGen.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
namespace NES {
namespace QueryCompilation {
GeneratableDataTypePtr SharedPointerGen::createSharedPtrType(GeneratableDataTypePtr type) {
    return GeneratableTypesFactory().createAnonymusDataType("std::shared_ptr<" + type->getCode()->code_ + ">");
}

StatementPtr SharedPointerGen::makeShared(GeneratableDataTypePtr type) {
    return FunctionCallStatement::create("std::make_shared<" + type->getTypeDefinitionCode()->code_ + ">");
}
}// namespace QueryCompilation
}// namespace NES