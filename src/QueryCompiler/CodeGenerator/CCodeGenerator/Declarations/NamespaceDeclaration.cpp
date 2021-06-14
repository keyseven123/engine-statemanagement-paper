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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/NamespaceDeclaration.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>

namespace NES {
namespace QueryCompilation {
NamespaceDeclaration::NamespaceDeclaration(Code code) : namespaceCode(code) {}

NamespaceDeclarationPtr NamespaceDeclaration::create(Code code) { return std::make_shared<NamespaceDeclaration>(code); }

const GeneratableDataTypePtr NamespaceDeclaration::getType() const { return GeneratableDataTypePtr(); }
const std::string NamespaceDeclaration::getIdentifierName() const { return ""; }

const Code NamespaceDeclaration::getTypeDefinitionCode() const { return Code(); }

const Code NamespaceDeclaration::getCode() const { return namespaceCode; }
const DeclarationPtr NamespaceDeclaration::copy() const { return std::make_shared<NamespaceDeclaration>(*this); }
}// namespace QueryCompilation
}// namespace NES