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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/ClassDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/GeneratableTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/UserDefinedDataType.hpp>
#include <iostream>
#include <sstream>
#include <string>

namespace NES {
namespace QueryCompilation {
ClassDeclaration::ClassDeclaration(ClassDefinitionPtr classDefinition) : classDefinition(classDefinition) {}

ClassDeclarationPtr ClassDeclaration::create(ClassDefinitionPtr classDefinition) {
    return std::make_shared<ClassDeclaration>(classDefinition);
}

const GeneratableDataTypePtr ClassDeclaration::getType() const {
    return GeneratableTypesFactory().createAnonymusDataType(classDefinition->name);
}
const std::string ClassDeclaration::getIdentifierName() const { return ""; }

const Code ClassDeclaration::getTypeDefinitionCode() const { return Code(); }

const Code ClassDeclaration::getCode() const {
    std::stringstream classCode;
    classCode << "class " << classDefinition->name;
    classCode << generateBaseClassNames();
    classCode << "{";

    if (!classDefinition->publicConstructors.empty()) {
        classCode << "public:";
        classCode << generateConstructors(classDefinition->publicConstructors);
    }

    if (!classDefinition->publicFunctions.empty()) {
        classCode << "public:";
        classCode << generateFunctions(classDefinition->publicFunctions);
    }

    if (!classDefinition->privateFunctions.empty()) {
        classCode << "private:";
        classCode << generateFunctions(classDefinition->privateFunctions);
    }

    classCode << "};";
    return classCode.str();
}

std::string ClassDeclaration::generateFunctions(std::vector<FunctionDefinitionPtr>& functions) const {
    std::stringstream classCode;
    for (const auto& function : functions) {
        auto functionDeclaration = function->getDeclaration();
        classCode << functionDeclaration->getCode();
    }
    return classCode.str();
}

std::string ClassDeclaration::generateConstructors(std::vector<ConstructorDefinitionPtr>& ctors) const {
    std::stringstream classCode;
    for (const auto& ctor : ctors) {
        auto functionDeclaration = ctor->getDeclaration();
        classCode << functionDeclaration->getCode();
    }
    return classCode.str();
}

std::string ClassDeclaration::generateBaseClassNames() const {
    std::stringstream classCode;
    auto baseClasses = classDefinition->baseClasses;
    if (!baseClasses.empty()) {
        classCode << ":";
        for (int i = 0; i < baseClasses.size() - 1; i++) {
            classCode << " public " << baseClasses[i] << ",";
        }
        classCode << " public " << baseClasses[baseClasses.size() - 1];
    }
    return classCode.str();
}

const DeclarationPtr ClassDeclaration::copy() const { return std::make_shared<ClassDeclaration>(*this); }
}// namespace QueryCompilation
}// namespace NES