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

#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>

namespace NES {

const StatementPtr ExpressionStatment::createCopy() const { return this->copy(); }

ExpressionStatment::~ExpressionStatment() {}

BinaryOperatorStatement ExpressionStatment::operator[](const ExpressionStatment& ref) {
    return BinaryOperatorStatement(*this, ARRAY_REFERENCE_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::accessPtr(const ExpressionStatment& ref) {
    return BinaryOperatorStatement(*this, MEMBER_SELECT_POINTER_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::accessPtr(const ExpressionStatmentPtr ref) {
    return BinaryOperatorStatement(this->copy(), MEMBER_SELECT_POINTER_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::accessRef(const ExpressionStatment& ref) {
    return BinaryOperatorStatement(*this, MEMBER_SELECT_REFERENCE_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::accessRef(ExpressionStatmentPtr ref) {
    return BinaryOperatorStatement(this->copy(), MEMBER_SELECT_REFERENCE_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::assign(const ExpressionStatment& ref) {
    return BinaryOperatorStatement(*this, ASSIGNMENT_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::assign(ExpressionStatmentPtr ref) {
    return BinaryOperatorStatement(this->copy(), ASSIGNMENT_OP, ref);
}

}// namespace NES
