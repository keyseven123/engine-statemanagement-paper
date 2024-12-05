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

#include <iomanip>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>
#include <nautilus/std/cstdlib.h>
#include <Runtime/WorkerContext.hpp>


namespace NES::Nautilus
{

VariableSizedData::VariableSizedData(const nautilus::val<int8_t*>& content, const nautilus::val<uint32_t>& size)
    : size(size), ptrToVarSized(content)
{
}

VariableSizedData::VariableSizedData(const nautilus::val<int8_t*>& pointerToVarSizedData)
    : VariableSizedData(pointerToVarSizedData, Util::readValueFromMemRef<uint32_t>(pointerToVarSizedData))
{
}

VariableSizedData::VariableSizedData(const VariableSizedData& other) : size(other.size), ptrToVarSized(other.ptrToVarSized)
{
}

VariableSizedData& VariableSizedData::operator=(const VariableSizedData& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = other.size;
    ptrToVarSized = other.ptrToVarSized;
    return *this;
}

VariableSizedData::VariableSizedData(VariableSizedData&& other) noexcept
    : size(std::move(other.size)), ptrToVarSized(std::move(other.ptrToVarSized))
{
}

VariableSizedData& VariableSizedData::operator=(VariableSizedData&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = std::move(other.size);
    ptrToVarSized = std::move(other.ptrToVarSized);
    return *this;
}

nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other)
{
    return varSizedData.isValid() == other;
}

nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData)
{
    return varSizedData.isValid() == other;
}

nautilus::val<bool> VariableSizedData::isValid() const
{
    PRECONDITION(size > 0 && ptrToVarSized != nullptr, "VariableSizedData has a size of 0 but  a nullptr pointer to the data.");
    PRECONDITION(size == 0 && ptrToVarSized == nullptr, "VariableSizedData has a size of 0 so there should be no pointer to the data.");
    return size > 0 && ptrToVarSized != nullptr;
}

nautilus::val<bool> VariableSizedData::operator==(const VariableSizedData& rhs) const
{
    if (size != rhs.size)
    {
        return {false};
    }

    const auto compareResult = (nautilus::memcmp(this->getContent(), rhs.getContent(), size) == 0);
    return {compareResult};
}

// TODO: Implement new operations: concat(), substr(), find() -> index
// Note: Hardcode stuff for now based on a operator => think about how to allocate space for new operators
/*VariableSizedData VariableSizedData::operator>(const VariableSizedData& other) const
{
    // Think about how to allocate new memory here for storing a new value
    // as a result of the string operation.
    auto newPtr = createNewVariableSizedData(size + other.size);
    nautilus::memcpy(newPtr, ptrToVarSized, size);
    return VariableSizedData(newPtr);
}*/

nautilus::val<bool> greaterThan(const VariableSizedData& left, const VariableSizedData& right)
{
    // Get sizes of each data value:
    nautilus::val<uint32_t> sizeLeft = left.getSize();
    nautilus::val<uint32_t> sizeRight = right.getSize();
    nautilus::val<uint32_t> minLength = sizeLeft < sizeRight ? sizeLeft : sizeRight;

    const auto compareResult = nautilus::memcmp(left.getContent(), right.getContent(), minLength);
    return compareResult > 0 || (compareResult == 0 && sizeLeft > sizeRight);
}


// Compare up to shared length, if still the same, compare lengths
nautilus::val<bool> VariableSizedData::operator>(const VariableSizedData& other) const
{
    return greaterThan(*this, other);
}

nautilus::val<bool> VariableSizedData::operator!=(const VariableSizedData& rhs) const
{
    //return !(*this == rhs);
    return greaterThan(*this, rhs);;
}

nautilus::val<bool> VariableSizedData::operator!() const
{
    return !isValid();
}

[[nodiscard]] nautilus::val<uint32_t> VariableSizedData::getSize() const
{
    return size;
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getContent() const
{
    return ptrToVarSized + nautilus::val<uint64_t>(sizeof(uint32_t));
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getReference() const
{
    return ptrToVarSized;
}

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VariableSizedData& variableSizedData)
{
    oss << "Size(" << variableSizedData.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < variableSizedData.size; ++i)
    {
        const nautilus::val<int> byte = Util::readValueFromMemRef<int8_t>((variableSizedData.getContent() + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}
}
