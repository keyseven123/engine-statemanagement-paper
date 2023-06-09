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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

ColumnMemoryProvider::ColumnMemoryProvider(Runtime::MemoryLayouts::ColumnLayoutPtr columnMemoryLayoutPtr,
                                           const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections)
    : MemoryProvider(columnMemoryLayoutPtr, projections), columnMemoryLayoutPtr(columnMemoryLayoutPtr){};

MemoryLayouts::MemoryLayoutPtr ColumnMemoryProvider::getMemoryLayoutPtr() { return columnMemoryLayoutPtr; }

Nautilus::Value<Nautilus::MemRef> ColumnMemoryProvider::calculateFieldAddress(Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                                                                              Nautilus::Value<Nautilus::UInt64>& recordIndex,
                                                                              uint64_t fieldIndex) const {
    auto& fieldSize = columnMemoryLayoutPtr->getFieldSizes()[fieldIndex];
    auto& columnOffset = columnMemoryLayoutPtr->getColumnOffsets()[fieldIndex];
    auto fieldOffset = recordIndex * fieldSize + columnOffset;
    auto fieldAddress = bufferAddress + fieldOffset;
    return fieldAddress.as<Nautilus::MemRef>();
}

Nautilus::Record ColumnMemoryProvider::read(Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                                            Nautilus::Value<Nautilus::UInt64>& recordIndex) const {
    // read all fields
    Nautilus::Record record;
    for (auto [fieldIndex, fieldName] : fields) {
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, fieldIndex);
        auto value = load(columnMemoryLayoutPtr->getPhysicalTypes()[fieldIndex], bufferAddress, fieldAddress);
        record.write(fieldName, value);
    }
    return record;
}

void ColumnMemoryProvider::write(Nautilus::Value<NES::Nautilus::UInt64>& recordIndex,
                                 Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                                 NES::Nautilus::Record& rec) const {
    for (const auto& [fieldIndex, fieldName] : fields) {
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, fieldIndex);
        auto value = rec.read(fieldName);
        store(columnMemoryLayoutPtr->getPhysicalTypes()[fieldIndex], bufferAddress, fieldAddress, value);
    }
}

}// namespace NES::Runtime::Execution::MemoryProvider