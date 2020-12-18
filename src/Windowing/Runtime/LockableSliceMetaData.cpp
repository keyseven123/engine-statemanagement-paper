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

#include <Windowing/Runtime/LockableSliceMetaData.hpp>
#include <mutex>

namespace NES::Windowing {

LockableSliceMetaData::LockableSliceMetaData(uint64_t startTs, uint64_t endTs) : startTs(startTs), endTs(endTs), recordsPerSlice(0){};

uint64_t LockableSliceMetaData::getEndTs() {
    std::unique_lock lock(mutex);
    return endTs;
}

uint64_t LockableSliceMetaData::getStartTs() {
    std::unique_lock lock(mutex);
    return startTs;
}

uint64_t LockableSliceMetaData::getRecordsPerSlice() {
    std::unique_lock lock(mutex);
    return recordsPerSlice;
}

void LockableSliceMetaData::incrementRecordsPerSlice() {
    std::unique_lock lock(mutex);
    recordsPerSlice++;
}

void LockableSliceMetaData::incrementRecordsPerSliceByValue(uint64_t value) {
    std::unique_lock lock(mutex);
    recordsPerSlice += value;
}

}// namespace NES::Windowing