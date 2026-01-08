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

#include <SliceCache/SliceCacheAlwaysMiss.hpp>

#include <cstdint>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val.hpp>

namespace NES
{
SliceCacheAlwaysMiss::SliceCacheAlwaysMiss(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef)
    : SliceCache(operatorHandler, numberOfEntries, sizeOfEntry, startOfEntries, hitsRef, missesRef)
{
}

nautilus::val<int8_t*>
SliceCacheAlwaysMiss::getDataStructureRef(const nautilus::val<Timestamp>&, const SliceCache::SliceCacheReplacement& replacementFunction)
{
    /// We never check if the slice is already in the cache, thus, we always have a cache miss.
    incrementNumberOfMisses();

    /// As we always have a cache miss, we do not care what index to replace
    const nautilus::val<SliceCacheEntry*> sliceCacheEntryToReplace = startOfEntries;
    const auto dataStructure = replacementFunction(sliceCacheEntryToReplace, 0);
    return dataStructure;
}
}
