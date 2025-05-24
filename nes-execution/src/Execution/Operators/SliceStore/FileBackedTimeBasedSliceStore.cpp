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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Execution/Operators/SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/Locks.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution
{
FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(
    const uint64_t windowSize,
    const uint64_t windowSlide,
    const uint8_t numberOfInputOrigins,
    const std::filesystem::path& workingDir,
    const QueryId queryId,
    const OriginId originId)
    : memCtrl(workingDir, queryId, originId)
    , sliceAssigner(windowSize, windowSlide)
    , sequenceNumber(SequenceNumber::INITIAL)
    , numberOfInputOriginsTerminated(numberOfInputOrigins)
{
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(const FileBackedTimeBasedSliceStore& other)
    : memCtrl(other.memCtrl)
    , sliceAssigner(other.sliceAssigner)
    , sequenceNumber(other.sequenceNumber.load())
    , numberOfInputOriginsTerminated(other.numberOfInputOriginsTerminated.load())
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesReadLocked, otherWindowsReadLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = *otherSlicesReadLocked;
    *windowsWriteLocked = *otherWindowsReadLocked;
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore&& other) noexcept
    : memCtrl(std::move(other.memCtrl))
    , sliceAssigner(std::move(other.sliceAssigner))
    , sequenceNumber(std::move(other.sequenceNumber.load()))
    , numberOfInputOriginsTerminated(std::move(other.numberOfInputOriginsTerminated.load()))
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = std::move(*otherSlicesWriteLocked);
    *windowsWriteLocked = std::move(*otherWindowsWriteLocked);
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(const FileBackedTimeBasedSliceStore& other)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesReadLocked, otherWindowsReadLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = *otherSlicesReadLocked;
    *windowsWriteLocked = *otherWindowsReadLocked;

    memCtrl = other.memCtrl;
    sliceAssigner = other.sliceAssigner;
    sequenceNumber = other.sequenceNumber.load();
    numberOfInputOriginsTerminated = other.numberOfInputOriginsTerminated.load();
    return *this;
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore&& other) noexcept
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = std::move(*otherSlicesWriteLocked);
    *windowsWriteLocked = std::move(*otherWindowsWriteLocked);

    memCtrl = std::move(other.memCtrl);
    sliceAssigner = std::move(other.sliceAssigner);
    sequenceNumber = std::move(other.sequenceNumber.load());
    numberOfInputOriginsTerminated = std::move(other.numberOfInputOriginsTerminated.load());
    return *this;
}

std::vector<WindowInfo> FileBackedTimeBasedSliceStore::getAllWindowInfosForSlice(const Slice& slice) const
{
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart().getRawValue();
    const auto sliceEnd = slice.getSliceEnd().getRawValue();
    const auto windowSize = sliceAssigner.getWindowSize();
    const auto windowSlide = sliceAssigner.getWindowSlide();

    /// Taking the max out of sliceEnd and windowSize, allows us to not create windows, such as 0-5 for slide 5 and size 100.
    /// In our window model, a window is always the size of the window size.
    const auto firstWindowEnd = std::max(sliceEnd, windowSize);
    const auto lastWindowEnd = sliceStart + windowSize;

    for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide)
    {
        allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
    }

    return allWindows;
}

FileBackedTimeBasedSliceStore::~FileBackedTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);

    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);

    if (slicesWriteLocked->contains(sliceEnd))
    {
        return {slicesWriteLocked->find(sliceEnd)->second};
    }

    /// We assume that only one slice is created per timestamp
    const auto newSlices = createNewSlice(sliceStart, sliceEnd);
    INVARIANT(newSlices.size() == 1, "We assume that only one slice is created per timestamp for our default time-based slice store.");
    auto newSlice = newSlices[0];
    slicesWriteLocked->emplace(sliceEnd, newSlice);

    /// Update the state of all windows that contain this slice as we have to expect new tuples
    for (auto windowInfo : getAllWindowInfosForSlice(*newSlice))
    {
        auto& [windowSlices, windowState] = (*windowsWriteLocked)[windowInfo];
        windowState = WindowInfoState::WINDOW_FILLING;
        windowSlices.emplace_back(newSlice);
    }

    return {newSlice};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
FileBackedTimeBasedSliceStore::getTriggerableWindowSlices(const Timestamp globalWatermark)
{
    /// We are iterating over all windows and check if they can be triggered
    /// A window can be triggered if both sides have been filled and the window end is smaller than the new global watermark
    const auto windowsWriteLocked = windows.wlock();
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        if (windowInfo.windowEnd > globalWatermark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        if (windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            /// This window can not be triggered yet or has already been triggered
            continue;
        }

        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
        /// As the windows are sorted, we can simply increment the sequence number here.
        const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
    }
    return windowsToSlices;
}

std::optional<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSliceBySliceEnd(
    const SliceEnd sliceEnd,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t numberOfWorkerThreads)
{
    if (const auto slicesReadLocked = slices.rlock(); slicesReadLocked->contains(sliceEnd))
    {
        auto slice = slicesReadLocked->find(sliceEnd)->second;
        readSliceFromFiles(slice, bufferProvider, memoryLayout, joinBuildSide, numberOfWorkerThreads);
        return slice;
    }
    return {};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> FileBackedTimeBasedSliceStore::getAllNonTriggeredSlices()
{
    /// If this method gets called, we know that an origin has terminated.
    INVARIANT(numberOfInputOriginsTerminated > 0, "Method should not be called if all origin have terminated.");
    --numberOfInputOriginsTerminated;

    /// Acquiring a lock for the windows, as we have to iterate over all windows and trigger all non-triggered windows
    const auto windowsWriteLocked = windows.wlock();

    /// Creating a lambda to add all slices to the return map windowsToSlices
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    auto addAllSlicesToReturnMap = [&windowsToSlices, this](const WindowInfo& windowInfo, SlicesAndState& windowSlicesAndState)
    {
        const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
    };

    /// We are iterating over all windows and check if they can be triggered
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        switch (windowSlicesAndState.windowState)
        {
            case WindowInfoState::EMITTED_TO_PROBE:
                continue;
            case WindowInfoState::WINDOW_FILLING: {
                /// If we are waiting on more than one origin to terminate, we can not trigger the window yet
                if (numberOfInputOriginsTerminated > 0)
                {
                    windowSlicesAndState.windowState = WindowInfoState::WAITING_ON_TERMINATION;
                    NES_TRACE(
                        "Waiting on termination for window end {} and number of origins terminated {}",
                        windowInfo.windowEnd,
                        numberOfInputOriginsTerminated.load());
                    break;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
            }
            case WindowInfoState::WAITING_ON_TERMINATION: {
                /// Checking if all origins have terminated (i.e., the number of origins terminated is 0, as we will decrement it during fetch_sub)
                NES_TRACE(
                    "Checking if all origins have terminated for window with window end {} and number of origins terminated {}",
                    windowInfo.windowEnd,
                    numberOfInputOriginsTerminated.load());
                if (numberOfInputOriginsTerminated > 0)
                {
                    continue;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
            }
        }
    }

    return windowsToSlices;
}

void FileBackedTimeBasedSliceStore::garbageCollectSlicesAndWindows(const Timestamp newGlobalWaterMark)
{
    auto lockedSlicesAndWindows = tryAcquireLocked(slices, windows);
    if (not lockedSlicesAndWindows)
    {
        /// We could not acquire the lock, so we opt for not performing the garbage collection this time.
        return;
    }
    auto& [slicesWriteLocked, windowsWriteLocked] = *lockedSlicesAndWindows;

    /// 1. We iterate over all windows and set their state to CAN_BE_DELETED if they can be deleted
    /// This condition is true, if the window end is smaller than the new global watermark of the probe phase.
    for (auto windowsLockedIt = windowsWriteLocked->cbegin(); windowsLockedIt != windowsWriteLocked->cend();)
    {
        const auto& [windowInfo, windowSlicesAndState] = *windowsLockedIt;
        if (windowInfo.windowEnd <= newGlobalWaterMark and windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            // TODO delete state from ssd if there is any and if not done below
            for (const auto& slice : windowsLockedIt->second.windowSlices)
            {
                //memCtrl.deleteSliceFiles(slice->getSliceEnd());
            }
            windowsWriteLocked->erase(windowsLockedIt++);
        }
        else if (windowInfo.windowEnd > newGlobalWaterMark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        else
        {
            ++windowsLockedIt;
        }
    }

    /// 2. We gather all slices if they are not used in any window that has not been triggered/can not be deleted yet
    for (auto slicesLockedIt = slicesWriteLocked->cbegin(); slicesLockedIt != slicesWriteLocked->cend();)
    {
        const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
        if (sliceEnd + sliceAssigner.getWindowSize() <= newGlobalWaterMark)
        {
            // TODO delete state from ssd if there is any and if not done above
            memCtrl.deleteSliceFiles(sliceEnd);
            slicesWriteLocked->erase(slicesLockedIt++);
        }
        else
        {
            /// As the slices are sorted (due to std::map), we can break here as we will not find any slices with a smaller slice end
            break;
        }
    }
}

void FileBackedTimeBasedSliceStore::deleteState()
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    slicesWriteLocked->clear();
    windowsWriteLocked->clear();
    // TODO delete memCtrl and state from ssd if there is any
}

uint64_t FileBackedTimeBasedSliceStore::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}

void FileBackedTimeBasedSliceStore::updateSlices(
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t numberOfWorkerThreads,
    const SliceStoreMetaData& metaData)
{
    const auto threadId = WorkerThreadId(metaData.threadId % numberOfWorkerThreads);

    // TODO write adaptively to file
    const auto slicesLocked = slices.rlock();
    for (const auto& [sliceEnd, slice] : *slicesLocked)
    {
        auto fileWriter = memCtrl.getFileWriter(sliceEnd, threadId, metaData.pipelineId, joinBuildSide);
        slice->writeToFile(bufferProvider, memoryLayout, joinBuildSide, threadId, *fileWriter, USE_FILE_LAYOUT);
        slice->truncate(joinBuildSide, threadId, USE_FILE_LAYOUT);
        // TODO force flush FileWriter?
    }

    // TODO predictiveRead()
}

void FileBackedTimeBasedSliceStore::readSliceFromFiles(
    const std::shared_ptr<Slice>& slice,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t numberOfWorkerThreads)
{
    /// Read files in order by WorkerThreadId as all pagedVectorKeys have already been combined
    for (auto threadId = 0UL; threadId < numberOfWorkerThreads; ++threadId)
    {
        while (auto fileReader = memCtrl.getFileReader(slice->getSliceEnd(), WorkerThreadId(threadId), joinBuildSide))
        {
            slice->readFromFile(bufferProvider, memoryLayout, joinBuildSide, WorkerThreadId(threadId), *fileReader, USE_FILE_LAYOUT);
        }
    }
}

}
