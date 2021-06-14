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

#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_

#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

/// Check: not zero and `v` has got no 1 in common with `v - 1`.
/// Making use of short-circuit evaluation here because otherwise v-1 might be an underflow.
/// TODO: switch to std::ispow2 when we use C++2a.
template<std::size_t v>
static constexpr bool ispow2 = (!!v) && !(v & (v - 1));

namespace NES::Network {
class OutputChannel;
}

namespace NES::NodeEngine {
/**
 * @brief The TupleBuffer allows runtime components to access memory to store records in a reference-counted and
 * thread-safe manner.
 *
 * The purpose of the TupleBuffer is to zero memory allocations and enable batching.
 * In order to zero the memory allocation, a BufferManager keeps a fixed set of fixed-size buffers that it hands out to
 * components.
 * A TupleBuffer's content is automatically recycled or deleted once its reference count reaches zero.
 *
 * Prefer passing the TupleBuffer by reference whenever possible, pass the TupleBuffer to another thread by value.
 *
 * Important note: when a component is done with a TupleBuffer, it must be released. Not returning a TupleBuffer will
 * result in a runtime error that the BufferManager will raise by the termination of the NES program.
 *
 * Reminder: this class should be header-only to help inlining
 */

class [[nodiscard]] TupleBuffer {
    /// Utilize the wrapped-memory constructor
    friend class BufferManager;
    friend class FixedSizeBufferPool;
    friend class LocalBufferPool;
    friend class detail::MemorySegment;

    /// Utilize the wrapped-memory constructor and requires direct access to the control block for the ZMQ sink.
    friend class Network::OutputChannel;

    [[nodiscard]] constexpr explicit TupleBuffer(detail::BufferControlBlock * controlBlock, uint8_t * ptr, uint32_t size) noexcept
        : controlBlock(controlBlock), ptr(ptr), size(size) {}

  public:
    /// @brief Default constructor creates an empty wrapper around nullptr without controlBlock (nullptr) and size 0.
    [[nodiscard]] constexpr TupleBuffer() noexcept = default;

    /**
     * @brief Creates a TupleBuffer of length bytes starting at ptr address.
     *
     * @param ptr    resource's address.
     * @param lenght the size of the allocated memory.
     * @param parent will be notified of the buffer release. Only at that point, the ptr memory area can be freed,
     *               which is the caller's responsability.
     *
     */
    [[nodiscard]] static TupleBuffer wrapMemory(uint8_t * ptr, size_t length, BufferRecycler * parent);

    /// @brief Copy constructor: Increase the reference count associated to the control buffer.
    [[nodiscard]] constexpr TupleBuffer(TupleBuffer const& other) noexcept
        : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
        if (controlBlock) {
            controlBlock->retain();
        }
    }

    /// @brief Move constructor: Steal the resources from `other`. This does not affect the reference count.
    /// @dev In this constructor, `other` is cleared, because otherwise its destructor would release its old memory.
    [[nodiscard]] constexpr TupleBuffer(TupleBuffer && other) noexcept
        : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
        other.controlBlock = nullptr;
        other.ptr = nullptr;
        other.size = 0;
    }

    /// @brief Assign the `other` resource to this TupleBuffer; increase and decrease reference count if necessary.
    [[nodiscard]] TupleBuffer& operator=(TupleBuffer const& other) noexcept {

        if PLACEHOLDER_UNLIKELY (this == std::addressof(other)) {
            return *this;
        }

        // Override the content of this with those of `other`
        auto const oldControlBlock = std::exchange(controlBlock, other.controlBlock);
        ptr = other.ptr;
        size = other.size;

        // Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
        if (oldControlBlock != controlBlock) {
            retain();
            if (oldControlBlock) {
                oldControlBlock->release();
            }
        }
        return *this;
    }

    /// @brief Assign the `other` resource to this TupleBuffer; Might release the resource this currently points to.
    [[nodiscard]] inline TupleBuffer& operator=(TupleBuffer&& other) noexcept {

        // Especially for rvalues, the following branch should most likely never be taken if the caller writes
        // reasonable code. Therefore, this branch is considered unlikely.
        if PLACEHOLDER_UNLIKELY (this == std::addressof(other)) {
            return *this;
        }

        // Swap content of this with those of `other` to let the other's destructor take care of releasing the overwritten
        // resource.
        using std::swap;
        swap(*this, other);

        return *this;
    }

    /// @brief Delete address-of operator to make it harder to circumvent reference counting mechanism with an l-value.
    TupleBuffer* operator&() = delete;

    /// @brief Return if this is not valid.
    [[nodiscard]] constexpr auto operator!() noexcept->bool { return !isValid(); }

    /// @brief release the resource if necessary.
    inline ~TupleBuffer() noexcept { release(); }

    /// @brief Swap `lhs` and `rhs`.
    /// @dev Accessible via ADL in an unqualified call.
    inline friend void swap(TupleBuffer & lhs, TupleBuffer & rhs) noexcept {
        // Enable ADL to spell out to onlookers how swap should be used.
        using std::swap;

        swap(lhs.ptr, rhs.ptr);
        swap(lhs.size, rhs.size);
        swap(lhs.controlBlock, rhs.controlBlock);
    }

    /// @brief Increases the internal reference counter by one and return this.
    inline TupleBuffer& retain() noexcept {
        if (controlBlock) {
            controlBlock->retain();
        }
        return *this;
    }

    /// @brief Decrease internal reference counter by one and release the resource when the reference count reaches 0.
    inline void release() noexcept {
        if (controlBlock) {
            controlBlock->release();
        }
        controlBlock = nullptr;
        ptr = nullptr;
        size = 0;
    }

    /// @brief return the TupleBuffer's content as pointer to `T`.
    template<typename T = uint8_t>
    inline T* getBuffer() noexcept {
        static_assert(alignof(T) <= alignof(std::max_align_t), "Alignment of type T is stricter than allowed.");
        static_assert(ispow2<alignof(T)>);
        return reinterpret_cast<T*>(ptr);
    }

    /// @brief Print the buffer's address.
    /// @dev TODO: consider changing the reinterpret_cast to  std::bit_cast in C++2a if possible.
    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) noexcept {
        return os << reinterpret_cast<std::uintptr_t>(buff.ptr);
    }

    /// @brief true if the interal pointer is not null
    [[nodiscard]] constexpr bool isValid() const noexcept { return ptr != nullptr; }

    /// @brief get the buffer's size.
    inline uint64_t getBufferSize() const noexcept { return size; }

    /// @brief get the number of tuples stored.
    [[nodiscard]] constexpr uint64_t getNumberOfTuples() const noexcept { return controlBlock->getNumberOfTuples(); }

    /// @brief set the number of tuples stored.
    inline void setNumberOfTuples(uint64_t numberOfTuples) noexcept { controlBlock->setNumberOfTuples(numberOfTuples); }

    /// @brief get the watermark as a timestamp
    [[nodiscard]] constexpr uint64_t getWatermark() const noexcept { return controlBlock->getWatermark(); }

    /// @brief set the watermark from a timestamp
    inline void setWatermark(uint64_t value) noexcept { controlBlock->setWatermark(value); }

    /// @brief get the creation timestamp as a timestamp
    [[nodiscard]] constexpr uint64_t getCreationTimestamp() const noexcept { return controlBlock->getCreationTimestamp(); }

    /// @brief set the creation timestamp with a timestamp
    inline void setCreationTimestamp(uint64_t value) noexcept { controlBlock->setCreationTimestamp(value); }

    ///@brief get the buffer's origin id (the operator id that creates this buffer).
    [[nodiscard]] constexpr uint64_t getOriginId() const noexcept { return controlBlock->getOriginId(); }

    ///@brief set the buffer's origin id (the operator id that creates this buffer).
    inline void setOriginId(uint64_t id) noexcept { controlBlock->setOriginId(id); }

  private:
    /**
     * @brief returns the control block of the buffer USE THIS WITH CAUTION!
     */
    detail::BufferControlBlock* getControlBlock() const { return controlBlock; }

    detail::BufferControlBlock* controlBlock = nullptr;
    uint8_t* ptr = nullptr;
    uint32_t size = 0;
};

}// namespace NES::NodeEngine
#endif /* INCLUDE_TUPLEBUFFER_H_ */
