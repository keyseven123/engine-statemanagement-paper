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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_HASHTABLE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_HASHTABLE_HPP_

#include <atomic>

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/FixedPage.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map that is not thread safe. It consists of multiple buckets each
 * consisting of a FixedPagesLinkedList.
 */
class HashTable {

  public:
    /**
     * @brief Constructor for a HashTable that
     * @param sizeOfRecord
     * @param numPartitions
     * @param fixedPagesAllocator
     * @param pageSize
     * @param preAllocPageSizeCnt
     */
    explicit HashTable(size_t sizeOfRecord,
                       size_t numPartitions,
                       FixedPagesAllocator& fixedPagesAllocator,
                       size_t pageSize,
                       size_t preAllocPageSizeCnt);

    HashTable(const HashTable&) = delete;

    HashTable& operator=(const HashTable&) = delete;

    virtual ~HashTable() = default;

    /**
     * @brief Inserts the key into this hash table by returning a pointer to a free memory space
     * @param key
     * @return Pointer to free memory space where the data shall be written
     */
    virtual uint8_t* insert(uint64_t key) const = 0;

    /**
     * @brief Returns the bucket at bucketPos
     * @param bucketPos
     * @return bucket
     */
    FixedPagesLinkedList* getBucketLinkedList(size_t bucketPos);

    /**
     * @brief Calculates the bucket position for the hash
     * @param hash
     * @return bucket position
     */
    size_t getBucketPos(uint64_t hash) const;

    /**
     * @brief debug mehtod to print the statistics of the hash table
     * @return
     */
    std::string getStatistics();

    /**
     * @brief get number of tuples in hash table
     * @return
     */
    uint64_t getNumberOfTuples();

  protected:
    std::vector<std::unique_ptr<FixedPagesLinkedList>> buckets;
    size_t mask;
    size_t numPartitions;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_HashTable_HPP_
