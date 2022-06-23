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

#include <Experimental/Trace/Tag.hpp>
#include <execinfo.h>
#include <iostream>
#include <map>
namespace NES::ExecutionEngine::Experimental::Trace {
Tag::Tag(std::vector<TagAddress> addresses) : addresses(std::move(addresses)) {}

bool Tag::operator==(const Tag& other) const { return other.addresses == addresses; }

std::ostream& operator<<(std::ostream& os, const Tag& tag) {
    os << "addresses: [";
    for (auto address : tag.addresses) {
        os << address << ";";
    }
    os << "]";
    return os;
}

Tag Tag::createTag(uint64_t startAddress) {
    void* buffer[20];
    // In the following we use backtrace from glibc to extract the return address pointers.
    int size = backtrace(buffer, 20);
    std::vector<TagAddress> addresses;
    for (int i = 0; i < size; i++) {
        auto address = (TagAddress) buffer[i];
        if (address == startAddress) {
            size = i;
            break;
        }
    }
    for (int i = 0; i < size - 2; i++) {
        auto address = (TagAddress) buffer[i];
        addresses.push_back(address);
    }
    return {addresses};
}

TagAddress Tag::createCurrentAddress() {
#pragma GCC diagnostic ignored "-Wframe-address"
    return (uint64_t) (__builtin_return_address(3));
}

std::size_t Tag::TagHasher::operator()(const Tag& k) const {
    using std::hash;
    using std::size_t;
    using std::string;
    auto hasher = std::hash<uint64_t>();
    std::size_t hashVal = 1;
    for (auto address : k.addresses) {
        hashVal = hashVal ^ hasher(address) << 1;
    }
    return hashVal;
}

}// namespace NES::ExecutionEngine::Experimental::Trace