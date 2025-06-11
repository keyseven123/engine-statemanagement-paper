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

#ifndef NES_INCLUDE_UTIL_CHECKPOINTSTORAGETYPE_HPP_
#define NES_INCLUDE_UTIL_CHECKPOINTSTORAGETYPE_HPP_
#include <stdint.h>
#include <string>
#include <unordered_map>
namespace NES {
    enum class CheckpointStorageType : int8_t {
        NONE = 0, /// No Storage Option
        CRD = 1,  /// local storage (with rpc to coordinator)
        HDFS = 2, /// HDFS
        RDB = 3,  /// RocksDB
        INVALID = 4
    };

    std::string toString(const CheckpointStorageType checkpointStorageMode);

    CheckpointStorageType stringToCheckpointStorageTypeMap(const std::string checkpointStorageMode);
}// namespace NES

#endif// NES_INCLUDE_UTIL_CHECKPOINTSTORAGETYPE_HPP_
