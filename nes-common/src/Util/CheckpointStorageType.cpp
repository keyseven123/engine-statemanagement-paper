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

#include <Util/CheckpointStorageType.hpp>
#include <string>

namespace NES {
    CheckpointStorageType stringToCheckpointStorageTypeMap(const std::string checkpointStorageMode) {
        if (checkpointStorageMode == "NONE") {
            return CheckpointStorageType::NONE;
        } else if (checkpointStorageMode == "CRD") {
            return CheckpointStorageType::CRD;
        } else if (checkpointStorageMode == "HDFS") {
            return CheckpointStorageType::HDFS;
        } else if (checkpointStorageMode == "RDB") {
            return CheckpointStorageType::RDB;
        } else {
            return CheckpointStorageType::INVALID;
        }
    }

    std::string toString(const CheckpointStorageType checkpointStorageMode) {
        switch (checkpointStorageMode) {
            case CheckpointStorageType::NONE: return "NONE";
            case CheckpointStorageType::CRD: return "CRD";
            case CheckpointStorageType::HDFS: return "HDFS";
            case CheckpointStorageType::RDB: return "RDB";
            case CheckpointStorageType::INVALID: return "INVALID";
        }
    }
}// namespace NES
