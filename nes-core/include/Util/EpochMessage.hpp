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

namespace NES {

class EpochMessage {

  public:
    EpochMessage(uint64_t timestamp, uint64_t replicationLevel);

    uint64_t getTimestamp() const;

    uint64_t getReplicationLevel() const;

  private:
    uint64_t timestamp;
    uint64_t replicationLevel;
};
}// namespace NES
