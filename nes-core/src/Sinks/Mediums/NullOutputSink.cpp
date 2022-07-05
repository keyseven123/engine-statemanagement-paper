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

#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

namespace NES {
NullOutputSink::NullOutputSink(Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(nullptr, std::move(nodeEngine), numOfProducers, queryId, querySubPlanId, faultToleranceType, numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)) {
    if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE) {
        updateWatermarkCallback = [this](Runtime::TupleBuffer& inputBuffer) {
            updateWatermark(inputBuffer);
        };
    }
    else {
        updateWatermarkCallback = [](Runtime::TupleBuffer&) {};
    }
    statisticsFile.open("latency.csv", std::ios::out);
    statisticsFile << "latency\n";
}

NullOutputSink::~NullOutputSink() = default;

SinkMediumTypes NullOutputSink::getSinkMediumType() { return NULL_SINK; }

bool NullOutputSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    updateWatermarkCallback(inputBuffer);
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_ms.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    statisticsFile << value.count() - inputBuffer.getWatermark() << "\n";
    statisticsFile.flush();
    return true;
}

std::string NullOutputSink::toString() const {
    std::stringstream ss;
    ss << "NULL_SINK(";
    ss << ")";
    return ss.str();
}

void NullOutputSink::setup() {
    // currently not required
}
void NullOutputSink::shutdown() {
    // currently not required
}

}// namespace NES
