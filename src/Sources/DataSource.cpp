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

#include <chrono>
#include <functional>
#include <iostream>
#include <thread>

#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>

#include <Sources/ZmqSource.hpp>

#include <Sources/DataSource.hpp>
#include <Util/ThreadNaming.hpp>
#include <zconf.h>
namespace NES {

std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> DataSource::getExecutableSuccessors() {
    return executableSuccessors;
}

DataSource::GatheringMode DataSource::getGatheringModeFromString(std::string mode) {
    UtilityFunctions::trim(mode);
    if (mode == "frequency") {
        return GatheringMode::FREQUENCY_MODE;
    } else if (mode == "ingestionrate") {
        return GatheringMode::INGESTION_RATE_MODE;
    } else {
        NES_THROW_RUNTIME_ERROR("mode not supported " << mode);
    }
}

DataSource::DataSource(const SchemaPtr pSchema,
                       NodeEngine::BufferManagerPtr bufferManager,
                       NodeEngine::QueryManagerPtr queryManager,
                       OperatorId operatorId,
                       OperatorId logicalSourceOperatorId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : queryManager(queryManager), globalBufferManager(bufferManager), executableSuccessors(executableSuccessors),
      operatorId(operatorId), logicalSourceOperatorId(logicalSourceOperatorId), schema(pSchema), generatedTuples(0),
      generatedBuffers(0), numBuffersToProcess(UINT64_MAX), numSourceLocalBuffers(numSourceLocalBuffers), gatheringInterval(0),
      gatheringMode(gatheringMode), NodeEngine::Reconfigurable(), wasGracefullyStopped(false), running(false), thread(nullptr),
      DataEmitter() {
    NES_DEBUG("DataSource " << operatorId << ": Init Data Source with schema");
    NES_ASSERT(this->globalBufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
}

void DataSource::emitWork(NodeEngine::TupleBuffer& buffer) {
    for (auto successor : executableSuccessors) {
        queryManager->addWorkForNextPipeline(buffer, successor);
    }
}

OperatorId DataSource::getOperatorId() { return operatorId; }

void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
    executableSuccessors.clear();
    stop(false);
    NES_DEBUG("DataSource " << operatorId << ": Destroy Data Source.");
}

OperatorId DataSource::getLogicalSourceOperatorId() const { return logicalSourceOperatorId; }

bool DataSource::start() {
    NES_DEBUG("DataSource " << operatorId << ": start source " << this);
    std::promise<bool> prom;
    std::unique_lock lock(startStopMutex);
    if (running) {
        NES_WARNING("DataSource " << operatorId << ": is already running " << this);
        return false;
    }
    running = true;
    type = getType();
    NES_DEBUG("DataSource " << operatorId << ": Spawn thread");
    thread = std::make_shared<std::thread>([this, &prom]() {
        prom.set_value(true);
        runningRoutine();
    });
    return prom.get_future().get();
}

bool DataSource::stop(bool graceful) {
    std::unique_lock lock(startStopMutex);
    NES_DEBUG("DataSource " << operatorId << ": Stop called and source is " << (running ? "running" : "not running"));
    if (!running) {
        NES_DEBUG("DataSource " << operatorId << " is not running");
        if (thread && thread->joinable()) {
            thread->join();
            thread.reset();
        }
        return false;
    }

    wasGracefullyStopped = graceful;
    // TODO add wakeUp call if source is blocking on something, e.g., tcp socket
    // TODO in general this highlights how our source model has some issues
    running = false;
    bool ret = false;

    try {
        NES_ASSERT2_FMT(!!thread, "Thread for source " << operatorId << " is not existing");
        {
            NES_DEBUG("DataSource::stop try to join threads=" << thread->get_id());
            if (thread->joinable()) {
                NES_DEBUG("DataSource::stop thread is joinable=" << thread->get_id());
                // TODO this is only a workaround and will be replaced by the network stack upate
                if (type == 0) {
                    NES_WARNING("DataSource::stop source hard cause of zmq_source");
                    auto ptr = dynamic_cast<ZmqSource*>(this);
                    ptr->disconnect();
                }

                thread->join();
                NES_DEBUG("DataSource: Thread joinded");
                ret = true;
                thread.reset();
            } else {
                NES_ERROR("DataSource " << operatorId << ": Thread is not joinable");
                wasGracefullyStopped = false;
                return false;
            }
        }
    } catch (...) {
        NES_ERROR("DataSource::stop error IN CATCH");
        auto expPtr = std::current_exception();
        wasGracefullyStopped = false;
        try {
            if (expPtr) {
                std::rethrow_exception(expPtr);
            }
        } catch (const std::exception& e) {// it would not work if you pass by value
            NES_ERROR("DataSource::stop error while stopping data source " << this << " error=" << e.what());
        }
    }
    NES_WARNING("Stopped Source " << operatorId << " = " << wasGracefullyStopped);
    return ret;
}

void DataSource::setGatheringInterval(std::chrono::milliseconds interval) { this->gatheringInterval = interval; }

void DataSource::open() { bufferManager = globalBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers); }

void DataSource::runningRoutine() {

    if (gatheringMode == GatheringMode::FREQUENCY_MODE) {
        runningRoutineWithFrequency();
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        runningRoutineWithIngestionRate();
    }
}

void DataSource::runningRoutineWithIngestionRate() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " ingestion rate=" << gatheringIngestionRate);
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }
    open();

    uint64_t nextPeriodStartTime = 0;
    uint64_t curPeriod = 0;
    uint64_t processedOverallBufferCnt = 0;
    while (running) {
        //create as many tuples as requested and then sleep
        auto startPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        uint64_t buffersProcessedCnt = 0;

        //produce buffers until limit for this second or for all perionds is reached or source is topped
        while (buffersProcessedCnt < gatheringIngestionRate && running && processedOverallBufferCnt < numBuffersToProcess) {
            auto optBuf = receiveData();

            if (optBuf.has_value()) {
                // here we got a valid bu fer
                NES_DEBUG("DataSource: add task for buffer");
                auto& buf = optBuf.value();
                buf.setOriginId(operatorId);
                buf.setCreationTimestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::high_resolution_clock::now().time_since_epoch())
                                             .count());
                emitWork(buf);

                buffersProcessedCnt++;
                processedOverallBufferCnt++;
            } else {
                NES_ERROR("DataSource: Buffer is invalid");
            }
            NES_DEBUG("DataSource: buffersProcessedCnt=" << buffersProcessedCnt
                                                         << " buffersPerSecond=" << gatheringIngestionRate);
        }

        uint64_t endPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        //next point in time when to start producing again
        nextPeriodStartTime = uint64_t(startPeriod + (1000));
        NES_DEBUG("DataSource: startTimeSendBuffers=" << startPeriod << " endTimeSendBuffers=" << endPeriod
                                                      << " nextPeriodStartTime=" << nextPeriodStartTime);

        //If this happoens then the second was not enough to create so many tuples and the ingestion rate should be decreased
        if (nextPeriodStartTime < endPeriod) {
            NES_ERROR("Creating buffer(s) for DataSource took longer than periodLength. nextPeriodStartTime="
                      << nextPeriodStartTime << " endTimeSendBuffers=" << endPeriod);
        }

        uint64_t sleepCnt = 0;
        uint64_t curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        //wait until the next period starts
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
            if (nextPeriodStartTime > curTime) {
                sleepCnt++;
                std::this_thread::sleep_for(std::chrono::milliseconds(nextPeriodStartTime - curTime));
            }
        }
        NES_DEBUG("DataSource: Done with period " << curPeriod
                                                  << " "
                                                     "and buffers="
                                                  << processedOverallBufferCnt << " sleepCnt=" << sleepCnt);
    }

    // inject reconfiguration task containing end of stream
    queryManager->addEndOfStream(shared_from_base<DataSource>(), wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

void DataSource::runningRoutineWithFrequency() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    auto ts = std::chrono::system_clock::now();
    std::chrono::milliseconds lastTimeStampMillis = std::chrono::duration_cast<std::chrono::milliseconds>(ts.time_since_epoch());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " frequency=" << gatheringInterval.count());
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }
    open();
    uint64_t cnt = 0;
    while (running) {
        bool recNow = false;
        auto tsNow = std::chrono::steady_clock::now();
        std::chrono::milliseconds nowInMillis = std::chrono::duration_cast<std::chrono::milliseconds>(tsNow.time_since_epoch());

        //this check checks if the gathering interval is zero or a ZMQ_Source, where we do not create a watermark-only buffer
        NES_DEBUG("DataSource::runningRoutine will now check src type with gatheringInterval=" << gatheringInterval.count());
        if (gatheringInterval.count() == 0 || type == ZMQ_SOURCE) {// 0 means never sleep
            NES_DEBUG("DataSource::runningRoutine will produce buffers fast enough for source type="
                      << getType() << " and gatheringInterval=" << gatheringInterval.count()
                      << "ms, tsNow=" << lastTimeStampMillis.count() << "ms, now=" << nowInMillis.count() << "ms");
            if (gatheringInterval.count() == 0 || lastTimeStampMillis != nowInMillis) {
                NES_DEBUG("DataSource::runningRoutine gathering interval reached so produce a buffer gatheringInterval="
                          << gatheringInterval.count() << "ms, tsNow=" << lastTimeStampMillis.count()
                          << "ms, now=" << nowInMillis.count() << "ms");
                recNow = true;
                lastTimeStampMillis = nowInMillis;
            } else {
                NES_DEBUG("lastTimeStampMillis=" << lastTimeStampMillis.count() << "nowInMillis=" << nowInMillis.count());
            }
        } else {
            NES_TRACE("DataSource::runningRoutine check for interval");
            // check each interval
            if (nowInMillis != lastTimeStampMillis) {//we are in another interval
                if ((nowInMillis - lastTimeStampMillis) <= gatheringInterval
                    || ((nowInMillis - lastTimeStampMillis) % gatheringInterval).count() == 0) {//produce a regular buffer
                    NES_DEBUG("DataSource::runningRoutine sending regular buffer");
                    recNow = true;
                }
                lastTimeStampMillis = nowInMillis;
            }
        }

        //repeat test
        if (!recNow) {
            std::this_thread::sleep_for(gatheringInterval);
            continue;
        }

        //check if already produced enough buffer
        if (cnt < numBuffersToProcess || numBuffersToProcess == 0) {
            auto optBuf = receiveData();

            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
                NES_DEBUG("DataSource produced buffer" << operatorId << " type=" << getType() << " string=" << toString()
                                                       << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                       << " iteration=" << cnt << " operatorId=" << this->operatorId
                                                       << " orgID=" << this->operatorId);
                buf.setOriginId(operatorId);
                buf.setCreationTimestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::high_resolution_clock::now().time_since_epoch())
                                             .count());
                emitWork(buf);
                ++cnt;
            } else {
                if (!wasGracefullyStopped) {
                    NES_ERROR("DataSource " << operatorId << ": stopping cause of invalid buffer");
                    running = false;
                }
                NES_DEBUG("DataSource " << operatorId << ": Thread terminating after graceful exit.");
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            running = false;
            wasGracefullyStopped = true;
        }
        NES_DEBUG("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);
    }
    // inject reconfiguration task containing end of stream
    queryManager->addEndOfStream(shared_from_base<DataSource>(), wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

void DataSource::reconfigure(NodeEngine::ReconfigurationMessage& message, NodeEngine::WorkerContext& context) {
    Reconfigurable::reconfigure(message, context);
}

void DataSource::postReconfigurationCallback(NodeEngine::ReconfigurationMessage& message) {
    Reconfigurable::postReconfigurationCallback(message);
    switch (message.getType()) {
        case NodeEngine::ReplaceDataEmitter: {
            executableSuccessors = message.getUserData<std::vector<NodeEngine::Execution::SuccessorExecutablePipeline>>();
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("DataSource: task type not supported");
        }
    }
}

// debugging
uint64_t DataSource::getNumberOfGeneratedTuples() { return generatedTuples; };
uint64_t DataSource::getNumberOfGeneratedBuffers() { return generatedBuffers; };

std::string DataSource::getSourceSchemaAsString() { return schema->toString(); }

uint64_t DataSource::getNumBuffersToProcess() const { return numBuffersToProcess; }

std::chrono::milliseconds DataSource::getGatheringInterval() const { return gatheringInterval; }
uint64_t DataSource::getGatheringIntervalCount() const { return gatheringInterval.count(); }

}// namespace NES
