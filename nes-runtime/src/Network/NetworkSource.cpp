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

#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSource.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Network {

NetworkSource::NetworkSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager,
                             NesPartition nesPartition,
                             NodeLocation sinkLocation,
                             size_t numSourceLocalBuffers,
                             std::chrono::milliseconds waitTime,
                             uint8_t retryTimes,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                             DecomposedQueryPlanVersion version,
                             uint64_t uniqueNetworkSourceIdentifier,
                             const std::string& physicalSourceName)

    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 nesPartition.getOperatorId(),
                 /*invalid origin id for the network source, as the network source does not change the origin id*/
                 INVALID_ORIGIN_ID,
                 /*invalid statistic id for the network source, as the network source does not change the statistic id*/
                 INVALID_STATISTIC_ID,
                 numSourceLocalBuffers,
                 GatheringMode::INTERVAL_MODE,
                 physicalSourceName,
                 std::move(successors)),
      networkManager(std::move(networkManager)), nesPartition(nesPartition), sinkLocation(std::move(sinkLocation)),
      waitTime(waitTime), retryTimes(retryTimes), version(version), uniqueNetworkSourceIdentifier(uniqueNetworkSourceIdentifier) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

std::optional<Runtime::TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const { return SourceType::NETWORK_SOURCE; }

std::string NetworkSource::toString() const { return "NetworkSource: " + nesPartition.toString(); }

// this is necessary to use std::visit below (see example: https://en.cppreference.com/w/cpp/utility/variant/visit)
namespace detail {
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
}// namespace detail

bool NetworkSource::bind() {
    auto emitter = shared_from_base<DataEmitter>();
    return networkManager->registerSubpartitionConsumer(nesPartition, sinkLocation, emitter);
}

bool NetworkSource::start() {
    using namespace Runtime;
    NES_DEBUG("NetworkSource: start called on {}", nesPartition);
    auto emitter = shared_from_base<DataEmitter>();
    auto expected = false;
    if (running.compare_exchange_strong(expected, true)) {
        for (const auto& successor : executableSuccessors) {
            auto decomposedQueryPlanId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                                           return sink->getParentPlanId();
                                                                       },
                                                                       [](Execution::ExecutablePipelinePtr pipeline) {
                                                                           return pipeline->getDecomposedQueryPlanId();
                                                                       }},
                                                    successor);
            auto sharedQueryId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                                   return sink->getSharedQueryId();
                                                               },
                                                               [](Execution::ExecutablePipelinePtr pipeline) {
                                                                   return pipeline->getSharedQueryId();
                                                               }},
                                            successor);

            auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                    decomposedQueryPlanId,
                                                    Runtime::ReconfigurationType::Initialize,
                                                    shared_from_base<DataSource>());
            NES_DEBUG("inserting reconfig message to start source")
            queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryPlanId, newReconf, true);
            NES_DEBUG("completed blockign reconfig to start source ")
            break;// hack as currently we assume only one executableSuccessor
        }
        NES_DEBUG("NetworkSource: start completed on {}", nesPartition);
        return true;
    }
    return false;
}

bool NetworkSource::fail() {
    using namespace Runtime;
    bool expected = true;
    if (running.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NetworkSource: fail called on {}", nesPartition);
        auto newReconf =
            ReconfigurationMessage(-1, -1, ReconfigurationType::FailEndOfStream, DataSource::shared_from_base<DataSource>());
        queryManager->addReconfigurationMessage(-1, -1, newReconf, false);
        queryManager->notifySourceCompletion(shared_from_base<DataSource>(), Runtime::QueryTerminationType::Failure);
        return queryManager->addEndOfStream(shared_from_base<NetworkSource>(), Runtime::QueryTerminationType::Failure);
    }
    return false;
}

bool NetworkSource::stop(Runtime::QueryTerminationType type) {
    using namespace Runtime;
    bool expected = true;
    NES_ASSERT2_FMT(type == QueryTerminationType::HardStop,
                    "NetworkSource::stop only supports HardStop or Failure :: partition " << nesPartition);
    if (running.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NetworkSource: stop called on {}", nesPartition);
        int invalidId = -1;
        auto newReconf = ReconfigurationMessage(invalidId,
                                                invalidId,
                                                ReconfigurationType::HardEndOfStream,
                                                DataSource::shared_from_base<DataSource>());
        queryManager->addReconfigurationMessage(invalidId, invalidId, newReconf, false);
        queryManager->notifySourceCompletion(shared_from_base<DataSource>(), Runtime::QueryTerminationType::HardStop);
        queryManager->addEndOfStream(shared_from_base<DataSource>(), Runtime::QueryTerminationType::HardStop);
        NES_DEBUG("NetworkSource: stop called on {} sent hard eos", nesPartition);
    } else {
        NES_DEBUG("NetworkSource: stop called on {} but was already stopped", nesPartition);
    }
    return true;
}

void NetworkSource::onEvent(Runtime::BaseEvent&) { NES_DEBUG("NetworkSource: received an event"); }

void NetworkSource::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSource: reconfigure() called {} for the type {}",
              nesPartition.toString(),
              magic_enum::enum_name(task.getType()));
    NES::DataSource::reconfigure(task, workerContext);
    bool isTermination = false;
    auto terminationType = Runtime::QueryTerminationType::Failure;

    switch (task.getType()) {
        case Runtime::ReconfigurationType::UpdateVersion: {
            //NES_NOT_IMPLEMENTED();
//            if (!networkManager->getConnectSourceEventChannelsAsync()) {
//                NES_THROW_RUNTIME_ERROR(
//                    "Attempt to reconfigure a network source but asynchronous connecting of event channels is not "
//                    "activated. To use source reconfiguration allow asynchronous connecting in the the configuration");
//            }
            workerContext.releaseEventOnlyChannel(uniqueNetworkSourceIdentifier, terminationType);
            NES_DEBUG("NetworkSource: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
            break;
        }
        case Runtime::ReconfigurationType::Initialize: {
            NES_INFO("Initializing network source with unique id {} and partition {}",
                     uniqueNetworkSourceIdentifier,
                     nesPartition);
            // we need to check again because between the invocations of
            // NetworkSource::start() and NetworkSource::reconfigure() the query might have
            // been stopped for some reason
            if (networkManager->isPartitionConsumerRegistered(nesPartition) == PartitionRegistrationStatus::Deleted) {
                NES_WARNING(
                    "NetworkManager shows the partition {} to be deleted, but now we should init it here, so we simply return!",
                    nesPartition.toString());
                //todo: this will need to become allowed when ack protocol exists
                NES_THROW_RUNTIME_ERROR("Trying to reuse deleted partition");
                return;
            }

            if (workerContext.doesEventChannelExist(uniqueNetworkSourceIdentifier)) {
                NES_DEBUG("NetworkSource: reconfigure() channel already exists on {} Thread {}, unique id {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId(),
                          uniqueNetworkSourceIdentifier);
                NES_THROW_RUNTIME_ERROR("An channel has already been created for the source with the unique id");
                return;
            }

            //            if (networkManager->getConnectSourceEventChannelsAsync()) {
            //                auto channelFuture = networkManager->registerSubpartitionEventProducerAsync(sinkLocation,
            //                                                                                            nesPartition,
            //                                                                                            localBufferManager,
            //                                                                                            waitTime,
            //                                                                                            retryTimes);
            //                workerContext.storeEventChannelFuture(uniqueNetworkSourceIdentifier, std::move(channelFuture));
            //                break;
            //            } else {
            (void) waitTime;
            (void) retryTimes;
            EventOnlyNetworkChannelPtr channel = nullptr;
            //                auto channel = networkManager->registerSubpartitionEventProducer(sinkLocation,
            //                                                                                 nesPartition,
            //                                                                                 localBufferManager,
            //                                                                                 waitTime,
            //                                                                                 retryTimes);
            if (channel == nullptr) {
                NES_WARNING("NetworkSource: reconfigure() cannot get event channel {} on Thread {}",
                            nesPartition.toString(),
                            Runtime::NesThread::getId());
                return;// partition was deleted on the other side of the channel... no point in waiting for a channel
            }
            //workerContext.storeEventOnlyChannel(this->operatorId, std::move(channel));
            workerContext.storeEventOnlyChannel(uniqueNetworkSourceIdentifier, std::move(channel));
            NES_DEBUG("NetworkSource: reconfigure() stored event-channel {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
            //}
            break;
        }
        case Runtime::ReconfigurationType::Destroy: {
            // necessary as event channel are lazily created so in the case of an immediate stop
            // they might not be established yet
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::Drain: {
            //event channels do not need to be drained
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            isTermination = true;
            break;
        }
        default: {
            break;
        }
    }
    if (isTermination) {
        if (!workerContext.doesEventChannelExist(uniqueNetworkSourceIdentifier)) {
            //todo #4490: allow aborting connection here
            //            auto channel = workerContext.waitForAsyncConnectionEventChannel(uniqueNetworkSourceIdentifier);
            //            if (channel) {
            //                channel->close(terminationType);
            //            }
            return;
        }
        //workerContext.releaseEventOnlyChannel(this->operatorId, terminationType);
        workerContext.releaseEventOnlyChannel(uniqueNetworkSourceIdentifier, terminationType);
        NES_DEBUG("NetworkSource: reconfigure() released channel on {} Thread {}",
                  nesPartition.toString(),
                  Runtime::NesThread::getId());
    }
}

void NetworkSource::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSource: postReconfigurationCallback() called {}", nesPartition.toString());
    NES::DataSource::postReconfigurationCallback(task);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (task.getType()) {
        case Runtime::ReconfigurationType::UpdateVersion: {
            break;
        }
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            break;
        }
        case Runtime::ReconfigurationType::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            break;
        }
        case Runtime::ReconfigurationType::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            break;
        }
        case Runtime::ReconfigurationType::Drain: {
            terminationType = Runtime::QueryTerminationType::Drain;
            break;
        }
        default: {
            break;
        }
    }
    if (Runtime::QueryTerminationType::Invalid != terminationType) {
        NES_DEBUG("NetworkSource: postReconfigurationCallback(): unregistering SubpartitionConsumer {}", nesPartition.toString());
        networkManager->unregisterSubpartitionConsumer(nesPartition);
        bool expected = true;
        if (running.compare_exchange_strong(expected, false)) {
            NES_DEBUG("NetworkSource is stopped on reconf task with id {}", nesPartition.toString());
            queryManager->notifySourceCompletion(shared_from_base<DataSource>(), terminationType);
        }
    }
}

void NetworkSource::runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

void NetworkSource::onEndOfStream(Runtime::QueryTerminationType terminationType) {
    // propagate EOS to the locally running QEPs that use the network source
    NES_DEBUG("Going to inject eos for {} terminationType={}", nesPartition, terminationType);
    if (Runtime::QueryTerminationType::Graceful == terminationType || Runtime::QueryTerminationType::Drain == terminationType) {
        queryManager->addEndOfStream(shared_from_base<DataSource>(), terminationType);
    } else {
        NES_WARNING("Ignoring forceful EoS on {}", nesPartition);
    }
}

void NetworkSource::onDrainMessage(uint64_t version) {
    std::unique_lock lock(versionMutex);
    receivedDrain = version;
    lock.unlock();
    tryStartingNewVersion();
}

void NetworkSource::markAsMigrated(uint64_t version) {
    std::unique_lock lock(versionMutex);
    migrated = version;
    lock.unlock();
    tryStartingNewVersion();
}

bool NetworkSource::tryStartingNewVersion() {
    NES_DEBUG("Updating version for network source {}", nesPartition);
    std::unique_lock lock(versionMutex);
        if (!scheduledDescriptors.empty()) {
//        if (nextSourceDescriptor) {
            NES_ASSERT(!migrated, "Network source has a new version but was also marked as migrated");
            //check if the partition is still registered of if it was removed because no channels were connected
//            if (networkManager->unregisterSubpartitionConsumerIfNotConnected(nesPartition)) {
            auto newDescriptor = scheduledDescriptors.front();
            if (receivedDrain.has_value()
                //todo: the version check is actually obsolete, as the a new version can only be started after a drain now
                && (receivedDrain.value() == newDescriptor.getVersion() || receivedDrain.value() == 0)
                && networkManager->unregisterSubpartitionConsumerIfNotConnected(nesPartition)) {
//                auto newDescriptor = nextSourceDescriptor.value();
                version = newDescriptor.getVersion();
                sinkLocation = newDescriptor.getNodeLocation();
                receivedDrain = std::nullopt;
                nesPartition = newDescriptor.getNesPartition();
//                nextSourceDescriptor = std::nullopt;
                scheduledDescriptors.pop_front();
                //bind the sink to the new partition
                bind();
                auto reconfMessage = Runtime::ReconfigurationMessage(-1,
                                                                     -1,
                                                                     Runtime::ReconfigurationType::UpdateVersion,
                                                                     Runtime::Reconfigurable::shared_from_this());
                queryManager->addReconfigurationMessage(-1, -1, reconfMessage, false);
                return true;
            }
            return false;
        }
    if (migrated.has_value()) {
        //we have to check receive drian here, because otherwise we might migrate the source before the upstream has connected at all
        //todo: move load() here
        if (receivedDrain.has_value()
            && (receivedDrain.value() == migrated.value() || receivedDrain.value() == 0)
            && networkManager->unregisterSubpartitionConsumerIfNotConnected(nesPartition)) {
            migrated = std::nullopt;
            receivedDrain = std::nullopt;
            lock.unlock();
            onEndOfStream(Runtime::QueryTerminationType::Drain);
            return true;
        }
        return false;
    }
    return false;
}

//DecomposedQueryPlanVersion NetworkSource::getVersion() {
//    std::unique_lock lock(versionMutex);
//    return version;
//}

void NetworkSource::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef workerContext) {
    NES_DEBUG("NetworkSource::onEvent(event, wrkContext) called. operatorId: {}", this->operatorId);
    if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        //auto senderChannel = workerContext.getEventOnlyNetworkChannel(this->operatorId);
        auto senderChannel = workerContext.getEventOnlyNetworkChannel(uniqueNetworkSourceIdentifier);
        if (!senderChannel) {
            //auto senderChannelOptional = workerContext.getAsyncEventChannelConnectionResult(this->operatorId);
            auto senderChannelOptional = workerContext.getAsyncEventChannelConnectionResult(uniqueNetworkSourceIdentifier);
            if (!senderChannelOptional) {
                NES_DEBUG("NetworkSource::onEvent(event, wrkContext) operatorId: {}: could not send event because event "
                          "channel "
                          "has not been established yet",
                          this->operatorId);
                return;
            }
            //workerContext.storeEventOnlyChannel(this->operatorId, std::move(senderChannelOptional.value()));
            workerContext.storeEventOnlyChannel(uniqueNetworkSourceIdentifier, std::move(senderChannelOptional.value()));
            //senderChannel = workerContext.getEventOnlyNetworkChannel(this->operatorId);
            senderChannel = workerContext.getEventOnlyNetworkChannel(uniqueNetworkSourceIdentifier);
            if (!senderChannel) {
                return;
            }
        }

        senderChannel->sendEvent<Runtime::StartSourceEvent>();
    }
}

OperatorId NetworkSource::getUniqueId() const { return uniqueNetworkSourceIdentifier; }

bool NetworkSource::scheduleNewDescriptor(const NetworkSourceDescriptor& networkSourceDescriptor) {
    std::unique_lock lock(versionMutex);
    if (nesPartition != networkSourceDescriptor.getNesPartition()) {
//        nextSourceDescriptor = networkSourceDescriptor;
        scheduledDescriptors.push_back(networkSourceDescriptor);
        tryStartingNewVersion();
        return true;
    }
    return false;
}
}// namespace NES::Network