#include <Network/NesPartition.hpp>
#include <Network/NetworkSource.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Network {

NetworkSource::NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager, NesPartition nesPartition) : DataSource(schema, bufferManager, queryManager, std::to_string(nesPartition.getOperatorId())),
                                                                                            networkManager(networkManager),
                                                                                            nesPartition(nesPartition) {
    NES_INFO("NetworkSource: Initializing NetworkSource for " << nesPartition.toString());
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

NetworkSource::~NetworkSource() {
    NES_DEBUG("NetworkSink: Destroying NetworkSource " << nesPartition.toString());
}

std::optional<TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const {
    return NETWORK_SOURCE;
}

const std::string NetworkSource::toString() const {
    return "NetworkSource: " + nesPartition.toString();
}

bool NetworkSource::start() {
    NES_DEBUG("NetworkSource: start called on " << nesPartition);
    return networkManager->registerSubpartitionConsumer(nesPartition) == 0;
}

bool NetworkSource::stop() {
    // TODO ensure proper termination: what should happen here when we call stop but refCnt has not reached zero?
    NES_DEBUG("NetworkSource: stop called on " << nesPartition);
    networkManager->unregisterSubpartitionConsumer(nesPartition);
    return true;
}

void NetworkSource::runningRoutine(BufferManagerPtr, QueryManagerPtr) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

}// namespace Network
}// namespace NES