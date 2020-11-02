#ifdef ENABLE_OPC_BUILD
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <utility>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, std::string url, UA_NodeId nodeId,
                                                std::string user, std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(url), std::move(nodeId), std::move(user), std::move(password)));
}

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string url, UA_NodeId nodeId,
                                                std::string user, std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(streamName), std::move(url), std::move(nodeId), std::move(user), std::move(password)));
}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, std::string url, UA_NodeId nodeId, std::string user,
                                         std::string password)
    : SourceDescriptor(std::move(schema)), url(std::move(url)), nodeId(std::move(nodeId)), user(std::move(user)), password(std::move(password)) {}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, std::string streamName, std::string url, UA_NodeId nodeId, std::string user,
                                         std::string password)
    : SourceDescriptor(std::move(schema), std::move(streamName)), url(std::move(url)), nodeId(std::move(nodeId)), user(std::move(user)), password(std::move(password)) {}

const std::string OPCSourceDescriptor::getUrl() const {
    return url;
}

UA_NodeId OPCSourceDescriptor::getNodeId() const {
    return nodeId;
}

const std::string OPCSourceDescriptor::getUser() const {
    return user;
}

const std::string OPCSourceDescriptor::getPassword() const {
    return password;
}

bool OPCSourceDescriptor::equal(SourceDescriptorPtr other) {

    if (!other->instanceOf<OPCSourceDescriptor>())
        return false;
    auto otherOPCSource = other->as<OPCSourceDescriptor>();
    NES_DEBUG("URL= " << url << " == " << otherOPCSource->getUrl());
    char* otherSourceIdent = (char*) UA_malloc(sizeof(char) * otherOPCSource->getNodeId().identifier.string.length + 1);
    memcpy(otherSourceIdent, otherOPCSource->getNodeId().identifier.string.data, otherOPCSource->getNodeId().identifier.string.length);
    otherSourceIdent[otherOPCSource->getNodeId().identifier.string.length] = '\0';
    char* ident = (char*) UA_malloc(sizeof(char) * nodeId.identifier.string.length + 1);
    memcpy(ident, nodeId.identifier.string.data, nodeId.identifier.string.length);
    ident[nodeId.identifier.string.length] = '\0';
    return url == otherOPCSource->getUrl() && !strcmp(ident, otherSourceIdent) && nodeId.namespaceIndex == otherOPCSource->getNodeId().namespaceIndex && nodeId.identifierType == otherOPCSource->getNodeId().identifierType && user == otherOPCSource->getUser() && password == otherOPCSource->getPassword();
}

std::string OPCSourceDescriptor::toString() {
    return "OPCSourceDescriptor()";
}

}// namespace NES

#endif