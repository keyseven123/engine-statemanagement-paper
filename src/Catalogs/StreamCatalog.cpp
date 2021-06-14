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

#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <assert.h>

namespace NES {

void StreamCatalog::addDefaultStreams() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("Streamcatalog addDefaultStreams");
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    bool success = addLogicalStream("default_logical", schema);
    if (!success) {
        NES_ERROR("StreamCatalog::addDefaultStreams: error while add default_logical");
        throw Exception("Error while addDefaultStreams StreamCatalog");
    }

    //TODO I think we should get rid of this soon
    SchemaPtr schemaExdra = Schema::create()
                                ->addField("id", DataTypeFactory::createUInt64())
                                ->addField("metadata_generated", DataTypeFactory::createUInt64())
                                ->addField("metadata_title", DataTypeFactory::createFixedChar(50))
                                ->addField("metadata_id", DataTypeFactory::createFixedChar(50))
                                ->addField("features_type", DataTypeFactory::createFixedChar(50))
                                ->addField("features_properties_capacity", DataTypeFactory::createUInt64())
                                ->addField("features_properties_efficiency", DataTypeFactory::createFloat())
                                ->addField("features_properties_mag", DataTypeFactory::createFloat())
                                ->addField("features_properties_time", DataTypeFactory::createUInt64())
                                ->addField("features_properties_updated", DataTypeFactory::createUInt64())
                                ->addField("features_properties_type", DataTypeFactory::createFixedChar(50))
                                ->addField("features_geometry_type", DataTypeFactory::createFixedChar(50))
                                ->addField("features_geometry_coordinates_longitude", DataTypeFactory::createFloat())
                                ->addField("features_geometry_coordinates_latitude", DataTypeFactory::createFloat())
                                ->addField("features_eventId ", DataTypeFactory::createFixedChar(50));

    bool success2 = addLogicalStream("exdra", schemaExdra);
    if (!success2) {
        NES_ERROR("StreamCatalog::addDefaultStreams: error while adding exdra logical stream");
        throw Exception("Error while addDefaultStreams StreamCatalog");
    }
}
StreamCatalog::StreamCatalog() : catalogMutex() {
    NES_DEBUG("StreamCatalog: construct stream catalog");
    addDefaultStreams();
    NES_DEBUG("StreamCatalog: construct stream catalog successfully");
}

StreamCatalog::~StreamCatalog() { NES_DEBUG("~StreamCatalog:"); }

bool StreamCatalog::addLogicalStream(const std::string& streamName, const std::string& streamSchema) {
    std::unique_lock lock(catalogMutex);
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    NES_DEBUG("StreamCatalog: schema successfully created");
    return addLogicalStream(streamName, schema);
}

bool StreamCatalog::addLogicalStream(std::string logicalStreamName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);
    //check if stream already exist
    NES_DEBUG("StreamCatalog: search for logical stream in addLogicalStream() " << logicalStreamName);

    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_DEBUG("StreamCatalog: add logical stream " << logicalStreamName);
        logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;
        return true;
    } else {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " already exists");
        return false;
    }
}

bool StreamCatalog::removeLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in removeLogicalStream() " << logicalStreamName);

    //if log stream does not exists
    if (logicalStreamToSchemaMapping.find(logicalStreamName) == logicalStreamToSchemaMapping.end()) {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " does not exists");
        return false;
    } else {

        NES_DEBUG("StreamCatalog: remove logical stream " << logicalStreamName);
        if (logicalToPhysicalStreamMapping[logicalStreamName].size() != 0) {
            NES_DEBUG("StreamCatalog: cannot remove " << logicalStreamName
                                                      << " because there are physical entries for this stream");
            return false;
        }
        uint64_t cnt = logicalStreamToSchemaMapping.erase(logicalStreamName);
        NES_DEBUG("StreamCatalog: removed " << cnt << " copies of the stream");
        NES_ASSERT(!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName), "log stream should not exist");
        return true;
    }
}

// BDAPRO - discuss why bool is necessary here. i.e. what should be checked here?
bool StreamCatalog::addPhysicalStreamWithoutLogicalStreams(StreamCatalogEntryPtr newEntry) {
    // BDAPRO introduce some kind of error status enum
    misconfiguredPhysicalStreams[newEntry->getPhysicalName()] = "Missing logical stream name";
    nameToPhysicalStream[newEntry->getPhysicalName()] = newEntry;
    return true;
}


bool StreamCatalog::addPhysicalStream(std::string logicalStreamName, StreamCatalogEntryPtr newEntry) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in addPhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " does not exists when inserting physical stream "
                                                   << newEntry->getPhysicalName());
        return false;
    } else {
        NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " exists try to add physical stream "
                                                   << newEntry->getPhysicalName());
        //get current physicalStreamNames for this logicalStream
        std::vector<std::string> physicalStreams = logicalToPhysicalStreamMapping[logicalStreamName];
        //get corresponding StreamCatalogEntries

        // BDAPRO discuss why streamName as such is not sufficient.
        std::vector<StreamCatalogEntryPtr> entries;
        for (std::string physicalStreamName : physicalStreams){
            NES_INFO("PUSH BACK");
            entries.push_back(nameToPhysicalStream[physicalStreamName]);
        }
        //check if physical stream does not exist yet
        for (StreamCatalogEntryPtr entry : entries) {
            NES_DEBUG("test node id=" << entry->getNode()->getId() << " phyStr=" << entry->getPhysicalName());
            NES_DEBUG("test to be inserted id=" << newEntry->getNode()->getId() << " phyStr=" << newEntry->getPhysicalName());
            if (entry->getPhysicalName() == newEntry->getPhysicalName()) {
                if (entry->getNode()->getId() == newEntry->getNode()->getId()) {
                    NES_ERROR("StreamCatalog: node with id=" << newEntry->getNode()->getId()
                                                             << " name=" << newEntry->getPhysicalName() << " already exists");
                    return false;
                }
            }
        }
    }

    NES_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist, try to add");

    //if first one
    if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
        NES_DEBUG("stream already exist, just add new entry");
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry->getPhysicalName());
        nameToPhysicalStream[newEntry->getPhysicalName()] = newEntry;
    } else {
        NES_DEBUG("stream does not exist, create new item");
        logicalToPhysicalStreamMapping.insert(
            std::pair<std::string, std::vector<std::string>>(logicalStreamName, std::vector<std::string>()));
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry->getPhysicalName());
        nameToPhysicalStream[newEntry->getPhysicalName()] = newEntry;
    }

    NES_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " id=" << newEntry->getNode()->getId()
                                                << " successful added");
    return true;
}

// BDAPRO consider implementing for completeness reasons.
// DELETE mapping log to vector
// ADD LABEL MISCONFIGURED - needs to checked because stream can be present in other log stream
bool StreamCatalog::removeAllPhysicalStreams(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in removeAllPhysicalStreams() " << logicalStreamName);
    if (logicalStreamToSchemaMapping.find(logicalStreamName) == logicalStreamToSchemaMapping.end()) {
        NES_DEBUG("StreamCatalog: logical stream "
        << logicalStreamName << " does not exists when trying to remove all physicals stream)");
        return false;
    }
    else{
        logicalStreamToSchemaMapping.erase(logicalStreamName);
        return true;
    }
}

//BDAPRO consider implementing a function removing a physical stream from ALL logical streams.
bool StreamCatalog::removePhysicalStreamFromAllLogicalStreams(std::string physicalStreamName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG(physicalStreamName);
    NES_NOT_IMPLEMENTED();
}

// BDAPRO rename hashID - discuss meaning.
// BDAPRO needs to be changed: Currently deleted a physicalStream from logicalStream mapping and deleting it from nameToPhysical which is wrong
// nameToPhysical should stay as it could be important in other logical streams
bool StreamCatalog::removePhysicalStream(std::string logicalStreamName, std::string physicalStreamName, std::uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in removePhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (logicalStreamToSchemaMapping.find(logicalStreamName) == logicalStreamToSchemaMapping.end()) {
        NES_DEBUG("StreamCatalog: logical stream "
                  << logicalStreamName << " does not exists when trying to remove physical stream with hashId" << hashId);
        return false;
    } else {
        NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " exists try to remove physical stream"
                                                   << physicalStreamName << " from node " << hashId);
        for (std::vector<std::string>::const_iterator streamName =
                 logicalToPhysicalStreamMapping[logicalStreamName].cbegin();
             streamName != logicalToPhysicalStreamMapping[logicalStreamName].cend();
             streamName++) {
            StreamCatalogEntryPtr entry = nameToPhysicalStream[*streamName];
            NES_DEBUG("test node id=" << entry->getNode()->getId() << " phyStr=" << entry->getPhysicalName());
            NES_DEBUG("test to be deleted id=" << hashId << " phyStr=" << physicalStreamName);
            if (entry->getPhysicalName() == physicalStreamName) {
                NES_DEBUG("StreamCatalog: node with name=" << physicalStreamName << " exists try match hashId" << hashId);

                if (entry->getNode()->getId() == hashId) {
                    NES_DEBUG("StreamCatalog: node with id=" << hashId << " name=" << physicalStreamName
                                                             << " exists try to erase");
                    logicalToPhysicalStreamMapping[logicalStreamName].erase(streamName);
                    NES_DEBUG("StreamCatalog: number of entries afterwards "
                              << logicalToPhysicalStreamMapping[logicalStreamName].size());
                    NES_DEBUG("StreamCatalog: deleting physicalStream from nameToPhysicalStream mapping");
                    nameToPhysicalStream.erase(*streamName);
                    NES_DEBUG("StreamCatalog: number of entries afterwards "
                                      << nameToPhysicalStream.size());
                    return true;
                }
            }
        }
        NES_DEBUG("StreamCatalog: physical stream " << physicalStreamName << " does not exist on node with id" << hashId
                                                    << " and with logicalStreamName " << logicalStreamName);
    }
    NES_DEBUG("StreamCatalog: physical stream " << physicalStreamName << " does not exist on node with id" << hashId);
    return false;
}

//BDAPRO add removeMisconfiguredLogicalStream function - allowing to delete a physicalStream with no logical stream just by its name.
// Add check for misconfigured reason.
// bool StreamCatalog::removeMisconfiguredLogicalStream(std::string physicalStreamName){}
/*
 *     if(std::find(hashIdToPhysicalStream.begin(), hashIdToPhysicalStream.end(),hashId)!=hashIdToPhysicalStream.end()){
        hashIdToPhysicalStream.erase(hashId);
        return true
    }*/

bool StreamCatalog::removePhysicalStreamByHashId(uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    //removing physical streams for which a mapping to at least one logical stream exists.
    for (auto logStream : logicalToPhysicalStreamMapping) {
        NES_DEBUG("StreamCatalog: check log stream " << logStream.first);
        for (std::vector<std::string>::const_iterator physicalStreamName = logicalToPhysicalStreamMapping[logStream.first].cbegin();
             physicalStreamName != logicalToPhysicalStreamMapping[logStream.first].cend();
             physicalStreamName++) {
            StreamCatalogEntryPtr entry = nameToPhysicalStream[*physicalStreamName];
            if (entry->getNode()->getId() == hashId){
                NES_DEBUG("StreamCatalog: found entry with nodeid=" << entry->getNode()->getId()
                                                                    << " physicalStream=" << entry->getPhysicalName()
                                                                    << " logicalStream=" << logStream.first);
                //TODO: fix this to return value of erase to update entry or if you use the foreach loop, collect the entries to remove, and remove them in a batch after
                NES_DEBUG("StreamCatalog: deleted physical stream with hashID" << hashId << "and name"
                                                                               << entry->getPhysicalName());
                logicalToPhysicalStreamMapping[logStream.first].erase(physicalStreamName);
                nameToPhysicalStream.erase(*physicalStreamName);
                return true;
            }
        }
    }
    return false;
}

SchemaPtr StreamCatalog::getSchemaForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalStreamToSchemaMapping[logicalStreamName];
}

// BDAPRO change to vector function
LogicalStreamPtr StreamCatalog::getStreamForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return std::make_shared<LogicalStream>(logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
}

// BDAPRO change to vector function
LogicalStreamPtr StreamCatalog::getStreamForLogicalStreamOrThrowException(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    if (logicalStreamToSchemaMapping.find(logicalStreamName) != logicalStreamToSchemaMapping.end()) {
        return std::make_shared<LogicalStream>(logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
    } else {
        NES_ERROR("StreamCatalog::getStreamForLogicalStreamOrThrowException: stream does not exists " << logicalStreamName);
        throw Exception("Required stream does not exists " + logicalStreamName);
    }
}

std::tuple<std::vector<std::string>, std::vector<std::string>>
StreamCatalog::testIfLogicalStreamVecExistsInSchemaMapping(std::vector<std::string> logicalStreamNames){
    std::vector<std::string> included;
    std::vector<std::string> excluded;

    for(std::string logicalStreamName: logicalStreamNames){
        if(StreamCatalog::testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)){
            included.push_back(logicalStreamName);
        }else{
            included.push_back(logicalStreamName);
        }
    }

    return std::make_tuple(included, excluded);
}

bool StreamCatalog::testIfLogicalStreamExistsInSchemaMapping(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalStreamToSchemaMapping.find(logicalStreamName)//if log stream does not exists
        != logicalStreamToSchemaMapping.end();
}
bool StreamCatalog::testIfLogicalStreamExistsInLogicalToPhysicalMapping(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalToPhysicalStreamMapping.find(logicalStreamName)//if log stream does not exists
        != logicalToPhysicalStreamMapping.end();
}

std::vector<TopologyNodePtr> StreamCatalog::getSourceNodesForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<TopologyNodePtr> listOfSourceNodes;

    //get current physical stream for this logical stream
    std::vector<std::string> physicalStreamNames = logicalToPhysicalStreamMapping[logicalStreamName];

    // BDAPRO loop over vector to get StreamCatalogEntryPtrs - done
    std::vector<StreamCatalogEntryPtr> physicalStreams;
    for (auto name : physicalStreamNames){
        physicalStreams.push_back(nameToPhysicalStream[name]);
    }

    if (physicalStreams.empty()) {
        return listOfSourceNodes;
    }

    for (StreamCatalogEntryPtr entry : physicalStreams) {
        listOfSourceNodes.push_back(entry->getNode());
    }

    return listOfSourceNodes;
}

bool StreamCatalog::reset() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: reset Stream Catalog");
    logicalStreamToSchemaMapping.clear();
    logicalToPhysicalStreamMapping.clear();

    addDefaultStreams();
    NES_DEBUG("StreamCatalog: reset Stream Catalog completed");
    return true;
}

// BDAPRO test this function
std::string StreamCatalog::getPhysicalStreamAndSchemaAsString() {
    std::unique_lock lock(catalogMutex);
    std::stringstream ss;
    for (auto entry : logicalToPhysicalStreamMapping) {
        ss << "stream name=" << entry.first << " with " << entry.second.size() << " elements:";
        for (std::string physicalStreamName : entry.second) {
            ss << nameToPhysicalStream[physicalStreamName]->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}

// BDAPRO test this function
std::vector<StreamCatalogEntryPtr> StreamCatalog::getPhysicalStreams(std::string logicalStreamName) {
    std::vector<std::string> physicalStreamsNames = logicalToPhysicalStreamMapping[logicalStreamName];
    std::vector<StreamCatalogEntryPtr> physicalStreams;

    // Iterate over all hashIDs and retrieve respective StreamCatalogEntryPtr
    for (auto physicalStreamName : physicalStreamsNames){
        physicalStreams.push_back(nameToPhysicalStream[physicalStreamName]);
    }
    return physicalStreams;
}

std::map<std::string, SchemaPtr> StreamCatalog::getAllLogicalStream() { return logicalStreamToSchemaMapping; }

std::map<std::string, std::string> StreamCatalog::getAllLogicalStreamAsString() {
    std::unique_lock lock(catalogMutex);
    std::map<std::string, std::string> allLogicalStreamAsString;
    const std::map<std::string, SchemaPtr> allLogicalStream = getAllLogicalStream();

    for (auto const& [key, val] : allLogicalStream) {
        allLogicalStreamAsString[key] = val->toString();
    }
    return allLogicalStreamAsString;
}

bool StreamCatalog::updatedLogicalStream(std::string& streamName, std::string& streamSchema) {
    NES_INFO("StreamCatalog: Update the logical stream " << streamName << " with the schema " << streamSchema);
    std::unique_lock lock(catalogMutex);

    NES_TRACE("StreamCatalog: Check if logical stream exists in the catalog.");
    if (!testIfLogicalStreamExistsInSchemaMapping(streamName)) {
        NES_ERROR("StreamCatalog: Unable to find logical stream " << streamName << " to update.");
        return false;
    }

    NES_TRACE("StreamCatalog: create a new schema object and add to the catalog");
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    logicalStreamToSchemaMapping[streamName] = schema;
    return true;
}

}// namespace NES
