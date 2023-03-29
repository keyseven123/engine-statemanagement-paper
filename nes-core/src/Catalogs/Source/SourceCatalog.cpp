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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <utility>

namespace NES::Catalogs::Source {

void SourceCatalog::addDefaultSources() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG2("Sourcecatalog addDefaultSources");
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    bool success = addLogicalSource("default_logical", schema);
    if (!success) {
        NES_ERROR2("SourceCatalog::addDefaultSources: error while add default_logical");
        throw Exceptions::RuntimeException("Error while addDefaultSources SourceCatalog");
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

    bool success2 = addLogicalSource("exdra", schemaExdra);
    if (!success2) {
        NES_ERROR2("SourceCatalog::addDefaultSources: error while adding exdra logical source");
        throw Exceptions::RuntimeException("Error while addDefaultSources SourceCatalog");
    }

    //    SchemaPtr iris = Schema::create()
    //                            ->addField(createField("id", UINT64))
    //                            ->addField(createField("SepalLengthCm", FLOAT32))
    //                            ->addField(createField("SepalWidthCm", FLOAT32))
    //                            ->addField(createField("PetalLengthCm", FLOAT32))
    //                            ->addField(createField("PetalWidthCm", FLOAT32))
    //                            ->addField(createField("SpeciesCode", UINT64))
    //                            ->addField(createField("CreationTime", UINT64));
    //    bool success3 = addLogicalSource("iris", iris);
    //    if (!success3) {
    //        NES_ERROR2("SourceCatalog::addDefaultSources: error while adding iris logical source");
    //        throw Exceptions::RuntimeException("Error while addDefaultSources SourceCatalog");
    //    }
}

SourceCatalog::SourceCatalog(QueryParsingServicePtr queryParsingService) : queryParsingService(queryParsingService) {
    NES_DEBUG2("SourceCatalog: construct source catalog");
    addDefaultSources();
    NES_DEBUG2("SourceCatalog: construct source catalog successfully");
}

bool SourceCatalog::addLogicalSource(const std::string& sourceName, const std::string& sourceSchema) {
    std::unique_lock lock(catalogMutex);
    SchemaPtr schema = queryParsingService->createSchemaFromCode(sourceSchema);
    NES_DEBUG2("SourceCatalog: schema successfully created");
    return addLogicalSource(sourceName, schema);
}

bool SourceCatalog::addLogicalSource(const std::string& logicalSourceName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);
    //check if source already exist
    NES_DEBUG2("SourceCatalog: Check if logical source {} already exist.", logicalSourceName);

    if (!containsLogicalSource(logicalSourceName)) {
        NES_DEBUG2("SourceCatalog: add logical source {}", logicalSourceName);
        logicalSourceNameToSchemaMapping[logicalSourceName] = std::move(schemaPtr);
        return true;
    }
    NES_ERROR2("SourceCatalog: logical source {} already exists", logicalSourceName);
    return false;
}

bool SourceCatalog::removeLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG2("SourceCatalog: search for logical source in removeLogicalSource() {}", logicalSourceName);

    //if log source does not exists
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) == logicalSourceNameToSchemaMapping.end()) {
        NES_ERROR2("SourceCatalog: logical source {} already exists", logicalSourceName);
        return false;
    }
    NES_DEBUG2("SourceCatalog: remove logical source {}", logicalSourceName);
    if (logicalToPhysicalSourceMapping[logicalSourceName].size() != 0) {
        NES_DEBUG2("SourceCatalog: cannot remove {} because there are physical entries for this source", logicalSourceName);
        return false;
    }
    uint64_t cnt = logicalSourceNameToSchemaMapping.erase(logicalSourceName);
    NES_DEBUG2("SourceCatalog: removed {} copies of the source", cnt);
    NES_ASSERT(!containsLogicalSource(logicalSourceName), "log source should not exist");
    return true;
}

bool SourceCatalog::addPhysicalSource(const std::string& logicalSourceName, const SourceCatalogEntryPtr& newSourceCatalogEntry) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG2("SourceCatalog: search for logical source in addPhysicalSource() {}", logicalSourceName);

    // check if logical source exists
    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR2("SourceCatalog: logical source {} does not exists when inserting physical source {} ",
                   logicalSourceName,
                   newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());
        return false;
    } else {
        NES_DEBUG2("SourceCatalog: logical source {} exists try to add physical source {}",
                   logicalSourceName,
                   newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());

        //get current physical source for this logical source
        std::vector<SourceCatalogEntryPtr> physicalSources = logicalToPhysicalSourceMapping[logicalSourceName];

        //check if physical source does not exist yet
        for (const SourceCatalogEntryPtr& sourceCatalogEntry : physicalSources) {
            auto physicalSourceToTest = sourceCatalogEntry->getPhysicalSource();
            NES_DEBUG2("test node id={} phyStr={}",
                       sourceCatalogEntry->getNode()->getId(),
                       physicalSourceToTest->getPhysicalSourceName());
            if (physicalSourceToTest->getPhysicalSourceName()
                    == newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName()
                && sourceCatalogEntry->getNode()->getId() == newSourceCatalogEntry->getNode()->getId()) {
                NES_ERROR2("SourceCatalog: node with id={} name={} already exists",
                           sourceCatalogEntry->getNode()->getId(),
                           physicalSourceToTest->getPhysicalSourceName());
                return false;
            }
        }
    }

    NES_DEBUG2("SourceCatalog: physical source  {} does not exist, try to add",
               newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());

    //if first one
    if (testIfLogicalSourceExistsInLogicalToPhysicalMapping(logicalSourceName)) {
        NES_DEBUG2("SourceCatalog: Logical source already exists, add new physical entry");
        logicalToPhysicalSourceMapping[logicalSourceName].push_back(newSourceCatalogEntry);
    } else {
        NES_DEBUG2("SourceCatalog: Logical source does not exist, create new item");
        logicalToPhysicalSourceMapping.insert(
            std::pair<std::string, std::vector<SourceCatalogEntryPtr>>(logicalSourceName, std::vector<SourceCatalogEntryPtr>()));
        logicalToPhysicalSourceMapping[logicalSourceName].push_back(newSourceCatalogEntry);
    }

    NES_DEBUG2("SourceCatalog: physical source with id={} successful added", newSourceCatalogEntry->getNode()->getId());
    return true;
}

bool SourceCatalog::removeAllPhysicalSources(const std::string&) {
    std::unique_lock lock(catalogMutex);
    NES_NOT_IMPLEMENTED();
}

bool SourceCatalog::removePhysicalSource(const std::string& logicalSourceName,
                                         const std::string& physicalSourceName,
                                         std::uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG2("SourceCatalog: search for logical source in removePhysicalSource() {}", logicalSourceName);

    // check if logical source exists
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) == logicalSourceNameToSchemaMapping.end()) {
        NES_ERROR2("SourceCatalog: logical source {} does not exists when trying to remove physical source with hashId {}",
                   logicalSourceName,
                   hashId);
        return false;
    }
    NES_DEBUG2("SourceCatalog: logical source {} exists try to remove physical source{} from node {}",
               logicalSourceName,
               physicalSourceName,
               hashId);
    for (auto entry = logicalToPhysicalSourceMapping[logicalSourceName].cbegin();
         entry != logicalToPhysicalSourceMapping[logicalSourceName].cend();
         entry++) {
        NES_DEBUG2("test node id={} phyStr={}",
                   entry->get()->getNode()->getId(),
                   entry->get()->getPhysicalSource()->getPhysicalSourceName());
        NES_DEBUG2("test to be deleted id={} phyStr={}", hashId, physicalSourceName);
        if (entry->get()->getPhysicalSource()->getPhysicalSourceName() == physicalSourceName) {
            NES_DEBUG2("SourceCatalog: node with name={} exists try match hashId {}", physicalSourceName, hashId);

            if (entry->get()->getNode()->getId() == hashId) {
                NES_DEBUG2("SourceCatalog: node with id={} name={} exists try to erase", hashId, physicalSourceName);
                logicalToPhysicalSourceMapping[logicalSourceName].erase(entry);
                NES_DEBUG2("SourceCatalog: number of entries afterwards {}",
                           logicalToPhysicalSourceMapping[logicalSourceName].size());
                return true;
            }
        }
    }
    NES_DEBUG2("SourceCatalog: physical source {} does not exist on node with id {} and with logicalSourceName {}",
               physicalSourceName,
               hashId,
               logicalSourceName);

    NES_DEBUG2("SourceCatalog: physical source {} does not exist on node with id {}", physicalSourceName, hashId);
    return false;
}

bool SourceCatalog::removePhysicalSourceByHashId(uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    for (const auto& logSource : logicalToPhysicalSourceMapping) {
        NES_DEBUG2("SourceCatalog: check log source {}", logSource.first);
        for (auto entry = logicalToPhysicalSourceMapping[logSource.first].cbegin();
             entry != logicalToPhysicalSourceMapping[logSource.first].cend();
             entry++) {
            if (entry->get()->getNode()->getId() == hashId) {
                NES_DEBUG2("SourceCatalog: found entry with nodeid={} physicalSource={} logicalSource={}",
                           entry->get()->getNode()->getId(),
                           entry->get()->getPhysicalSource()->getPhysicalSourceName(),
                           logSource.first);
                //TODO: fix this to return value of erase to update entry or if you use the foreach loop, collect the entries to remove, and remove them in a batch after
                NES_DEBUG2("SourceCatalog: deleted physical source with hashID {} and name {}",
                           hashId,
                           entry->get()->getPhysicalSource()->getPhysicalSourceName());
                logicalToPhysicalSourceMapping[logSource.first].erase(entry);
                return true;
            }
        }
    }
    return false;
}

SchemaPtr SourceCatalog::getSchemaForLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) == logicalSourceNameToSchemaMapping.end()) {
        throw MapEntryNotFoundException("SourceCatalog: No schema found for logical source " + logicalSourceName);
    }
    return logicalSourceNameToSchemaMapping[logicalSourceName];
}

LogicalSourcePtr SourceCatalog::getLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    return LogicalSource::create(logicalSourceName, logicalSourceNameToSchemaMapping[logicalSourceName]);
}

LogicalSourcePtr SourceCatalog::getLogicalSourceOrThrowException(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) != logicalSourceNameToSchemaMapping.end()) {
        return LogicalSource::create(logicalSourceName, logicalSourceNameToSchemaMapping[logicalSourceName]);
    }
    NES_ERROR2("SourceCatalog::getLogicalSourceOrThrowException: source does not exists {}", logicalSourceName);
    throw Exceptions::RuntimeException("Required source does not exists " + logicalSourceName);
}

bool SourceCatalog::containsLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    return logicalSourceNameToSchemaMapping.find(logicalSourceName)//if log source does not exists
        != logicalSourceNameToSchemaMapping.end();
}
bool SourceCatalog::testIfLogicalSourceExistsInLogicalToPhysicalMapping(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    return logicalToPhysicalSourceMapping.find(logicalSourceName)//if log source does not exists
        != logicalToPhysicalSourceMapping.end();
}

std::vector<TopologyNodePtr> SourceCatalog::getSourceNodesForLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock lock(catalogMutex);
    std::vector<TopologyNodePtr> listOfSourceNodes;

    //get current physical source for this logical source
    std::vector<SourceCatalogEntryPtr> physicalSources = logicalToPhysicalSourceMapping[logicalSourceName];

    if (physicalSources.empty()) {
        return listOfSourceNodes;
    }

    for (const SourceCatalogEntryPtr& entry : physicalSources) {
        listOfSourceNodes.push_back(entry->getNode());
    }

    return listOfSourceNodes;
}

bool SourceCatalog::reset() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG2("SourceCatalog: reset Source Catalog");
    logicalSourceNameToSchemaMapping.clear();
    logicalToPhysicalSourceMapping.clear();

    addDefaultSources();
    NES_DEBUG2("SourceCatalog: reset Source Catalog completed");
    return true;
}

std::string SourceCatalog::getPhysicalSourceAndSchemaAsString() {
    std::unique_lock lock(catalogMutex);
    std::stringstream ss;
    for (const auto& entry : logicalToPhysicalSourceMapping) {
        ss << "source name=" << entry.first << " with " << entry.second.size() << " elements:";
        for (const SourceCatalogEntryPtr& sce : entry.second) {
            ss << sce->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}

std::vector<SourceCatalogEntryPtr> SourceCatalog::getPhysicalSources(const std::string& logicalSourceName) {
    if (logicalToPhysicalSourceMapping.find(logicalSourceName) == logicalToPhysicalSourceMapping.end()) {
        NES_ERROR2("SourceCatalog: Unable to find source catalog entry with logical source name {}", logicalSourceName);
        throw MapEntryNotFoundException("SourceCatalog: Logical source(s) [" + logicalSourceName
                                        + "] are found to have no physical source(s) defined. ");
    }
    return logicalToPhysicalSourceMapping[logicalSourceName];
}

std::map<std::string, SchemaPtr> SourceCatalog::getAllLogicalSource() { return logicalSourceNameToSchemaMapping; }

std::map<std::string, std::string> SourceCatalog::getAllLogicalSourceAsString() {
    std::unique_lock lock(catalogMutex);
    std::map<std::string, std::string> allLogicalSourceAsString;
    const std::map<std::string, SchemaPtr> allLogicalSource = getAllLogicalSource();

    for (auto const& [key, val] : allLogicalSource) {
        allLogicalSourceAsString[key] = val->toString();
    }
    return allLogicalSourceAsString;
}

bool SourceCatalog::updateLogicalSource(const std::string& sourceName, const std::string& sourceSchema) {
    NES_INFO2("SourceCatalog: Update the logical source {} with the schema {} ", sourceName, sourceSchema);
    std::unique_lock lock(catalogMutex);

    NES_TRACE2("SourceCatalog: Check if logical source exists in the catalog.");
    if (!containsLogicalSource(sourceName)) {
        NES_ERROR2("SourceCatalog: Unable to find logical source {} to update.", sourceName);
        return false;
    }

    NES_TRACE2("SourceCatalog: create a new schema object and add to the catalog");
    SchemaPtr schema = queryParsingService->createSchemaFromCode(sourceSchema);
    logicalSourceNameToSchemaMapping[sourceName] = schema;
    return true;
}

bool SourceCatalog::updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);
    //check if source already exist
    NES_DEBUG2("SourceCatalog: search for logical source in addLogicalSource() {}", logicalSourceName);

    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR2("SourceCatalog: Unable to find logical source {} to update.", logicalSourceName);
        return false;
    }
    NES_TRACE2("SourceCatalog: create a new schema object and add to the catalog");
    logicalSourceNameToSchemaMapping[logicalSourceName] = std::move(schemaPtr);
    return true;
}

}// namespace NES::Catalogs::Source
