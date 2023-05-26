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
#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <WorkQueues/StorageHandles/SerialStorageHandler.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/TopologyNode.hpp>

namespace NES {
class SerialStorageHandlerTest : public Testing::TestWithErrorHandling {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SerialStorageHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup SerialAccessHandle test class.");
    }
};

TEST_F(SerialStorageHandlerTest, TestResourceAccess) {
    //create access handle
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto serialAccessHandle = SerialStorageHandler::create(globalExecutionPlan,
                                                           topology,
                                                           queryCatalogService,
                                                           globalQueryPlan,
                                                           sourceCatalog,
                                                           udfCatalog);

    //test if we can obtain the resource we passed to the constructor
    ASSERT_EQ(globalExecutionPlan.get(), serialAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_EQ(topology.get(), serialAccessHandle->getTopologyHandle().get());
    ASSERT_EQ(queryCatalogService.get(), serialAccessHandle->getQueryCatalogServiceHandle().get());
    ASSERT_EQ(globalQueryPlan.get(), serialAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_EQ(sourceCatalog.get(), serialAccessHandle->getSourceCatalogHandle().get());
    ASSERT_EQ(udfCatalog.get(), serialAccessHandle->getUDFCatalogHandle().get());
}

}// namespace NES
