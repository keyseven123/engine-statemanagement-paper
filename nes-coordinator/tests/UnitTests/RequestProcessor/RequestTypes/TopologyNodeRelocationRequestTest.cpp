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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

uint32_t EVENT_CHANNEL_RETRY_TIMES = 1;
auto WAIT_TIME = std::chrono::milliseconds(1);

uint32_t DATA_CHANNEL_RETRY_TIMES = 1;
uint64_t DEFAULT_NUMBER_OF_ORIGINS = 1;
namespace NES {

class TopologyNodeRelocationRequestTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    QueryCatalogServicePtr queryCatalogService;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyNodeRelocationRequestTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyNodeRelocationRequestTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        context = std::make_shared<z3::context>();
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

        //Setup source catalog
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();

        //Setup topology
        topology = Topology::create();

        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    }

    z3::ContextPtr context;
};

/**
* @brief Test the algorithm to identify the upstream and downstream operators used as an input for an incremental
* placement to be performed due to a topology link removal. Construct a topology, place query subplans and calculate
* the sets of upstream and downstream operators of an incremental placement.
*
*/
TEST_F(TopologyNodeRelocationRequestTest, testFindingIncrementalUpstreamAndDownstream) {
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                  topology,
                                                                  globalExecutionPlan,
                                                                  queryCatalogService,
                                                                  globalQueryPlan,
                                                                  sourceCatalog,
                                                                  udfCatalog);

    auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    WorkerId workerIdCounter = 1;
    const SharedQueryId sharedQueryId = 1;
    const uint64_t version = 0;
    DecomposedQueryPlanId subPlanId = 1;
    Network::NodeLocation sinkLocation;
    Network::NodeLocation sourceLocation;
    SourceDescriptorPtr networkSourceDescriptor;
    SourceLogicalOperatorNodePtr sourceLogicalOperatorNode;
    SinkDescriptorPtr networkSinkDescriptor;
    SinkLogicalOperatorNodePtr sinkLogicalOperatorNode;
    Optimizer::ExecutionNodePtr executionNode;
    LogicalUnaryOperatorNodePtr unaryOperatorNode;
    LogicalBinaryOperatorNodePtr binaryOperatorNode;
    auto pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));

    int64_t restPort = 123;
    int64_t dataPort = 124;
    std::string workerAddress = "localhost";
    std::string outputFileName = "dummy.out";
    std::string inputFileName = "dummy.in";
    DecomposedQueryPlanPtr subPlan;

    //root node
    //id = 1
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->setRootTopologyNodeId(workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 2
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(1, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 3
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 4
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 5
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 6
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(4, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 7
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(5, workerIdCounter);
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 8
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 9
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);

    std::cout << topology->toString() << std::endl;

    auto innerSharedQueryPlan = QueryPlan::create();

    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    auto fileSinkOperatorId = getNextOperatorId();
    auto fileSinkDescriptor = FileSinkDescriptor::create(outputFileName, "CSV_FORMAT", "APPEND");
    auto fileSinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkOperatorId);
    WorkerId pinnedId = 1;
    fileSinkOperatorNode->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    subPlan->addRootOperator(fileSinkOperatorNode);
    auto networkSourceId = getNextOperatorId();
    auto nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    auto networkSinkHostWorkerId = 2;
    auto uniqueId = 1;
    networkSourceDescriptor =
        Network::NetworkSourceDescriptor::create(schema,
                                                 nesPartition,
                                                 Network::NodeLocation(networkSinkHostWorkerId, workerAddress, dataPort),
                                                 WAIT_TIME,
                                                 EVENT_CHANNEL_RETRY_TIMES,
                                                 version,
                                                 uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{1});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    fileSinkOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(1)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    auto networkSinkId = getNextOperatorId();
    //sub plan on node 2
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(1, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{1});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto unaryOperatorId = getNextOperatorId();
    unaryOperatorNode = LogicalOperatorFactory::createFilterOperator(pred1, unaryOperatorId);
    pinnedId = 2;
    auto copiedUnaryOperator = unaryOperatorNode->copy();
    copiedUnaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    innerSharedQueryPlan->addRootOperator(copiedUnaryOperator);
    sinkLogicalOperatorNode->addChild(unaryOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(3, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    unaryOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(2)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //test link
    auto linkedSinkSourcePairs = Experimental::findNetworkOperatorsForLink(sharedQueryId,
                                                                           globalExecutionPlan->getExecutionNodeById(2),
                                                                           globalExecutionPlan->getExecutionNodeById(1));
    ASSERT_EQ(linkedSinkSourcePairs.size(), 1);
    auto [upstreamSink, downstreamSource] = linkedSinkSourcePairs.front();
    ASSERT_EQ(upstreamSink, sinkLogicalOperatorNode);
    ASSERT_EQ(downstreamSource,
              globalExecutionPlan->getExecutionNodeById(1)
                  ->getAllDecomposedQueryPlans(sharedQueryId)
                  .front()
                  ->getSourceOperators()
                  .front());

    networkSinkId = getNextOperatorId();
    //sub plan on node 3
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(2, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto binaryOperatorId = getNextOperatorId();
    binaryOperatorNode = LogicalOperatorFactory::createUnionOperator(binaryOperatorId);
    pinnedId = 3;
    auto copiedBinaryOperator = binaryOperatorNode->copy();
    copiedBinaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedUnaryOperator->addChild(copiedBinaryOperator);
    sinkLogicalOperatorNode->addChild(binaryOperatorNode);
    //network source left
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto leftsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    leftsourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    leftsourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    binaryOperatorNode->addChild(leftsourceLogicalOperatorNode);
    //network source right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto rightsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    rightsourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    rightsourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    binaryOperatorNode->addChild(rightsourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(3)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //first sub plan on node 6
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(
        Network::NodeLocation(3, workerAddress, dataPort),
        leftsourceLogicalOperatorNode->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
        WAIT_TIME,
        DATA_CHANNEL_RETRY_TIMES,
        version,
        DEFAULT_NUMBER_OF_ORIGINS,
        networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(7, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    sinkLogicalOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(6)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 7
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto defaultSourceIdLeft = getNextOperatorId();
    auto csvSourceType = CSVSourceType::create("physicalName", "logicalName");
    auto defaultSourcedescriptor = CsvSourceDescriptor::create(schema, csvSourceType);
    auto defaultSourceLeft = std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, defaultSourceIdLeft);
    pinnedId = 7;
    auto copiedDefaultSourceLeft = defaultSourceLeft->copy();
    copiedDefaultSourceLeft->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceLeft);
    sinkLogicalOperatorNode->addChild(defaultSourceLeft);
    globalExecutionPlan->getExecutionNodeById(7)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //second sub plan on node 6
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(
        Network::NodeLocation(3, workerAddress, dataPort),
        rightsourceLogicalOperatorNode->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
        WAIT_TIME,
        DATA_CHANNEL_RETRY_TIMES,
        version,
        DEFAULT_NUMBER_OF_ORIGINS,
        networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(8, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    sinkLogicalOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(6)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto defaultSourceIdRight = getNextOperatorId();
    auto defaultSourceRight = std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, defaultSourceIdRight);
    pinnedId = 8;
    auto copiedDefaultSourceRight = defaultSourceRight->copy();
    copiedDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceRight);
    sinkLogicalOperatorNode->addChild(defaultSourceRight);
    globalExecutionPlan->getExecutionNodeById(8)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //additional operators on node 3
    //second network source on the right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(9, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto secondRightsourceLogicalOperatorNode =
        std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    secondRightsourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    secondRightsourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{20});
    binaryOperatorNode->addChild(secondRightsourceLogicalOperatorNode);

    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(3, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, 7);
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, 20);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto secondDefaultSourceIdRight = getNextOperatorId();
    auto secondDefaultSourceRight =
        std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, secondDefaultSourceIdRight);
    auto copiedSecondDefaultSourceRight = secondDefaultSourceRight->copy();
    copiedSecondDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedSecondDefaultSourceRight);
    sinkLogicalOperatorNode->addChild(secondDefaultSourceRight);
    globalExecutionPlan->getExecutionNodeById(9)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    auto sharedQueryPlan = SharedQueryPlan::create(innerSharedQueryPlan);

    auto [upstreamPinned, downStreamPinned] =
        Experimental::findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                               globalExecutionPlan->getExecutionNodeById(6),
                                                               globalExecutionPlan->getExecutionNodeById(3),
                                                               topology);
    ASSERT_EQ(upstreamPinned.size(), 3);
    ASSERT_TRUE(upstreamPinned.contains(13));
    ASSERT_TRUE(upstreamPinned.contains(17));
    ASSERT_TRUE(upstreamPinned.contains(20));
    ASSERT_EQ(downStreamPinned.size(), 1);
    ASSERT_TRUE(downStreamPinned.contains(4));
}

TEST_F(TopologyNodeRelocationRequestTest, testFindingIncrementalUpstreamAndDownstreamWithAdditionalNonSystemOperator) {
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                  topology,
                                                                  globalExecutionPlan,
                                                                  queryCatalogService,
                                                                  globalQueryPlan,
                                                                  sourceCatalog,
                                                                  udfCatalog);

    auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    WorkerId workerIdCounter = 1;
    const SharedQueryId sharedQueryId = 1;
    const uint64_t version = 0;
    DecomposedQueryPlanId subPlanId = 1;
    Network::NodeLocation sinkLocation;
    Network::NodeLocation sourceLocation;
    SourceDescriptorPtr networkSourceDescriptor;
    SourceLogicalOperatorNodePtr sourceLogicalOperatorNode;
    SinkDescriptorPtr networkSinkDescriptor;
    SinkLogicalOperatorNodePtr sinkLogicalOperatorNode;
    Optimizer::ExecutionNodePtr executionNode;
    LogicalUnaryOperatorNodePtr unaryOperatorNode;
    LogicalBinaryOperatorNodePtr binaryOperatorNode;
    auto pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));

    int64_t restPort = 123;
    int64_t dataPort = 124;
    std::string workerAddress = "localhost";
    std::string outputFileName = "dummy.out";
    std::string inputFileName = "dummy.in";
    DecomposedQueryPlanPtr subPlan;

    //root node
    //id = 1
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->setRootTopologyNodeId(workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 2
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(1, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 3
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 4
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 5
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 6
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(4, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 7
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(5, workerIdCounter);
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 8
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 9
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);

    std::cout << topology->toString() << std::endl;

    auto innerSharedQueryPlan = QueryPlan::create();

    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    auto fileSinkOperatorId = getNextOperatorId();
    auto fileSinkDescriptor = FileSinkDescriptor::create(outputFileName, "CSV_FORMAT", "APPEND");
    auto fileSinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkOperatorId);
    WorkerId pinnedId = 1;
    fileSinkOperatorNode->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    subPlan->addRootOperator(fileSinkOperatorNode);
    auto networkSourceId = getNextOperatorId();
    auto nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    auto networkSinkHostWorkerId = 2;
    auto uniqueId = 1;
    networkSourceDescriptor =
        Network::NetworkSourceDescriptor::create(schema,
                                                 nesPartition,
                                                 Network::NodeLocation(networkSinkHostWorkerId, workerAddress, dataPort),
                                                 WAIT_TIME,
                                                 EVENT_CHANNEL_RETRY_TIMES,
                                                 version,
                                                 uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{1});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    fileSinkOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(1)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    auto networkSinkId = getNextOperatorId();
    //sub plan on node 2
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(1, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{1});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto unaryOperatorId = getNextOperatorId();
    unaryOperatorNode = LogicalOperatorFactory::createFilterOperator(pred1, unaryOperatorId);
    pinnedId = 2;
    auto copiedUnaryOperator = unaryOperatorNode->copy();
    copiedUnaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    innerSharedQueryPlan->addRootOperator(copiedUnaryOperator);
    sinkLogicalOperatorNode->addChild(unaryOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(3, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    unaryOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(2)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //test link
    auto linkedSinkSourcePairs = Experimental::findNetworkOperatorsForLink(sharedQueryId,
                                                                           globalExecutionPlan->getExecutionNodeById(2),
                                                                           globalExecutionPlan->getExecutionNodeById(1));
    ASSERT_EQ(linkedSinkSourcePairs.size(), 1);
    auto [upstreamSink, downstreamSource] = linkedSinkSourcePairs.front();
    ASSERT_EQ(upstreamSink, sinkLogicalOperatorNode);
    ASSERT_EQ(downstreamSource,
              globalExecutionPlan->getExecutionNodeById(1)
                  ->getAllDecomposedQueryPlans(sharedQueryId)
                  .front()
                  ->getSourceOperators()
                  .front());

    networkSinkId = getNextOperatorId();
    //sub plan on node 3
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(2, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto binaryOperatorId = getNextOperatorId();
    binaryOperatorNode = LogicalOperatorFactory::createUnionOperator(binaryOperatorId);
    pinnedId = 3;
    auto copiedBinaryOperator = binaryOperatorNode->copy();
    copiedBinaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedUnaryOperator->addChild(copiedBinaryOperator);
    sinkLogicalOperatorNode->addChild(binaryOperatorNode);
    //network source left
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto leftsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    leftsourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    leftsourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    binaryOperatorNode->addChild(leftsourceLogicalOperatorNode);
    //network source right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto rightsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    rightsourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    rightsourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    binaryOperatorNode->addChild(rightsourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(3)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //first sub plan on node 6
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(
        Network::NodeLocation(3, workerAddress, dataPort),
        leftsourceLogicalOperatorNode->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
        WAIT_TIME,
        DATA_CHANNEL_RETRY_TIMES,
        version,
        DEFAULT_NUMBER_OF_ORIGINS,
        networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(7, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    sinkLogicalOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(6)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 7
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto defaultSourceIdLeft = getNextOperatorId();
    auto csvSourceType = CSVSourceType::create("physicalName", "logicalName");
    auto defaultSourcedescriptor = CsvSourceDescriptor::create(schema, csvSourceType);
    auto defaultSourceLeft = std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, defaultSourceIdLeft);
    pinnedId = 7;
    auto copiedDefaultSourceLeft = defaultSourceLeft->copy();
    copiedDefaultSourceLeft->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceLeft);
    sinkLogicalOperatorNode->addChild(defaultSourceLeft);
    globalExecutionPlan->getExecutionNodeById(7)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //second sub plan on node 6
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(
        Network::NodeLocation(3, workerAddress, dataPort),
        rightsourceLogicalOperatorNode->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
        WAIT_TIME,
        DATA_CHANNEL_RETRY_TIMES,
        version,
        DEFAULT_NUMBER_OF_ORIGINS,
        networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(8, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    sinkLogicalOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(6)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto defaultSourceIdRight = getNextOperatorId();
    auto defaultSourceRight = std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, defaultSourceIdRight);
    pinnedId = 8;
    auto copiedDefaultSourceRight = defaultSourceRight->copy();
    copiedDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceRight);
    sinkLogicalOperatorNode->addChild(defaultSourceRight);
    globalExecutionPlan->getExecutionNodeById(8)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //additional operators on node 3
    unaryOperatorId = getNextOperatorId();
    unaryOperatorNode = LogicalOperatorFactory::createFilterOperator(pred1, unaryOperatorId);
    pinnedId = 3;
    auto oldCopiedUnaryOperator = copiedUnaryOperator;
    copiedUnaryOperator = unaryOperatorNode->copy();
    copiedUnaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedUnaryOperator);
    binaryOperatorNode->addChild(unaryOperatorNode);
    //second network source on the right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(9, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto secondRightsourceLogicalOperatorNode =
        std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    secondRightsourceLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{unaryOperatorId});
    secondRightsourceLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{21});
    //binaryOperatorNode->addChild(secondRightsourceLogicalOperatorNode);
    unaryOperatorNode->addChild(secondRightsourceLogicalOperatorNode);

    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(3, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperatorNode->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, unaryOperatorId);
    sinkLogicalOperatorNode->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, 21);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto secondDefaultSourceIdRight = getNextOperatorId();
    auto secondDefaultSourceRight =
        std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, secondDefaultSourceIdRight);
    auto copiedSecondDefaultSourceRight = secondDefaultSourceRight->copy();
    pinnedId = 9;
    copiedSecondDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    //copiedBinaryOperator->addChild(copiedSecondDefaultSourceRight);
    copiedUnaryOperator->addChild(copiedSecondDefaultSourceRight);
    sinkLogicalOperatorNode->addChild(secondDefaultSourceRight);
    globalExecutionPlan->getExecutionNodeById(9)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    auto sharedQueryPlan = SharedQueryPlan::create(innerSharedQueryPlan);

    auto [upstreamPinned, downStreamPinned] =
        Experimental::findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                               globalExecutionPlan->getExecutionNodeById(6),
                                                               globalExecutionPlan->getExecutionNodeById(3),
                                                               topology);
    for (auto operatorId : upstreamPinned) {
        std::cout << "upstream pinned: " << operatorId << std::endl;
    }
    ASSERT_EQ(upstreamPinned.size(), 3);
    ASSERT_TRUE(upstreamPinned.contains(13));
    ASSERT_TRUE(upstreamPinned.contains(17));
    ASSERT_TRUE(upstreamPinned.contains(21));
    //ASSERT_TRUE(upstreamPinned.contains(unaryOperatorId));
    ASSERT_EQ(downStreamPinned.size(), 1);
    ASSERT_TRUE(downStreamPinned.contains(4));
}
}// namespace NES
