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

#include "gtest/gtest.h"

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Operators/OperatorNode.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>
#include <iostream>
#include <utility>

#include "../../util/DummySink.hpp"
#include "../../util/SchemaSourceDescriptor.hpp"
#include "../../util/TestQuery.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Topology/TopologyNode.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Sources/YSBSource.hpp>

#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/ReconfigurationType.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>

#include <vector>

using namespace NES;
using NodeEngine::MemoryLayoutPtr;
using NodeEngine::TupleBuffer;

class QueryExecutionTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup QueryCatalogTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() {
        // create test input buffer
        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test class."); }

    SchemaPtr testSchema;
};

/**
 * @brief A window source, which generates data consisting of (key, value, ts).
 * Key = 1||2
 * Value = 1
 * Ts = #Iteration
 */
class WindowSource : public NES::DefaultSource {
  public:
    uint64_t runCnt = 0;
    int64_t timestamp;
    bool varyWatermark;
    bool decreaseTime;
    WindowSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                 const uint64_t numbersOfBufferToProduce, uint64_t frequency, bool varyWatermark, bool decreaseTime,
                 int64_t timestamp)
        : DefaultSource(std::move(schema), std::move(bufferManager), std::move(queryManager), numbersOfBufferToProduce, frequency,
                        1),
          varyWatermark(varyWatermark), decreaseTime(decreaseTime), timestamp(timestamp) {}

    std::optional<TupleBuffer> receiveData() override {
        auto buffer = bufferManager->getBufferBlocking();
        auto rowLayout = NodeEngine::createRowLayout(schema);

        for (int i = 0; i < 10; i++) {
            rowLayout->getValueField<int64_t>(i, 0)->write(buffer, 1);
            rowLayout->getValueField<int64_t>(i, 1)->write(buffer, 1);
            if (varyWatermark) {
                if (!decreaseTime) {
                    rowLayout->getValueField<uint64_t>(i, 2)->write(buffer, timestamp++);
                } else {
                    if (runCnt == 0) {
                        /**
                         *in this run, we create normal tuples and one tuples that triggers a very large watermark
                         * first buffer
                         * |key:INT64|value:INT64|ts:UINT64|
                            +----------------------------------------------------+
                            |1|1|30|
                            |1|1|31|
                            |1|1|32|
                            |1|1|33|
                            |1|1|34|
                            |1|1|35|
                            |1|1|36|
                            |1|1|37|
                            |1|1|38|
                            |1|1|59|
                            +----------------------------------------------------+
                         */
                        if (i < 9) {
                            rowLayout->getValueField<uint64_t>(i, 2)->write(buffer, timestamp++);
                        } else {
                            rowLayout->getValueField<uint64_t>(i, 2)->write(buffer, timestamp + 20);
                        }
                    } else {
                        /**
                         * in this run we add ts below the current watermark to see if they are part of the result
                         * |key:INT64|value:INT64|ts:UINT64|
                            +----------------------------------------------------+
                            |1|1|48|
                            |1|1|47|
                            |1|1|46|
                            |1|1|45|
                            |1|1|44|
                            |1|1|43|
                            |1|1|42|
                            |1|1|41|
                            |1|1|40|
                            |1|1|39|
                            +----------------------------------------------------+
                         */
                        timestamp = timestamp - 1 <= 0 ? 0 : timestamp - 1;
                        rowLayout->getValueField<uint64_t>(i, 2)->write(buffer, timestamp);
                    }
                }
            } else {
                rowLayout->getValueField<uint64_t>(i, 2)->write(buffer, timestamp);
            }
        }
        buffer.setNumberOfTuples(10);
        timestamp = timestamp + 10;
        runCnt++;

        NES_DEBUG("QueryExecutionTest: source buffer=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, schema));
        return buffer;
    };

    static DataSourcePtr create(NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                                const uint64_t numbersOfBufferToProduce, uint64_t frequency, const bool varyWatermark = false,
                                bool decreaseTime = false, int64_t timestamp = 5) {
        auto windowSchema = Schema::create()
                                ->addField("test$key", BasicType::INT64)
                                ->addField("test$value", BasicType::INT64)
                                ->addField("test$ts", BasicType::UINT64);
        return std::make_shared<WindowSource>(windowSchema, bufferManager, queryManager, numbersOfBufferToProduce, frequency,
                                              varyWatermark, decreaseTime, timestamp);
    }
};

typedef std::shared_ptr<DefaultSource> DefaultSourcePtr;

class TestSink : public NES::SinkMedium {
  public:
    TestSink(uint64_t expectedBuffer, SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0, 1), expectedBuffer(expectedBuffer){};

    static std::shared_ptr<TestSink> create(uint64_t expectedBuffer, SchemaPtr schema,
                                            NodeEngine::BufferManagerPtr bufferManager) {
        return std::make_shared<TestSink>(expectedBuffer, schema, bufferManager);
    }

    bool writeData(TupleBuffer& input_buffer, NodeEngine::WorkerContext&) override {
        std::unique_lock lock(m);
        NES_DEBUG("QueryExecutionTest: TestSink: got buffer " << input_buffer);
        NES_DEBUG("QueryExecutionTest: PrettyPrintTupleBuffer"
                  << UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchemaPtr()));

        resultBuffers.emplace_back(input_buffer);
        if (resultBuffers.size() == expectedBuffer) {
            completed.set_value(true);
        } else if (resultBuffers.size() > expectedBuffer) {
            EXPECT_TRUE(false);
        }
        return true;
    }

    /**
     * @brief Factory to create a new TestSink.
     * @param expectedBuffer number of buffers expected this sink should receive.
     * @return
     */

    TupleBuffer& get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override { return ""; }

    void setup() override{};

    std::string toString() { return "Test_Sink"; }

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~TestSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

    SinkMediumTypes getSinkMediumType() { return SinkMediumTypes::PRINT_SINK; }

  private:
    void cleanupBuffers() {
        for (auto& buffer : resultBuffers) {
            buffer.release();
        }
        resultBuffers.clear();
    }

    std::mutex m;
    uint64_t expectedBuffer;

  public:
    std::promise<bool> completed;
    std::vector<TupleBuffer> resultBuffers;
};

void fillBuffer(TupleBuffer& buf, MemoryLayoutPtr memoryLayout) {
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->write(buf, recordIndex);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 1)->write(buf, 1);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 2)->write(buf, recordIndex % 2);
    }
    buf.setNumberOfTuples(10);
}

TEST_F(QueryExecutionTest, filterQuery) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager(), 1);

    auto query = TestQuery::from(testSource->getSchema()).filter(Attribute("id") < 5).sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);
    auto testSink = std::make_shared<TestSink>(10, testSchema, nodeEngine->getBufferManager());

    auto plan = GeneratedQueryExecutionPlanBuilder::create()
                    .addSink(testSink)
                    .addSource(testSource)
                    .addOperatorQueryPlan(generatableOperators)
                    .setCompiler(nodeEngine->getCompiler())
                    .setBufferManager(nodeEngine->getBufferManager())
                    .setQueryManager(nodeEngine->getQueryManager())
                    .setQueryId(1)
                    .setQuerySubPlanId(1)
                    .build();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getNumberOfPipelines(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = NodeEngine::createRowLayout(testSchema);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Running);
    NodeEngine::WorkerContext workerContext{1};
    plan->getPipeline(0)->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto& resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);

    for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
        EXPECT_EQ(memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(buffer), recordIndex);
    }
    testSink->shutdown();
    plan->stop();
    nodeEngine->stop();
}

TEST_F(QueryExecutionTest, projectionQuery) {
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf);

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager(), 1);

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto query = TestQuery::from(testSource->getSchema()).project(Attribute("id")).sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());

    std::cout << "plan=" << queryPlan->toString() << std::endl;
    auto plan = GeneratedQueryExecutionPlanBuilder::create()
                    .addSink(testSink)
                    .addSource(testSource)
                    .addOperatorQueryPlan(generatableOperators)
                    .setCompiler(nodeEngine->getCompiler())
                    .setBufferManager(nodeEngine->getBufferManager())
                    .setQueryManager(nodeEngine->getQueryManager())
                    .setQueryId(1)
                    .setQuerySubPlanId(1)
                    .build();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getNumberOfPipelines(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = NodeEngine::createRowLayout(testSchema);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Running);
    NodeEngine::WorkerContext workerContext{1};
    plan->getPipeline(0)->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto& resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultLayout = NodeEngine::createRowLayout(outputSchema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        // id
        EXPECT_EQ(resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer), recordIndex);
    }
    testSink->shutdown();
    plan->stop();
    nodeEngine->stop();
}

/**
 * @brief This test verify that the watermark assigned correctly.
 * WindowSource -> WatermarkAssignerOperator -> TestSink
 */
TEST_F(QueryExecutionTest, DISABLED_watermarkAssignerTest) {
    uint64_t millisecondOfallowedLateness = 8; /*second of allowedLateness*/

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource = WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 1,
                                             /*frequency*/ 1, /*varyWatermark*/ true);

    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(12));
    query = query.assignWatermark(EventTimeWatermarkStrategyDescriptor::create(
        Attribute("ts"), Milliseconds(millisecondOfallowedLateness), Milliseconds()));

    query = query.windowByKey(Attribute("key", INT64), windowType, Sum(Attribute("value", INT64)));
    // add a watermark assigner operator with allowedLateness of 1 millisecond

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan=" << queryPlan->toString() << std::endl;

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    auto& resultBuffer = testSink->get(0);

    EXPECT_EQ(resultBuffer.getWatermark(), 15);
    nodeEngine->stop();
}

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 * The source generates 2. buffers.
 */
TEST_F(QueryExecutionTest, tumblingWindowQueryTest) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource =
        WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 2, /*frequency*/ 1);

    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(10));

    query = query.windowByKey(Attribute("key", INT64), windowType, Sum(Attribute("value", INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("_$start", UINT64))
                                  ->addField(createField("_$end", UINT64))
                                  ->addField(createField("test$key", INT64))
                                  ->addField("test$value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan=" << queryPlan->toString() << std::endl;

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto& resultBuffer = testSink->get(0);

    NES_DEBUG("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    //TODO 1 Tuple im result buffer in 312 2 results?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);
    auto resultLayout = NodeEngine::createRowLayout(windowResultSchema);
    for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
        // start
        EXPECT_EQ(resultLayout->getValueField<uint64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer), 0);
        // end
        EXPECT_EQ(resultLayout->getValueField<uint64_t>(recordIndex, /*fieldIndex*/ 1)->read(resultBuffer), 10);
        // key
        EXPECT_EQ(resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 2)->read(resultBuffer), 1);
        // value
        EXPECT_EQ(resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 3)->read(resultBuffer), 10);
    }
    // nodeEngine->stopQuery(1);
    nodeEngine->stop();
}

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 * The source generates 2. buffers.
 */
TEST_F(QueryExecutionTest, tumblingWindowQueryTestWithOutOfOrderBuffer) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource = WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 2,
                                             /*frequency*/ 1, true, true, 30);

    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10));

    query = query.windowByKey(Attribute("key", INT64), windowType, Sum(Attribute("value", INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan=" << queryPlan->toString() << std::endl;

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto& resultBuffer = testSink->get(0);
    NES_DEBUG("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    //TODO 1 Tuple im result buffer in 312 2 results?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);
    auto resultLayout = NodeEngine::createRowLayout(windowResultSchema);
    for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
        // start
        EXPECT_EQ(resultLayout->getValueField<uint64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer), 30);
        // end
        EXPECT_EQ(resultLayout->getValueField<uint64_t>(recordIndex, /*fieldIndex*/ 1)->read(resultBuffer), 40);
        // key
        EXPECT_EQ(resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 2)->read(resultBuffer), 1);
        // value
        EXPECT_EQ(resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 3)->read(resultBuffer), 9);
    }
    nodeEngine->stopQuery(1);
    nodeEngine->stop();
}

TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourcesize10slide5) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource =
        WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 2, /*frequency*/ 1);

    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(10), Milliseconds(5));

    auto aggregation = Sum(Attribute("value"));
    query = query.windowByKey(Attribute("key"), windowType, aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    NES_DEBUG(" QueryExecutionTest: plan=" << queryPlan->toString());

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");
    // get result buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto& resultBuffer = testSink->get(0);

    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer.getNumberOfTuples() << " tuples.");
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|0|10|1|10|\n"
                                  "|5|15|1|10|\n"
                                  "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    nodeEngine->stopQuery(1);
    nodeEngine->stop();
}

TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourceSize15Slide5) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource =
        WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 3, /*frequency*/ 0);

    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(15), Milliseconds(5));

    auto aggregation = Sum(Attribute("value"));
    query = query.windowByKey(Attribute("key"), windowType, aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    NES_DEBUG("QueryExecutionTest: plan=" << queryPlan->toString());

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");
    // get result buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto& resultBuffer = testSink->get(0);

    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer.getNumberOfTuples() << " tuples.");
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 3);
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|0|15|1|10|\n"
                                  "|5|20|1|20|\n"
                                  "|10|25|1|10|\n"
                                  "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    nodeEngine->stopQuery(1);
    nodeEngine->stop();
}

/*
 * In this test we use a slide size that is equivalent to the slice size, to test if slicing works proper for small slides as well
 */
TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourcesize4slide2) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource =
        WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 2, /*frequency*/ 0);

    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    auto aggregation = Sum(Attribute("value"));
    query = query.windowByKey(Attribute("key"), windowType, aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    NES_DEBUG(" QueryExecutionTest: plan=" << queryPlan->toString());

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");
    // get result buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto& resultBuffer = testSink->get(0);

    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer.getNumberOfTuples() << " tuples.");
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|2|6|1|10|\n"
                                  "|4|8|1|10|\n"
                                  "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    nodeEngine->stopQuery(1);
    nodeEngine->stop();
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(QueryExecutionTest, mergeQuery) {
    // created buffer per source * number of sources
    uint64_t expectedBuf = 20;

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    auto testSource1 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager(), 1);

    auto query1 = TestQuery::from(testSource1->getSchema());

    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    auto testSource2 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager(), 1);
    auto query2 = TestQuery::from(testSource2->getSchema()).filter(Attribute("id") <= 5);

    // creating P3
    // merge does not change schema
    SchemaPtr ptr = std::make_shared<Schema>(testSchema);
    auto mergedQuery = query2.unionWith(&query1).sink(DummySink::create());

    auto testSink = std::make_shared<TestSink>(expectedBuf, testSchema, nodeEngine->getBufferManager());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .addOperatorQueryPlan(generatableOperators)
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(testSource1)
                       .addSource(testSource2)
                       .addSink(testSink);

    auto plan = builder.build();
    nodeEngine->getQueryManager()->registerQuery(plan);

    // The plan should have three pipeline
    EXPECT_EQ(plan->getNumberOfPipelines(), 3);

    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = NodeEngine::createRowLayout(testSchema);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    plan->setup();
    plan->start();
    NodeEngine::WorkerContext workerContext{1};
    auto stage_0 = plan->getPipeline(0);
    auto stage_1 = plan->getPipeline(1);
    for (int i = 0; i < 10; i++) {

        stage_0->execute(buffer, workerContext);// P1
        stage_1->execute(buffer, workerContext);// P2
        // Contfext -> Context 1 and Context 2;
        //
        // P1 -> P2 -> P3
        // P1 -> 10 tuples -> sum=10;
        // P2 -> 10 tuples -> sum=10;
        // P1 -> 10 tuples -> P2 -> sum =10;
        // P2 -> 20 tuples -> sum=20;
        // TODO why sleep here?
        sleep(1);
    }
    testSink->completed.get_future().get();
    plan->stop();

    auto& resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);// how to interpret this?

    for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
        EXPECT_EQ(memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer), recordIndex);
    }

    testSink->shutdown();
    testSource1->stop();
    testSource2->stop();
    nodeEngine->stop();
}

TEST_F(QueryExecutionTest, ysbQueryTest) {
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, PhysicalStreamConfig::createEmpty());
    int numBuf = 1;
    int numTup = 50;

    auto updatedYSBSchema = Schema::create()
                                ->addField("ysb$user_id", UINT64)
                                ->addField("ysb$page_id", UINT64)
                                ->addField("ysb$campaign_id", UINT64)
                                ->addField("ysb$ad_type", UINT64)
                                ->addField("ysb$event_type", UINT64)
                                ->addField("ysb$current_ms", UINT64)
                                ->addField("ysb$ip", UINT64)
                                ->addField("ysb$d1", UINT64)
                                ->addField("ysb$d2", UINT64)
                                ->addField("ysb$d3", UINT32)
                                ->addField("ysb$d4", UINT16);

    auto ysbSource =
        std::make_shared<YSBSource>(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), numBuf, numTup, 1, 1);

    ysbSource->getSchema()->clear();
    ysbSource->getSchema()->copyFields(updatedYSBSchema);

    //TODO: make query work
    auto query = TestQuery::from(ysbSource->getSchema())
                     .filter(Attribute("event_type") > 1)
                     .windowByKey(Attribute("campaign_id", INT64),
                                  TumblingWindow::of(EventTime(Attribute("current_ms")), Milliseconds(10)), Count())
                     .sink(DummySink::create());

    auto ysbResultSchema = Schema::create()
                               ->addField(createField("_$start", UINT64))
                               ->addField(createField("_$end", UINT64))
                               ->addField(createField("key", INT64))
                               ->addField("value", INT64);
    auto testSink = TestSink::create(/*expected result buffer*/ numBuf, ysbResultSchema, nodeEngine->getBufferManager());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(ysbSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    bool completed = testSink->completed.get_future().get();
    EXPECT_TRUE(completed);

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), numBuf);

    EXPECT_TRUE(!testSink->resultBuffers.empty());
    uint64_t noBufs = 0;
    for (auto buf : testSink->resultBuffers) {
        noBufs++;
        auto resultLayout = NodeEngine::createRowLayout(ysbResultSchema);
        for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
            auto campaignId = resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 2)->read(buf);
            EXPECT_TRUE(0 <= campaignId && campaignId < 10000);

            auto eventType = resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 3)->read(buf);
            EXPECT_TRUE(0 <= eventType && eventType < 3);
        }
    }
    EXPECT_EQ(noBufs, numBuf);

    nodeEngine->stopQuery(1);
    nodeEngine->stop();
}