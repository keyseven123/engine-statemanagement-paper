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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATEDQUERYEXECUTIONPLANBUILDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATEDQUERYEXECUTIONPLANBUILDER_HPP_

#include <Common/ForwardDeclaration.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <map>
#include <memory>
#include <vector>

namespace NES {

/**
 * @brief This GeneratedQueryExecutionPlanBuilder is a mutable object that allows constructing
 * immutable ExecutableQueryPlan using the builder pattern.
 */
class GeneratedQueryExecutionPlanBuilder {
  public:
    /**
     * @brief Creates and returns an empty qep builder
     */
    static GeneratedQueryExecutionPlanBuilder create();

    /**
     * @brief add pipeline stage to plan
     * @param pipelineStagePtr
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addPipeline(NodeEngine::Execution::ExecutablePipelinePtr pipeline);

    /**
     * @brief configure buffer manager
     * @param bufferManager
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setBufferManager(NodeEngine::BufferManagerPtr bufferManager);

    /**
     * @brief returns currently set buffer manager for the builder
     * @return currently set buffer manager for the builder
     */
    NodeEngine::BufferManagerPtr getBufferManager() const;

    /**
     * @brief configure query manager
     * @param queryManager
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setQueryManager(NodeEngine::QueryManagerPtr queryManager);

    /**
     * @return currently set query manager for the builder
     */
    NodeEngine::QueryManagerPtr getQueryManager() const;

    /**
     * @return total number of pipeline stages in the qep
     */
    uint64_t getNumberOfPipelineStages() const;

    /**
     * @param index
     * @return the index-th data source
     */
    DataSourcePtr getSource(uint64_t index);

    /**
    * @param index
    * @return the index-th data sink
    */
    DataSinkPtr getSink(uint64_t index);

    /**
     * @return all sinks in the plan
     */
    std::vector<DataSinkPtr>& getSinks();

    /**
     * @return the query execution plan id
     */
    [[nodiscard]] QueryId getQueryId() const;

    /**
     * @return a query execution plan with the specified configuration
     */
    NodeEngine::Execution::ExecutableQueryPlanPtr build();

    /**
     * @brief configure query compiler
     * @param queryCompiler
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setCompiler(QueryCompilerPtr queryCompiler);

    /**
     * @brief configure the query execution plan id
     * @param queryId
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setQueryId(QueryId queryId);

    /**
     * @brief add operator to the plan (check compiler/code generator documentation)
     * @param operatorPtr
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addOperatorQueryPlan(OperatorNodePtr operatorPtr);

    /**
     * @brief add data source
     * @param source
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addSource(DataSourcePtr source);

    /**
     * @brief add data sink
     * @param sink
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addSink(DataSinkPtr sink);

    /**
     * @brief Add query execution id
     * @param querySubPlanId : the input query execution id
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setQuerySubPlanId(QuerySubPlanId querySubPlanId);

    /**
     * @brief Get the query execution plan id
     * @return the query execution plan id
     */
    QuerySubPlanId getQuerySubPlanId() const;

  private:
    GeneratedQueryExecutionPlanBuilder();

    QueryId queryId;
    QuerySubPlanId querySubPlanId;
    NodeEngine::BufferManagerPtr bufferManager;
    NodeEngine::QueryManagerPtr queryManager;
    QueryCompilerPtr queryCompiler;
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<NodeEngine::Execution::ExecutablePipelinePtr> pipelines;
    std::vector<OperatorNodePtr> leaves;
};
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_GENERATEDQUERYEXECUTIONPLANBUILDER_HPP_
