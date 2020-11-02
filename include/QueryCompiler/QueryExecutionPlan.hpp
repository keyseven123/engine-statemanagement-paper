#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DataSource.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <map>

namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

/**
 * @brief A running execution plan on a node engine.
 * This class is thread-safe.
 */
class QueryExecutionPlan {
  public:
    enum QueryExecutionPlanStatus {
        Created,
        Deployed,// Created->Deployed when calling setup()
        Running, // Deployed->Running when calling start()
        Finished,
        Stopped,// Running->Stopped when calling stop() and in Running state
        ErrorState,
        Invalid
    };

  protected:
    explicit QueryExecutionPlan(
        QueryId queryId,
        QuerySubPlanId querySubPlanId,
        std::vector<DataSourcePtr>&& sources,
        std::vector<DataSinkPtr>&& sinks,
        std::vector<PipelineStagePtr>&& stages,
        QueryManagerPtr&& queryManager,
        BufferManagerPtr&& bufferManager);

  public:
    virtual ~QueryExecutionPlan();

    /**
     * @brief Setup the query plan, e.g., instantiate state variables.
     */
    bool setup();

    /**
     * @brief Start the query plan, e.g., start window thread.
     */
    bool start();

    /**
     * @brief Stop the query plan and free all associated resources.
     */
    bool stop();

    QueryExecutionPlanStatus getStatus();

    /**
     * @brief Get data sources.
     */
    std::vector<DataSourcePtr> getSources() const;

    /**
     * @brief Get data sinks.
     */
    std::vector<DataSinkPtr> getSinks() const;

    /**
     * @brief Get i-th stage.
     */
    PipelineStagePtr getStage(size_t index) const;

    /**
     * @brief Gets number of pipeline stages.
     */
    uint32_t numberOfPipelineStages() {
        return stages.size();
    }

    QueryManagerPtr getQueryManager();

    BufferManagerPtr getBufferManager();

    void print();

    /**
     * @brief Get the query id
     * @return the query id
     */
    QueryId getQueryId();

    /**
     * @brief Get the query execution plan id
     * @return the query execution plan id
     */
    QuerySubPlanId getQuerySubPlanId() const;

  protected:
    const QueryId queryId;
    const QuerySubPlanId querySubPlanId;
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<PipelineStagePtr> stages;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    std::atomic<QueryExecutionPlanStatus> qepStatus;
};

}// namespace NES

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
