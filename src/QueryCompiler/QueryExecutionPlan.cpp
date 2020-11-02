#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

QueryExecutionPlan::QueryExecutionPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<DataSourcePtr>&& sources,
                                       std::vector<DataSinkPtr>&& sinks, std::vector<PipelineStagePtr>&& stages,
                                       QueryManagerPtr&& queryManager, BufferManagerPtr&& bufferManager)
    : queryId(queryId),
      querySubPlanId(querySubPlanId),
      sources(std::move(sources)),
      sinks(std::move(sinks)),
      stages(std::move(stages)),
      queryManager(std::move(queryManager)),
      bufferManager(std::move(bufferManager)),
      qepStatus(Created) {
}

QueryId QueryExecutionPlan::getQueryId() {
    return queryId;
}

QuerySubPlanId QueryExecutionPlan::getQuerySubPlanId() const {
    return querySubPlanId;
}

QueryExecutionPlan::~QueryExecutionPlan() {
    NES_DEBUG("destroy qep " << queryId << " " << querySubPlanId);
    NES_ASSERT(qepStatus.load() == Stopped || qepStatus.load() == ErrorState, "QueryPlan is created but not executing " << queryId);
    sources.clear();
    stages.clear();
}

bool QueryExecutionPlan::stop() {
    bool ret = true;
    //    NES_ASSERT(qepStatus == Running, "QueryPlan is created but not executing " << queryId << " " << querySubPlanId);

    auto expected = Running;
    if (qepStatus.compare_exchange_strong(expected, Stopped)) {
        NES_DEBUG("QueryExecutionPlan: stop " << queryId << " " << querySubPlanId);
        for (auto& stage : stages) {
            if (!stage->stop()) {
                NES_ERROR("QueryExecutionPlan: stop failed for stage " << stage);
                ret = false;
            }
        }
    }

    if (!ret) {
        qepStatus.store(ErrorState);
    }
    return ret;
}

QueryExecutionPlan::QueryExecutionPlanStatus QueryExecutionPlan::getStatus() {
    return qepStatus.load();
}

bool QueryExecutionPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup " << queryId << " " << querySubPlanId);
    auto expected = Created;
    if (qepStatus.compare_exchange_strong(expected, Deployed)) {
        for (auto& stage : stages) {
            if (!stage->setup(queryManager, bufferManager)) {
                NES_ERROR("QueryExecutionPlan: setup failed!" << queryId << " " << querySubPlanId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Created but was not");
    }
    return true;
}

bool QueryExecutionPlan::start() {
    NES_DEBUG("QueryExecutionPlan: start " << queryId << " " << querySubPlanId);
    auto expected = Deployed;
    if (qepStatus.compare_exchange_strong(expected, Running)) {
        for (auto& stage : stages) {
            if (!stage->start()) {
                NES_ERROR("QueryExecutionPlan: start failed! " << queryId << " " << querySubPlanId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Deployed but was not");
    }
    return true;
}

QueryManagerPtr QueryExecutionPlan::getQueryManager() {
    return queryManager;
}

BufferManagerPtr QueryExecutionPlan::getBufferManager() {
    return bufferManager;
}

std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

PipelineStagePtr QueryExecutionPlan::getStage(size_t index) const {
    return stages[index];
}

void QueryExecutionPlan::print() {
    for (auto source : sources) {
        NES_INFO("Source:" << source);
        NES_INFO("\t Generated Buffers=" << source->getNumberOfGeneratedBuffers());
        NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples());
        NES_INFO("\t Schema=" << source->getSourceSchemaAsString());
    }
    for (auto sink : sinks) {
        NES_INFO("Sink:" << sink);
        NES_INFO("\t Generated Buffers=" << sink->getNumberOfWrittenOutBuffers());
        NES_INFO("\t Generated Tuples=" << sink->getNumberOfWrittenOutTuples());
    }
}

}// namespace NES
