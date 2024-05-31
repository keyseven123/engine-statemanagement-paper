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

#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/FieldRenameExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {

ExpressionNodePtr getExpressionNodePtr(ExpressionItem& expressionItem) { return expressionItem.getExpressionNode(); }

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs) { return JoinOperatorBuilder::Join(subQueryRhs, *this); }

NES::Experimental::BatchJoinOperatorBuilder::Join Query::batchJoinWith(const Query& subQueryRhs) {
    return NES::Experimental::BatchJoinOperatorBuilder::Join(subQueryRhs, *this);
}

CrossJoinOperatorBuilder::CrossJoin Query::crossJoinWith(const Query& subQueryRhs) { return CrossJoinOperatorBuilder::CrossJoin(subQueryRhs, *this); }

CEPOperatorBuilder::And Query::andWith(const Query& subQueryRhs) { return CEPOperatorBuilder::And(subQueryRhs, *this); }

CEPOperatorBuilder::Seq Query::seqWith(const Query& subQueryRhs) { return CEPOperatorBuilder::Seq(subQueryRhs, *this); }

CEPOperatorBuilder::Times Query::times(const uint64_t minOccurrences, const uint64_t maxOccurrences) {
    return CEPOperatorBuilder::Times(minOccurrences, maxOccurrences, *this);
}

CEPOperatorBuilder::Times Query::times(const uint64_t maxOccurrences) { return CEPOperatorBuilder::Times(maxOccurrences, *this); }

CEPOperatorBuilder::Times Query::times() { return CEPOperatorBuilder::Times(*this); }

namespace CrossJoinOperatorBuilder {
    CrossJoin::CrossJoin(const Query& subQueryRhs, Query& originalQuery)
        : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}
    Query& CrossJoin::window(const Windowing::WindowTypePtr& windowType) {
        // return originalQuery.joinWith(subQueryRhs).where(onLeftKey).equalsTo(onRightKey).window(windowType);
        return originalQuery.crossJoinWith(subQueryRhs, windowType);
    }
}// namespace CrossJoinOperatorBuilder

namespace JoinOperatorBuilder {

JoinWhere Join::where(const ExpressionItem& onLeftKey) const { return JoinWhere(subQueryRhs, originalQuery, onLeftKey); }

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinCondition JoinWhere::equalsTo(const ExpressionItem& onRightKey) const {
    return JoinCondition(subQueryRhs, originalQuery, onLeftKey, onRightKey);
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, const ExpressionItem& onLeftKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onLeftKey(onLeftKey.getExpressionNode()) {}

Query& JoinCondition::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.joinWith(subQueryRhs, onLeftKey, onRightKey, windowType);//call original joinWith() function
}

JoinCondition::JoinCondition(const Query& subQueryRhs,
                             Query& originalQuery,
                             const ExpressionItem& onLeftKey,
                             const ExpressionItem& onRightKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onLeftKey(onLeftKey.getExpressionNode()),
      onRightKey(onRightKey.getExpressionNode()) {}

}// namespace JoinOperatorBuilder

namespace Experimental::BatchJoinOperatorBuilder {

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinWhere Join::where(const ExpressionItem& onProbeKey) const { return JoinWhere(subQueryRhs, originalQuery, onProbeKey); }

Query& JoinWhere::equalsTo(const ExpressionItem& onBuildKey) const {
    return originalQuery.batchJoinWith(subQueryRhs, onProbeKey, onBuildKey);
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, const ExpressionItem& onProbeKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onProbeKey(onProbeKey.getExpressionNode()) {}

}// namespace Experimental::BatchJoinOperatorBuilder

namespace CEPOperatorBuilder {

And::And(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to andWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    NES_DEBUG("Query: add name cepLeftKey {}", cepLeftKey);
    NES_DEBUG("Query: add name cepRightKey {}", cepRightKey);
    onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
    onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& And::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.andWith(subQueryRhs, onLeftKey, onRightKey, windowType);//call original andWith() function
}

Seq::Seq(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to seqWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
    onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& Seq::window(const Windowing::WindowTypePtr& windowType) const {
    NES_DEBUG("Sequence enters window function");
    auto timestamp = windowType->as<Windowing::TimeBasedWindowType>()
                         ->getTimeCharacteristic()
                         ->getField()
                         ->getName();// assume time-based windows
    std::string sourceNameLeft = originalQuery.getQueryPlan()->getSourceConsumed();
    std::string sourceNameRight = subQueryRhs.getQueryPlan()->getSourceConsumed();
    // to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
    // in case of composed streams on the right branch
    if (sourceNameRight.find("_") != std::string::npos) {
        // we find the most left source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameRight.find("_");
        uint64_t posEnd = sourceNameRight.find("_", posStart + 1);
        sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + timestamp;
    }// in case the right branch only contains 1 source we can just use it
    else {
        sourceNameRight = sourceNameRight + "$" + timestamp;
    }
    // in case of composed sources on the left branch
    if (sourceNameLeft.find("_") != std::string::npos) {
        // we find the most right source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameLeft.find_last_of("_");
        sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
    }// in case the left branch only contains 1 source we can just use it
    else {
        sourceNameLeft = sourceNameLeft + "$" + timestamp;
    }
    NES_DEBUG("ExpressionItem for Left Source {}", sourceNameLeft);
    NES_DEBUG("ExpressionItem for Right Source {}", sourceNameRight);
    return originalQuery.seqWith(subQueryRhs, onLeftKey, onRightKey, windowType)
        .filter(Attribute(sourceNameLeft) < Attribute(sourceNameRight));//call original seqWith() function
}

Times::Times(const uint64_t minOccurrences, const uint64_t maxOccurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(minOccurrences), maxOccurrences(maxOccurrences), bounded(true) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(const uint64_t occurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(occurrences), bounded(true) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(Query& originalQuery) : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(0), bounded(false) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Query& Times::window(const Windowing::WindowTypePtr& windowType) const {
    auto timestamp = windowType->as<Windowing::TimeBasedWindowType>()->getTimeCharacteristic()->getField()->getName();
    // if no min and max occurrence is defined, apply count without filter
    if (!bounded) {
        return originalQuery.window(windowType).apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)));
    } else {
        // if min and/or max occurrence are defined, apply count without filter
        if (maxOccurrences == 0) {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") >= minOccurrences);
        }

        if (minOccurrences == 0) {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") == maxOccurrences);
        }

        return originalQuery.window(windowType)
            .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
            .filter(Attribute("Count") >= minOccurrences && Attribute("Count") <= maxOccurrences);
    }

    return originalQuery;
}

}// namespace CEPOperatorBuilder

Query::Query(QueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan)) {}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& sourceName) {
    NES_DEBUG("Query: create new Query with source {}", sourceName);
    auto queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
    return Query(queryPlan);
}

Query& Query::buildStatistic(Windowing::WindowTypePtr window,
                             Statistic::WindowStatisticDescriptorPtr statisticDescriptor,
                             Statistic::StatisticMetricHash metricHash,
                             Statistic::SendingPolicyPtr sendingPolicy,
                             Statistic::TriggerConditionPtr triggerCondition) {
    this->queryPlan = QueryPlanBuilder::addStatisticBuildOperator(std::move(window),
                                                                  std::move(statisticDescriptor),
                                                                  metricHash,
                                                                  sendingPolicy,
                                                                  triggerCondition,
                                                                  this->queryPlan);
    return *this;
}

Query& Query::project(std::vector<ExpressionNodePtr> expressions) {
    NES_DEBUG("Query: add projection to query");
    this->queryPlan = QueryPlanBuilder::addProjection(expressions, this->queryPlan);
    return *this;
}

Query& Query::as(const std::string& newSourceName) {
    NES_DEBUG("Query: add rename operator to query");
    this->queryPlan = QueryPlanBuilder::addRename(newSourceName, this->queryPlan);
    return *this;
}

Query& Query::unionWith(const Query& subQuery) {
    NES_DEBUG("Query: unionWith the subQuery to current query");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQuery.getQueryPlan());
    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs,
                       ExpressionItem onLeftKey,
                       ExpressionItem onRightKey,
                       const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (INNER_JOIN) to Join Operator");
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan,
                                                subQueryRhs.getQueryPlan(),
                                                onLeftKey.getExpressionNode(),
                                                onRightKey.getExpressionNode(),
                                                windowType,
                                                joinType);
    return *this;
}

Query& Query::batchJoinWith(const Query& subQueryRhs, ExpressionItem onProbeKey, ExpressionItem onBuildKey) {
    NES_DEBUG("Query: add Batch Join Operator to Query");
    this->queryPlan = QueryPlanBuilder::addBatchJoin(this->queryPlan,
                                                     subQueryRhs.getQueryPlan(),
                                                     onProbeKey.getExpressionNode(),
                                                     onBuildKey.getExpressionNode());
    return *this;
}

Query& Query::crossJoinWith(const Query& subQueryRhs, const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (INNER_JOIN) to CrossJoin Operator");
    auto onLeftKey = FieldAccessExpressionNode::create(DataTypeFactory::createBoolean(), "left");
    auto onRightKey = FieldAccessExpressionNode::create(DataTypeFactory::createBoolean(), "right");
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan,
                                                subQueryRhs.getQueryPlan(),
                                                onLeftKey,
                                                onRightKey,
                                                windowType,
                                                joinType);
    return *this;
}

Query& Query::andWith(const Query& subQueryRhs,
                      ExpressionItem onLeftKey,
                      ExpressionItem onRightKey,
                      const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (CARTESIAN_PRODUCT) to AND Operator");
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan,
                                                subQueryRhs.getQueryPlan(),
                                                onLeftKey.getExpressionNode(),
                                                onRightKey.getExpressionNode(),
                                                windowType,
                                                joinType);
    return *this;
}

Query& Query::seqWith(const Query& subQueryRhs,
                      ExpressionItem onLeftKey,
                      ExpressionItem onRightKey,
                      const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (CARTESIAN_PRODUCT) to SEQ Operator");
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan,
                                                subQueryRhs.getQueryPlan(),
                                                onLeftKey.getExpressionNode(),
                                                onRightKey.getExpressionNode(),
                                                windowType,
                                                joinType);
    return *this;
}

Query& Query::orWith(const Query& subQueryRhs) {
    NES_DEBUG("Query: finally we translate the OR into a union OP ");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQueryRhs.getQueryPlan());
    return *this;
}

Query& Query::filter(const ExpressionNodePtr& filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    this->queryPlan = QueryPlanBuilder::addFilter(filterExpression, this->queryPlan);
    return *this;
}

Query& Query::limit(const uint64_t limit) {
    NES_DEBUG("Query: add limit operator to query");
    this->queryPlan = QueryPlanBuilder::addLimit(limit, this->queryPlan);
    return *this;
}

Query& Query::mapUDF(const Catalogs::UDF::UDFDescriptorPtr& descriptor) {
    NES_DEBUG("Query: add map java udf operator to query");
    this->queryPlan = QueryPlanBuilder::addMapUDF(descriptor, this->queryPlan);
    return *this;
}

Query& Query::flatMapUDF(const Catalogs::UDF::UDFDescriptorPtr& descriptor) {
    NES_DEBUG("Query: add flat map java udf operator to query");
    this->queryPlan = QueryPlanBuilder::addFlatMapUDF(descriptor, this->queryPlan);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr& mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    this->queryPlan = QueryPlanBuilder::addMap(mapExpression, this->queryPlan);
    return *this;
}

Query& Query::inferModel(const std::string model,
                         const std::initializer_list<ExpressionItem> inputFields,
                         const std::initializer_list<ExpressionItem> outputFields) {
    NES_DEBUG("Query: add map inferModel to query");
    auto inputFieldVector = std::vector(inputFields);
    auto outputFieldVector = std::vector(outputFields);
    std::vector<ExpressionNodePtr> inputFieldsPtr;
    std::vector<ExpressionNodePtr> outputFieldsPtr;
    for (const auto& inputField : inputFieldVector) {
        inputFieldsPtr.push_back(inputField.getExpressionNode());
    }
    for (const auto& outputField : outputFieldVector) {
        outputFieldsPtr.push_back(outputField.getExpressionNode());
    }

    OperatorPtr op = LogicalOperatorFactory::createInferModelOperator(model, inputFieldsPtr, outputFieldsPtr);
    NES_DEBUG("Query::inferModel: Current Operator: {}", op->toString());
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor, WorkerId workerId) {
    NES_DEBUG("Query: add sink operator to query");
    this->queryPlan = QueryPlanBuilder::addSink(this->queryPlan, sinkDescriptor, workerId);
    return *this;
}

Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    NES_DEBUG("Query: add assignWatermark operator to query");
    this->queryPlan = QueryPlanBuilder::assignWatermark(this->queryPlan, watermarkStrategyDescriptor);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() const { return queryPlan; }

}// namespace NES



// andwith test, crossjoin + test, times test
// future: issue mit dem seqwith multiple
