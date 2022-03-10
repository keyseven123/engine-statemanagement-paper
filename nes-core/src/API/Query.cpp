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
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>
#include <numeric>

#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <utility>

namespace NES {

ExpressionNodePtr getExpressionNodePtr(ExpressionItem& expressionItem) { return expressionItem.getExpressionNode(); }

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs) { return JoinOperatorBuilder::Join(subQueryRhs, *this); }

CEPOperatorBuilder::And Query::andWith(const Query& subQueryRhs) { return CEPOperatorBuilder::And(subQueryRhs, *this); }

CEPOperatorBuilder::Seq Query::seqWith(const Query& subQueryRhs) { return CEPOperatorBuilder::Seq(subQueryRhs, *this); }

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

namespace CEPOperatorBuilder {

And::And(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to andWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    std::string cepLeftKey = keyAssignmentLeft();
    std::string cepRightKey = keyAssignmentRight();
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    NES_DEBUG("Query: add name cepLeftKey " << cepLeftKey);
    NES_DEBUG("Query: add name cepRightKey " << cepRightKey);
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
    std::string cepLeftKey = keyAssignmentLeft();
    std::string cepRightKey = keyAssignmentRight();
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
    onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& Seq::window(const Windowing::WindowTypePtr& windowType) const {
    NES_DEBUG("Sequence enters window function");
    auto timestamp = windowType->getTimeCharacteristic()->getField()->getName();
    std::string sourceNameLeft = originalQuery.getQueryPlan()->getSourceConsumed() + "$" + timestamp;
    std::string sourceNameRight = subQueryRhs.getQueryPlan()->getSourceConsumed() + "$" + timestamp;
    NES_DEBUG("ExpressionItem for Left source " << sourceNameLeft);
    NES_DEBUG("ExpressionItem for Right source " << sourceNameRight);
    return originalQuery.seqWith(subQueryRhs, onLeftKey, onRightKey, windowType)
        .filter(Attribute(sourceNameLeft) < Attribute(sourceNameRight));//call original seqWith() function
}

//TODO that is a quick fix to generate unique keys for andWith chains and should be removed after implementation of Cartesian Product (#2296)
std::string keyAssignmentRight() {
    //first, get unique ids for the key attributes
    auto cepRightId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepRightKey = "cep_rightkey" + std::to_string(cepRightId);
    return cepRightKey;
}

std::string keyAssignmentLeft() {
    //first, get unique ids for the key attributes
    auto cepLeftId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepLeftKey = "cep_leftkey" + std::to_string(cepLeftId);
    return cepLeftKey;
}
}// namespace CEPOperatorBuilder

Query::Query(QueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan)) {}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& sourceName) {
    NES_DEBUG("Query: create query for input source " << sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    queryPlan->setSourceConsumed(sourceName);
    return Query(queryPlan);
}

Query& Query::project(std::vector<ExpressionNodePtr> expressions) {
    OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::as(const std::string& newSourceName) {
    auto renameOperator = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlan->appendOperatorAsNewRoot(renameOperator);
    return *this;
}

Query& Query::unionWith(const Query& subQuery) {
    NES_DEBUG("Query: unionWith the subQuery to current query");
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    const QueryPlanPtr& subQueryPlan = subQuery.queryPlan;
    queryPlan->addRootOperator(subQueryPlan->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(subQueryPlan->getSourceConsumed());
    sourceNames.emplace_back(queryPlan->getSourceConsumed());
    std::sort(sourceNames.begin(), sourceNames.end());
    auto updatedSourceName = std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"));
    queryPlan->setSourceConsumed(updatedSourceName);
    return *this;
}

Query& Query::join(const Query& subQueryRhs,
                   ExpressionItem onLeftKey,
                   ExpressionItem onRightKey,
                   const Windowing::WindowTypePtr& windowType,
                   Join::LogicalJoinDefinition::JoinType joinType) {
    NES_DEBUG("Query: joinWith the subQuery to current query");

    auto subQuery = const_cast<Query&>(subQueryRhs);

    auto leftKeyExpression = onLeftKey.getExpressionNode();
    if (!leftKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + leftKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto rightKeyExpression = onRightKey.getExpressionNode();
    if (!rightKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + rightKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto leftKeyFieldAccess = leftKeyExpression->as<FieldAccessExpressionNode>();
    auto rightKeyFieldAccess = rightKeyExpression->as<FieldAccessExpressionNode>();

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    //    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

    auto rightQueryPlan = subQuery.getQueryPlan();
    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid right query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = getQueryPlan()->getRootOperators()[0]->getOutputSchema();
    auto rightJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            queryPlan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit())));
        }
    }

    if (rightQueryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            auto op = LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
            rightQueryPlan->appendOperatorAsNewRoot(op);
        } else if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            auto op =
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit()));
            rightQueryPlan->appendOperatorAsNewRoot(op);
        }
    }

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDefinition::create(leftKeyFieldAccess,
                                                              rightKeyFieldAccess,
                                                              windowType,
                                                              distrType,
                                                              triggerPolicy,
                                                              triggerAction,
                                                              1,
                                                              1,
                                                              joinType);

    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    queryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(rightQueryPlan->getSourceConsumed());
    sourceNames.emplace_back(queryPlan->getSourceConsumed());
    std::sort(sourceNames.begin(), sourceNames.end());
    auto updatedSourceName = std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"));
    queryPlan->setSourceConsumed(updatedSourceName);

    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs,
                       ExpressionItem onLeftKey,
                       ExpressionItem onRightKey,
                       const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType to Join Operator");

    Join::LogicalJoinDefinition::JoinType joinType = Join::LogicalJoinDefinition::INNER_JOIN;
    return Query::join(subQueryRhs, onLeftKey, onRightKey, windowType, joinType);
}

Query& Query::andWith(const Query& subQueryRhs,
                      ExpressionItem onLeftKey,
                      ExpressionItem onRightKey,
                      const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType to AND Operator");
    Join::LogicalJoinDefinition::JoinType joinType = Join::LogicalJoinDefinition::CARTESIAN_PRODUCT;
    return Query::join(subQueryRhs, onLeftKey, onRightKey, windowType, joinType);
}

Query& Query::seqWith(const Query& subQueryRhs,
                      ExpressionItem onLeftKey,
                      ExpressionItem onRightKey,
                      const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType to SEQ Operator");
    Join::LogicalJoinDefinition::JoinType joinType = Join::LogicalJoinDefinition::CARTESIAN_PRODUCT;
    return Query::join(subQueryRhs, onLeftKey, onRightKey, windowType, joinType);
}

Query& Query::orWith(const Query& subQueryRhs) {
    NES_DEBUG("Query: add map operator that add the original source name to the left and right side sources of the OR ");
    //get source names
    auto sourceNameLeft = this->getQueryPlan()->getSourceConsumed();
    auto subQuery = const_cast<Query&>(subQueryRhs);
    auto sourceNameRight = subQuery.getQueryPlan()->getSourceConsumed();
    auto maxLength = std::max(sourceNameRight.length(), sourceNameLeft.length());
    sourceNameLeft.resize(maxLength, '_');
    sourceNameRight.resize(maxLength, '_');
    //map the attributes with value sourceNameLeft and sourceNameRight to the left and right source
    this->map(Attribute("SourceName") = sourceNameLeft);
    subQuery.map(Attribute("SourceName") = sourceNameRight);
    NES_DEBUG("Query: finally we translate the OR into a union OP ");
    return Query::unionWith(subQuery);
}

Query& Query::filter(const ExpressionNodePtr& filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("Query: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::filter(const ExpressionNodePtr& filterExpression, float selectivity) {
    NES_DEBUG("Query: add filter operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression, selectivity);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr& mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("Query: Map expression cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::times(const uint64_t minOccurrences, const uint64_t maxOccurrences) {
    NES_DEBUG("Pattern: enter iteration function with (min, max)" << minOccurrences << "," << maxOccurrences);
    OperatorNodePtr op = LogicalOperatorFactory::createCEPIterationOperator(minOccurrences, maxOccurrences);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::inferModel(const std::string model, const std::initializer_list<ExpressionItem> inputFields, const std::initializer_list<ExpressionItem> outputFields) {
    NES_DEBUG("Query: add map inferModel to query");
    auto inputFieldVector = std::vector(inputFields);
    auto outputFieldVector = std::vector(outputFields);
    std::vector<ExpressionItemPtr> inputFieldsPtr;
    std::vector<ExpressionItemPtr> outputFieldsPtr;
    for(auto f : inputFieldVector){
        ExpressionItemPtr fp = std::make_shared<ExpressionItem>(f);
        inputFieldsPtr.push_back(fp);
    }
    for(auto f : outputFieldVector){
        ExpressionItemPtr fp = std::make_shared<ExpressionItem>(f);
        outputFieldsPtr.push_back(fp);
    }

    OperatorNodePtr op = LogicalOperatorFactory::createInferModelOperator(model, inputFieldsPtr, outputFieldsPtr);
    std::cout << op->toString() << std::endl;
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    NES_DEBUG("Query: add assignWatermark operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() const { return queryPlan; }

}// namespace NES
