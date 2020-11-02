#include <API/Pattern.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <iostream>
#include <utility>

namespace NES {

Pattern::Pattern(const Pattern& query) : Query(query.queryPlan) {
    NES_DEBUG("Pattern: copy constructor: handover Pattern to Query");
}

Pattern::Pattern(QueryPlanPtr queryPlan) : Query(queryPlan) {
    NES_DEBUG("Pattern: copy constructor: handover Pattern to Query");
}

Pattern Pattern::from(const std::string sourceStreamName) {
    NES_DEBUG("Pattern: create query for input stream " << sourceStreamName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(sourceStreamName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    return Pattern(queryPlan);
}

Pattern& Pattern::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Pattern: enter sink function");
    NES_DEBUG("Pattern: add additional map operator to pattern");
    //TODO: replace '1'  with patternId or name after strings are supported by map
    this->map(Attribute("PatternId") = 1);
    NES_DEBUG("Pattern: add sink operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

const std::string& Pattern::getPatternName() const {
    return patternName;
}
void Pattern::setPatternName(const std::string& patternName) {
    this->patternName = patternName;
}

}// namespace NES
