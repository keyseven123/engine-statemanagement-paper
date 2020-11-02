#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMapOperator.hpp>

namespace NES {

void GeneratableMapOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableMapOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto field = getMapExpression()->getField();
    auto assignment = getMapExpression()->getAssignment();
    // todo remove if code gen can handle expressions
    auto mapExpression = TranslateToLegacyPlanPhase::create()->transformExpression(assignment);
    codegen->generateCodeForMap(AttributeField::create(field->getFieldName(), field->getStamp()), mapExpression, context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

GeneratableMapOperatorPtr GeneratableMapOperator::create(MapLogicalOperatorNodePtr mapLogicalOperator, OperatorId id) {
    return std::make_shared<GeneratableMapOperator>(GeneratableMapOperator(mapLogicalOperator->getMapExpression(), id));
}

GeneratableMapOperator::GeneratableMapOperator(FieldAssignmentExpressionNodePtr mapExpression, OperatorId id) : MapLogicalOperatorNode(mapExpression, id) {
}

const std::string GeneratableMapOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_MAP(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES