#ifndef NES_MIN_HPP
#define NES_MIN_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES::Windowing {

/**
 * @brief
 * The MinAggregationDescriptor aggregation calculates the minimum over the window.
 */
class MinAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
   * Factory method to creates a MinAggregationDescriptor aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);

    /*
     * @brief generate the code for lift and combine of MinAggregationDescriptor SumAggregationDescriptor aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    Type getType() override;

    /**
 * @brief Infers the stamp of the expression given the current schema.
 * @param SchemaPtr
 */
    void inferStamp(SchemaPtr schema) override;
    WindowAggregationPtr copy() override;

  private:
    MinAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    MinAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);
};
}// namespace NES::Windowing
#endif//NES_MIN_HPP
