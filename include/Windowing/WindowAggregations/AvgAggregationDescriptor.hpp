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

#ifndef NES_AVG_HPP
#define NES_AVG_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES::Windowing {
/**
 * @brief
 * The AvgAggregationDescriptor aggregation calculates the avg over the window.
 */
class AvgAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
    * Factory method to creates a avg aggregation on a particular field.
    */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);

    /**
    * @brief Infers the stamp of the expression given the current schema.
    * @param SchemaPtr
    */
    void inferStamp(SchemaPtr schema) override;

    WindowAggregationPtr copy() override;

    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

  private:
    AvgAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    AvgAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);
};
}// namespace NES::Windowing
#endif//NES_AVG_HPP
