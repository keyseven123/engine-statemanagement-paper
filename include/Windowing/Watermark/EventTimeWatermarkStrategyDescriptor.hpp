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

#ifndef NES_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP
#define NES_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP

#include <API/Expressions/Expressions.hpp>
#include <Windowing/Watermark/EventTimeWatermarkGenerator.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowMeasures/TimeUnit.hpp>
namespace NES::Windowing {

class EventTimeWatermarkStrategyDescriptor;
typedef std::shared_ptr<EventTimeWatermarkStrategyDescriptor> EventTimeWatermarkStrategyDescriptorPtr;

class EventTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor {
  public:
    static WatermarkStrategyDescriptorPtr create(ExpressionItem onField, TimeMeasure allowedLateness, TimeUnit unit);

    ExpressionItem getOnField();

    TimeMeasure getAllowedLateness();

    TimeUnit getTimeUnit();

    bool equal(WatermarkStrategyDescriptorPtr other) override;

    std::string toString() override;

    bool inferStamp(SchemaPtr schema) override;

  private:
    // Field where the watermark should be retrieved
    ExpressionItem onField;
    TimeUnit unit;
    TimeMeasure allowedLateness;

    explicit EventTimeWatermarkStrategyDescriptor(ExpressionItem onField, TimeMeasure allowedLateness, TimeUnit unit);
};

}// namespace NES::Windowing

#endif//NES_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP
