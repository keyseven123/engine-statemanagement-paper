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

#include <API/Schema.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>

#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SerializableOperator.pb.h>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>

#include <Windowing/WindowPolicies/OnBufferTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>

#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/BaseJoinActionDescriptor.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/SliceAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#ifdef ENABLE_OPC_BUILD
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#endif
#ifdef ENABLE_MQTT_BUILD
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#endif

namespace NES {
SerializableOperator* OperatorSerializationUtil::serializeOperator(OperatorNodePtr operatorNode,
                                                                   SerializableOperator* serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // serialize source operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SourceLogicalOperatorNode");
        auto sourceDetails = serializeSourceOperator(operatorNode->as<SourceLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(sourceDetails);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        // serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SinkLogicalOperatorNode");
        auto sinkDetails = serializeSinkOperator(operatorNode->as<SinkLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(sinkDetails);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        // serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: serialize to FilterLogicalOperatorNode");
        auto filterDetails = SerializableOperator_FilterDetails();
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        // serialize filter expression
        ExpressionSerializationUtil::serializeExpression(filterOperator->getPredicate(), filterDetails.mutable_predicate());
        serializedOperator->mutable_details()->PackFrom(filterDetails);
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        // serialize projection operator
        NES_TRACE("OperatorSerializationUtil:: serialize to ProjectionLogicalOperatorNode");
        auto projectionDetail = SerializableOperator_ProjectionDetails();
        auto projectionOperator = operatorNode->as<ProjectionLogicalOperatorNode>();
        for (auto& exp : projectionOperator->getExpressions()) {
            auto mutableExpression = projectionDetail.mutable_expression()->Add();
            ExpressionSerializationUtil::serializeExpression(exp, mutableExpression);
        }
        serializedOperator->mutable_details()->PackFrom(projectionDetail);
    } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        // serialize union operator
        NES_TRACE("OperatorSerializationUtil:: serialize to UnionLogicalOperatorNode");
        auto unionDetails = SerializableOperator_UnionDetails();
        serializedOperator->mutable_details()->PackFrom(unionDetails);
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        // serialize broadcast operator
        NES_TRACE("OperatorSerializationUtil:: serialize to BroadcastLogicalOperatorNode");
        auto broadcastDetails = SerializableOperator_BroadcastDetails();
        serializedOperator->mutable_details()->PackFrom(broadcastDetails);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        // serialize map operator
        NES_TRACE("OperatorSerializationUtil:: serialize to MapLogicalOperatorNode");
        auto mapDetails = SerializableOperator_MapDetails();
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        // serialize map expression
        ExpressionSerializationUtil::serializeExpression(mapOperator->getMapExpression(), mapDetails.mutable_expression());
        serializedOperator->mutable_details()->PackFrom(mapDetails);
    } else if (operatorNode->instanceOf<CentralWindowOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to CentralWindowOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<CentralWindowOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else if (operatorNode->instanceOf<SliceCreationOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SliceCreationOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<SliceCreationOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else if (operatorNode->instanceOf<SliceMergingOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SliceMergingOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<SliceMergingOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else if (operatorNode->instanceOf<WindowComputationOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to WindowComputationOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<WindowComputationOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to JoinLogicalOperatorNode");
        auto joinDetails = serializeJoinOperator(operatorNode->as<JoinLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(joinDetails);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        // serialize watermarkAssigner operator
        NES_TRACE("OperatorSerializationUtil:: serialize to WatermarkAssignerLogicalOperatorNode");
        auto watermarkAssignerDetail =
            serializeWatermarkAssignerOperator(operatorNode->as<WatermarkAssignerLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(watermarkAssignerDetail);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not serialize this operator: " << operatorNode->toString());
    }

    // serialize input schema
    if (!operatorNode->isBinaryOperator()) {
        if (operatorNode->isExchangeOperator()) {
            SchemaSerializationUtil::serializeSchema(operatorNode->as<ExchangeOperatorNode>()->getInputSchema(),
                                                     serializedOperator->mutable_inputschema());
        } else
            SchemaSerializationUtil::serializeSchema(operatorNode->as<UnaryOperatorNode>()->getInputSchema(),
                                                     serializedOperator->mutable_inputschema());
    } else {
        SchemaSerializationUtil::serializeSchema(operatorNode->as<BinaryOperatorNode>()->getLeftInputSchema(),
                                                 serializedOperator->mutable_leftinputschema());
        SchemaSerializationUtil::serializeSchema(operatorNode->as<BinaryOperatorNode>()->getRightInputSchema(),
                                                 serializedOperator->mutable_rightinputschema());
    }

    // serialize output schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator->mutable_outputschema());

    // serialize operator id
    serializedOperator->set_operatorid(operatorNode->getId());

    // serialize and append children if the node has any
    for (const auto& child : operatorNode->getChildren()) {
        auto serializedChild = serializedOperator->add_children();
        // serialize this child
        serializeOperator(child->as<OperatorNode>(), serializedChild);
    }

    NES_DEBUG("OperatorSerializationUtil:: serialize " << operatorNode->toString() << " to "
                                                       << serializedOperator->details().type_url());
    return serializedOperator;
}

OperatorNodePtr OperatorSerializationUtil::deserializeOperator(SerializableOperator* serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: de-serialize " << serializedOperator->DebugString());
    auto details = serializedOperator->details();
    LogicalOperatorNodePtr operatorNode;
    if (details.Is<SerializableOperator_SourceDetails>()) {
        // de-serialize source operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SourceLogicalOperator");
        auto serializedSourceDescriptor = SerializableOperator_SourceDetails();
        details.UnpackTo(&serializedSourceDescriptor);
        // de-serialize source descriptor
        auto sourceDescriptor = deserializeSourceDescriptor(&serializedSourceDescriptor);
        operatorNode = LogicalOperatorFactory::createSourceOperator(sourceDescriptor, serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_SinkDetails>()) {
        // de-serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SinkLogicalOperator");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails();
        details.UnpackTo(&serializedSinkDescriptor);
        // de-serialize sink descriptor
        auto sinkDescriptor = deserializeSinkDescriptor(&serializedSinkDescriptor);
        operatorNode = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_FilterDetails>()) {
        // de-serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to FilterLogicalOperator");
        auto serializedFilterOperator = SerializableOperator_FilterDetails();
        details.UnpackTo(&serializedFilterOperator);
        // de-serialize filter expression
        auto filterExpression = ExpressionSerializationUtil::deserializeExpression(serializedFilterOperator.mutable_predicate());
        operatorNode = LogicalOperatorFactory::createFilterOperator(filterExpression, serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_ProjectionDetails>()) {
        // de-serialize projection operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to ProjectionLogicalOperator");
        auto serializedProjectionOperator = SerializableOperator_ProjectionDetails();
        details.UnpackTo(&serializedProjectionOperator);

        std::vector<ExpressionNodePtr> exps;
        // serialize and append children if the node has any
        for (auto mutableExpression : *serializedProjectionOperator.mutable_expression()) {
            auto projectExpression = ExpressionSerializationUtil::deserializeExpression(&mutableExpression);
            exps.push_back(projectExpression);
        }
        operatorNode = LogicalOperatorFactory::createProjectionOperator(exps, serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_UnionDetails>()) {
        // de-serialize union operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to UnionLogicalOperator");
        auto serializedUnionDescriptor = SerializableOperator_UnionDetails();
        details.UnpackTo(&serializedUnionDescriptor);
        operatorNode = LogicalOperatorFactory::createUnionOperator(serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_BroadcastDetails>()) {
        // de-serialize broadcast operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to BroadcastLogicalOperator");
        auto serializedBroadcastDescriptor = SerializableOperator_BroadcastDetails();
        details.UnpackTo(&serializedBroadcastDescriptor);
        // de-serialize broadcast descriptor
        operatorNode = LogicalOperatorFactory::createBroadcastOperator(serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_MapDetails>()) {
        // de-serialize map operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to MapLogicalOperator");
        auto serializedMapOperator = SerializableOperator_MapDetails();
        details.UnpackTo(&serializedMapOperator);
        // de-serialize map expression
        auto fieldAssignmentExpression =
            ExpressionSerializationUtil::deserializeExpression(serializedMapOperator.mutable_expression());
        operatorNode = LogicalOperatorFactory::createMapOperator(fieldAssignmentExpression->as<FieldAssignmentExpressionNode>(),
                                                                 serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_WindowDetails>()) {
        // de-serialize window operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to WindowLogicalOperator");
        auto serializedWindowOperator = SerializableOperator_WindowDetails();
        details.UnpackTo(&serializedWindowOperator);
        operatorNode = deserializeWindowOperator(&serializedWindowOperator, serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_JoinDetails>()) {
        // de-serialize window operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to JoinLogicalOperator");
        auto serializedJoinOperator = SerializableOperator_JoinDetails();
        details.UnpackTo(&serializedJoinOperator);
        operatorNode = deserializeJoinOperator(&serializedJoinOperator, serializedOperator->operatorid());
    } else if (details.Is<SerializableOperator_WatermarkStrategyDetails>()) {
        // de-serialize watermark assigner operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to watermarkassigner operator");
        auto serializedWatermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
        details.UnpackTo(&serializedWatermarkStrategyDetails);
        auto watermarkStrategyDescriptor = deserializeWatermarkStrategyDescriptor(&serializedWatermarkStrategyDetails);
        operatorNode = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor,
                                                                               serializedOperator->operatorid());
    } else {
        NES_THROW_RUNTIME_ERROR("OperatorSerializationUtil: could not de-serialize this serialized operator: ");
    }

    // de-serialize operator output schema
    operatorNode->setOutputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_outputschema()));
    // de-serialize operator input schema
    if (!operatorNode->isBinaryOperator()) {
        if (operatorNode->isExchangeOperator()) {
            operatorNode->as<ExchangeOperatorNode>()->setInputSchema(
                SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_inputschema()));
        } else {
            operatorNode->as<UnaryOperatorNode>()->setInputSchema(
                SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_inputschema()));
        }
    } else {
        operatorNode->as<BinaryOperatorNode>()->setLeftInputSchema(
            SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_leftinputschema()));
        operatorNode->as<BinaryOperatorNode>()->setRightInputSchema(
            SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_rightinputschema()));
    }

    if (details.Is<SerializableOperator_JoinDetails>()) {
        auto joinOp = operatorNode->as<JoinLogicalOperatorNode>();
        joinOp->getJoinDefinition()->updateStreamTypes(joinOp->getLeftInputSchema(), joinOp->getRightInputSchema());
        joinOp->getJoinDefinition()->updateOutputDefinition(joinOp->getOutputSchema());
    }

    NES_TRACE("OperatorSerializationUtil:: de-serialize " << serializedOperator->DebugString() << " to "
                                                          << operatorNode->toString());
    return operatorNode;
}

SerializableOperator_WindowDetails OperatorSerializationUtil::serializeWindowOperator(WindowOperatorNodePtr windowOperator) {
    auto windowDetails = SerializableOperator_WindowDetails();
    auto windowDefinition = windowOperator->getWindowDefinition();

    if (windowDefinition->isKeyed()) {
        ExpressionSerializationUtil::serializeExpression(windowDefinition->getOnKey(), windowDetails.mutable_onkey());
    }
    windowDetails.set_numberofinputedges(windowDefinition->getNumberOfInputEdges());
    windowDetails.set_allowedlateness(windowDefinition->getAllowedLateness());
    auto windowType = windowDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_WindowDetails_TimeCharacteristic();
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_IngestionTime);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMultiplier());

    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        auto tumblingWindowDetails = SerializableOperator_WindowDetails_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        windowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
        auto slidingWindowDetails = SerializableOperator_WindowDetails_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        windowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    // serialize aggregation
    auto windowAggregation = windowDetails.mutable_windowaggregation();
    ExpressionSerializationUtil::serializeExpression(windowDefinition->getWindowAggregation()->as(),
                                                     windowAggregation->mutable_asfield());
    ExpressionSerializationUtil::serializeExpression(windowDefinition->getWindowAggregation()->on(),
                                                     windowAggregation->mutable_onfield());

    switch (windowDefinition->getWindowAggregation()->getType()) {
        case Windowing::WindowAggregationDescriptor::Count:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_COUNT);
            break;
        case Windowing::WindowAggregationDescriptor::Max:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MAX);
            break;
        case Windowing::WindowAggregationDescriptor::Min:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MIN);
            break;
        case Windowing::WindowAggregationDescriptor::Sum:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_SUM);
            break;
        default: NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
    }

    auto windowTrigger = windowDetails.mutable_triggerpolicy();

    switch (windowDefinition->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnTime: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnTime);
            Windowing::OnTimeTriggerDescriptionPtr triggerDesc =
                std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(windowDefinition->getTriggerPolicy());
            windowTrigger->set_timeinms(triggerDesc->getTriggerTimeInMs());
            break;
        }
        case Windowing::TriggerType::triggerOnRecord: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnRecord);
            break;
        }
        case Windowing::TriggerType::triggerOnBuffer: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnBuffer);
            break;
        }
        case Windowing::TriggerType::triggerOnWatermarkChange: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnWatermarkChange);
            break;
        }
        default: {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
        }
    }

    auto windowAction = windowDetails.mutable_action();
    switch (windowDefinition->getTriggerAction()->getActionType()) {
        case Windowing::ActionType::WindowAggregationTriggerAction: {
            windowAction->set_type(SerializableOperator_WindowDetails_TriggerAction_Type_Complete);
            break;
        }
        case Windowing::ActionType::SliceAggregationTriggerAction: {
            windowAction->set_type(SerializableOperator_WindowDetails_TriggerAction_Type_Slicing);
            break;
        }
        default: {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not cast action type");
        }
    }

    if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Complete) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete);
    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Combining) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining);
    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Slicing) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing);
    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Merging) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Merging);
    } else {
        NES_NOT_IMPLEMENTED();
    }
    return windowDetails;
}

SerializableOperator_JoinDetails OperatorSerializationUtil::serializeJoinOperator(JoinLogicalOperatorNodePtr joinOperator) {
    auto joinDetails = SerializableOperator_JoinDetails();
    auto joinDefinition = joinOperator->getJoinDefinition();

    ExpressionSerializationUtil::serializeExpression(joinDefinition->getLeftJoinKey(), joinDetails.mutable_onleftkey());
    ExpressionSerializationUtil::serializeExpression(joinDefinition->getRightJoinKey(), joinDetails.mutable_onrightkey());

    auto windowType = joinDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_JoinDetails_TimeCharacteristic();
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_JoinDetails_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_JoinDetails_TimeCharacteristic_Type_IngestionTime);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        auto tumblingWindowDetails = SerializableOperator_JoinDetails_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        joinDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
        auto slidingWindowDetails = SerializableOperator_JoinDetails_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        joinDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    auto windowTrigger = joinDetails.mutable_triggerpolicy();
    switch (joinDefinition->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnTime: {
            windowTrigger->set_type(SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnTime);
            Windowing::OnTimeTriggerDescriptionPtr triggerDesc =
                std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(joinDefinition->getTriggerPolicy());
            windowTrigger->set_timeinms(triggerDesc->getTriggerTimeInMs());
            break;
        }
        case Windowing::TriggerType::triggerOnRecord: {
            windowTrigger->set_type(SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnRecord);
            break;
        }
        case Windowing::TriggerType::triggerOnBuffer: {
            windowTrigger->set_type(SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnBuffer);
            break;
        }
        case Windowing::TriggerType::triggerOnWatermarkChange: {
            windowTrigger->set_type(SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnWatermarkChange);
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("OperatorSerializationUtil: could not cast aggregation type");
        }
    }

    auto windowAction = joinDetails.mutable_action();
    switch (joinDefinition->getTriggerAction()->getActionType()) {
        case Join::JoinActionType::LazyNestedLoopJoin: {
            windowAction->set_type(SerializableOperator_JoinDetails_TriggerAction_Type_LazyNestedLoop);
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("OperatorSerializationUtil: could not cast action type");
        }
    }

    if (joinDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Complete) {
        joinDetails.mutable_distrchar()->set_distr(
            SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Complete);
    } else if (joinDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Combining) {
        joinDetails.mutable_distrchar()->set_distr(
            SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Combining);
    } else if (joinDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Slicing) {
        joinDetails.mutable_distrchar()->set_distr(
            SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Slicing);
    } else if (joinDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Merging) {
        joinDetails.mutable_distrchar()->set_distr(
            SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Merging);
    } else {
        NES_NOT_IMPLEMENTED();
    }

    joinDetails.set_numberofinputedgesleft(joinDefinition->getNumberOfInputEdgesLeft());
    joinDetails.set_numberofinputedgesright(joinDefinition->getNumberOfInputEdgesRight());
    return joinDetails;
}

WindowOperatorNodePtr OperatorSerializationUtil::deserializeWindowOperator(SerializableOperator_WindowDetails* windowDetails,
                                                                           OperatorId operatorId) {
    auto serializedWindowAggregation = windowDetails->windowaggregation();
    auto serializedTriggerPolicy = windowDetails->triggerpolicy();
    auto serializedAction = windowDetails->action();

    auto serializedWindowType = windowDetails->windowtype();

    auto onField = ExpressionSerializationUtil::deserializeExpression(serializedWindowAggregation.mutable_onfield())
                       ->as<FieldAccessExpressionNode>();
    auto asField = ExpressionSerializationUtil::deserializeExpression(serializedWindowAggregation.mutable_asfield())
                       ->as<FieldAccessExpressionNode>();

    Windowing::WindowAggregationPtr aggregation;
    if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_SUM) {
        aggregation = Windowing::SumAggregationDescriptor::create(onField, asField);
    } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MAX) {
        aggregation = Windowing::MaxAggregationDescriptor::create(onField, asField);
    } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MIN) {
        aggregation = Windowing::MinAggregationDescriptor::create(onField, asField);
    } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_COUNT) {
        aggregation = Windowing::CountAggregationDescriptor::create(onField, asField);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window aggregation: "
                        << serializedWindowAggregation.DebugString());
    }

    Windowing::WindowTriggerPolicyPtr trigger;
    if (serializedTriggerPolicy.type() == SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnTime) {
        trigger = Windowing::OnTimeTriggerPolicyDescription::create(serializedTriggerPolicy.timeinms());
    } else if (serializedTriggerPolicy.type() == SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnBuffer) {
        trigger = Windowing::OnBufferTriggerPolicyDescription::create();
    } else if (serializedTriggerPolicy.type() == SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnRecord) {
        trigger = Windowing::OnRecordTriggerPolicyDescription::create();
    } else if (serializedTriggerPolicy.type() == SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnWatermarkChange) {
        trigger = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize trigger: " << serializedTriggerPolicy.DebugString());
    }

    Windowing::WindowActionDescriptorPtr action;
    if (serializedAction.type() == SerializableOperator_WindowDetails_TriggerAction_Type_Complete) {
        action = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    } else if (serializedAction.type() == SerializableOperator_WindowDetails_TriggerAction_Type_Slicing) {
        action = Windowing::SliceAggregationTriggerActionDescriptor::create();
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize action: " << serializedAction.DebugString());
    }

    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_WindowDetails_TumblingWindow>()) {
        auto serializedTumblingWindow = SerializableOperator_WindowDetails_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        auto serializedTimeCharacterisitc = serializedTumblingWindow.timecharacteristic();
        if (serializedTimeCharacterisitc.type() == SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime) {
            auto field = Attribute(serializedTimeCharacterisitc.field());
            auto multiplier = serializedTimeCharacterisitc.multiplier();
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else if (serializedTimeCharacterisitc.type()
                   == SerializableOperator_WindowDetails_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: "
                            << serializedTimeCharacterisitc.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_WindowDetails_SlidingWindow>()) {
        auto serializedSlidingWindow = SerializableOperator_WindowDetails_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        auto serializedTimeCharacterisitc = serializedSlidingWindow.timecharacteristic();
        if (serializedTimeCharacterisitc.type() == SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime) {
            auto field = Attribute(serializedTimeCharacterisitc.field());
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createEventTime(field),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else if (serializedTimeCharacterisitc.type()
                   == SerializableOperator_WindowDetails_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: "
                            << serializedTimeCharacterisitc.DebugString());
        }
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: " << serializedWindowType.DebugString());
    }

    auto distrChar = windowDetails->distrchar();
    Windowing::DistributionCharacteristicPtr distChar;
    if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: "
                  "SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete");
        distChar = Windowing::DistributionCharacteristic::createCompleteWindowType();
    } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: "
                  "SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining");
        distChar = std::make_shared<Windowing::DistributionCharacteristic>(Windowing::DistributionCharacteristic::Combining);
    } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: "
                  "SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing");
        distChar = std::make_shared<Windowing::DistributionCharacteristic>(Windowing::DistributionCharacteristic::Slicing);
    } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Merging) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: "
                  "SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Merging");
        distChar = std::make_shared<Windowing::DistributionCharacteristic>(Windowing::DistributionCharacteristic::Merging);
    } else {
        NES_NOT_IMPLEMENTED();
    }

    LogicalOperatorNodePtr ptr;
    auto allowedLateness = windowDetails->allowedlateness();
    if (!windowDetails->has_onkey()) {
        if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete) {
            auto windowDef =
                Windowing::LogicalWindowDefinition::create(aggregation, window, distChar, 1, trigger, action, allowedLateness);
            return LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDef, operatorId)
                ->as<CentralWindowOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining
                   || distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Merging) {
            auto windowDef = Windowing::LogicalWindowDefinition::create(
                aggregation, window, distChar, windowDetails->numberofinputedges(), trigger, action, allowedLateness);
            return LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef, operatorId)
                ->as<WindowComputationOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing) {
            auto windowDef =
                Windowing::LogicalWindowDefinition::create(aggregation, window, distChar, 1, trigger, action, allowedLateness);
            return LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef, operatorId)
                ->as<SliceCreationOperator>();
        } else {
            NES_NOT_IMPLEMENTED();
        }
    } else {
        auto keyAccessExpression =
            ExpressionSerializationUtil::deserializeExpression(windowDetails->mutable_onkey())->as<FieldAccessExpressionNode>();
        if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete) {
            auto windowDef = Windowing::LogicalWindowDefinition::create(keyAccessExpression, aggregation, window, distChar, 1,
                                                                        trigger, action, allowedLateness);
            return LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDef, operatorId)
                ->as<CentralWindowOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining
                   || distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Merging) {
            auto windowDef =
                Windowing::LogicalWindowDefinition::create(keyAccessExpression, aggregation, window, distChar,
                                                           windowDetails->numberofinputedges(), trigger, action, allowedLateness);
            return LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef, operatorId)
                ->as<WindowComputationOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing) {
            return LogicalOperatorFactory::createSliceCreationSpecializedOperator(
                       Windowing::LogicalWindowDefinition::create(keyAccessExpression, aggregation, window, distChar, 1, trigger,
                                                                  action, allowedLateness),
                       operatorId)
                ->as<SliceCreationOperator>();
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }
}

JoinLogicalOperatorNodePtr OperatorSerializationUtil::deserializeJoinOperator(SerializableOperator_JoinDetails* joinDetails,
                                                                              OperatorId operatorId) {
    auto serializedTriggerPolicy = joinDetails->triggerpolicy();
    auto serializedAction = joinDetails->action();

    auto serializedWindowType = joinDetails->windowtype();

    Windowing::WindowTriggerPolicyPtr trigger;
    if (serializedTriggerPolicy.type() == SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnTime) {
        trigger = Windowing::OnTimeTriggerPolicyDescription::create(serializedTriggerPolicy.timeinms());
    } else if (serializedTriggerPolicy.type() == SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnBuffer) {
        trigger = Windowing::OnBufferTriggerPolicyDescription::create();
    } else if (serializedTriggerPolicy.type() == SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnRecord) {
        trigger = Windowing::OnRecordTriggerPolicyDescription::create();
    } else if (serializedTriggerPolicy.type() == SerializableOperator_JoinDetails_TriggerPolicy_Type_triggerOnWatermarkChange) {
        trigger = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize trigger: " << serializedTriggerPolicy.DebugString());
    }

    Join::BaseJoinActionDescriptorPtr action;
    if (serializedAction.type() == SerializableOperator_JoinDetails_TriggerAction_Type_LazyNestedLoop) {
        action = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize action: " << serializedAction.DebugString());
    }

    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_JoinDetails_TumblingWindow>()) {
        auto serializedTumblingWindow = SerializableOperator_JoinDetails_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        auto serializedTimeCharacterisitc = serializedTumblingWindow.timecharacteristic();
        if (serializedTimeCharacterisitc.type() == SerializableOperator_JoinDetails_TimeCharacteristic_Type_EventTime) {
            auto field = Attribute(serializedTimeCharacterisitc.field());
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createEventTime(field),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else if (serializedTimeCharacterisitc.type()
                   == SerializableOperator_JoinDetails_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: "
                            << serializedTimeCharacterisitc.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_JoinDetails_SlidingWindow>()) {
        auto serializedSlidingWindow = SerializableOperator_JoinDetails_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        auto serializedTimeCharacterisitc = serializedSlidingWindow.timecharacteristic();
        if (serializedTimeCharacterisitc.type() == SerializableOperator_JoinDetails_TimeCharacteristic_Type_EventTime) {
            auto field = Attribute(serializedTimeCharacterisitc.field());
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createEventTime(field),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else if (serializedTimeCharacterisitc.type()
                   == SerializableOperator_JoinDetails_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: "
                            << serializedTimeCharacterisitc.DebugString());
        }
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: " << serializedWindowType.DebugString());
    }

    LogicalOperatorNodePtr ptr;
    auto distChar = Windowing::DistributionCharacteristic::createCompleteWindowType();
    auto leftKeyAccessExpression =
        ExpressionSerializationUtil::deserializeExpression(joinDetails->mutable_onleftkey())->as<FieldAccessExpressionNode>();
    auto rightKeyAccessExpression =
        ExpressionSerializationUtil::deserializeExpression(joinDetails->mutable_onrightkey())->as<FieldAccessExpressionNode>();
    auto joinDefinition =
        Join::LogicalJoinDefinition::create(leftKeyAccessExpression, rightKeyAccessExpression, window, distChar, trigger, action,
                                            joinDetails->numberofinputedgesleft(), joinDetails->numberofinputedgesright());
    auto retValue = LogicalOperatorFactory::createJoinOperator(joinDefinition, operatorId)->as<JoinLogicalOperatorNode>();
    return retValue;

    //TODO: enable distrChar for distributed joins
    //    if (distrChar.distr() == SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Complete) {
    //        return LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDef, operatorId)->as<CentralWindowOperator>();
    //    } else if (distrChar.distr() == SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Combining) {
    //        return LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef, operatorId)->as<WindowComputationOperator>();
    //    } else if (distrChar.distr() == SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Slicing) {
    //        return LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef, operatorId)->as<SliceCreationOperator>();
    //    } else {
    //        NES_NOT_IMPLEMENTED();
    //    }
}
SerializableOperator_SourceDetails
OperatorSerializationUtil::serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator) {
    auto sourceDetails = SerializableOperator_SourceDetails();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    serializeSourceSourceDescriptor(sourceDescriptor, &sourceDetails);
    return sourceDetails;
}

SerializableOperator_SinkDetails OperatorSerializationUtil::serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator) {
    auto sinkDetails = SerializableOperator_SinkDetails();
    auto sinkDescriptor = sinkOperator->getSinkDescriptor();
    serializeSinkDescriptor(sinkDescriptor, &sinkDetails);
    return sinkDetails;
}

OperatorNodePtr OperatorSerializationUtil::deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails) {
    auto sinkDescriptor = deserializeSinkDescriptor(sinkDetails);
    return LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
}

SerializableOperator_SourceDetails*
OperatorSerializationUtil::serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor,
                                                           SerializableOperator_SourceDetails* sourceDetails) {

    // serialize a source descriptor and all its properties depending of its type
    NES_DEBUG("OperatorSerializationUtil:: serialize to SourceDescriptor with =" << sourceDescriptor->toString());
    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
        // serialize zmq source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor");
        auto zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        zmqSerializedSourceDescriptor.set_host(zmqSourceDescriptor->getHost());
        zmqSerializedSourceDescriptor.set_port(zmqSourceDescriptor->getPort());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(zmqSourceDescriptor->getSchema(),
                                                 zmqSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(zmqSerializedSourceDescriptor);
    }
#ifdef ENABLE_MQTT_BUILD
    else if (sourceDescriptor->instanceOf<MQTTSourceDescriptor>()) {
        // serialize MQTT source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor");
        auto mqttSourceDescriptor = sourceDescriptor->as<MQTTSourceDescriptor>();
        auto mqttSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor();
        mqttSerializedSourceDescriptor.set_serveraddress(mqttSourceDescriptor->getServerAddress());
        mqttSerializedSourceDescriptor.set_clientid(mqttSourceDescriptor->getClientId());
        mqttSerializedSourceDescriptor.set_topic(mqttSourceDescriptor->getTopic());
        mqttSerializedSourceDescriptor.set_user(mqttSourceDescriptor->getUser());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(mqttSourceDescriptor->getSchema(),
                                                 mqttSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(mqttSerializedSourceDescriptor);
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (sourceDescriptor->instanceOf<OPCSourceDescriptor>()) {
        // serialize opc source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor");
        auto opcSourceDescriptor = sourceDescriptor->as<OPCSourceDescriptor>();
        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
        char* ident = (char*) UA_malloc(sizeof(char) * opcSourceDescriptor->getNodeId().identifier.string.length + 1);
        memcpy(ident, opcSourceDescriptor->getNodeId().identifier.string.data,
               opcSourceDescriptor->getNodeId().identifier.string.length);
        ident[opcSourceDescriptor->getNodeId().identifier.string.length] = '\0';
        opcSerializedSourceDescriptor.set_identifier(ident);
        opcSerializedSourceDescriptor.set_url(opcSourceDescriptor->getUrl());
        opcSerializedSourceDescriptor.set_namespaceindex(opcSourceDescriptor->getNodeId().namespaceIndex);
        opcSerializedSourceDescriptor.set_identifiertype(opcSourceDescriptor->getNodeId().identifierType);
        opcSerializedSourceDescriptor.set_user(opcSourceDescriptor->getUser());
        opcSerializedSourceDescriptor.set_password(opcSourceDescriptor->getPassword());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(opcSourceDescriptor->getSchema(),
                                                 opcSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(opcSerializedSourceDescriptor);
    }
#endif
    else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
        // serialize network source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor");
        auto networkSourceDescriptor = sourceDescriptor->as<Network::NetworkSourceDescriptor>();
        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
        networkSerializedSourceDescriptor.mutable_nespartition()->set_queryid(
            networkSourceDescriptor->getNesPartition().getQueryId());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_operatorid(
            networkSourceDescriptor->getNesPartition().getOperatorId());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_partitionid(
            networkSourceDescriptor->getNesPartition().getPartitionId());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_subpartitionid(
            networkSourceDescriptor->getNesPartition().getSubpartitionId());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(networkSourceDescriptor->getSchema(),
                                                 networkSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(networkSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
        // serialize default source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor");
        auto defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        defaultSerializedSourceDescriptor.set_frequency(defaultSourceDescriptor->getFrequencyCount());
        defaultSerializedSourceDescriptor.set_numbufferstoprocess(defaultSourceDescriptor->getNumbersOfBufferToProduce());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(defaultSourceDescriptor->getSchema(),
                                                 defaultSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(defaultSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
        // serialize binary source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor");
        auto binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        binarySerializedSourceDescriptor.set_filepath(binarySourceDescriptor->getFilePath());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(binarySourceDescriptor->getSchema(),
                                                 binarySerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(binarySerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
        // serialize csv source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor");
        auto csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        csvSerializedSourceDescriptor.set_filepath(csvSourceDescriptor->getFilePath());
        csvSerializedSourceDescriptor.set_frequency(csvSourceDescriptor->getFrequencyCount());
        csvSerializedSourceDescriptor.set_delimiter(csvSourceDescriptor->getDelimiter());
        csvSerializedSourceDescriptor.set_numberoftuplestoproduceperbuffer(
            csvSourceDescriptor->getNumberOfTuplesToProducePerBuffer());
        csvSerializedSourceDescriptor.set_numbufferstoprocess(csvSourceDescriptor->getNumBuffersToProcess());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(csvSourceDescriptor->getSchema(),
                                                 csvSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(csvSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
        // serialize sense source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor");
        auto senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        senseSerializedSourceDescriptor.set_udfs(senseSourceDescriptor->getUdfs());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(senseSourceDescriptor->getSchema(),
                                                 senseSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(senseSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
        // serialize logical stream source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor");
        auto logicalStreamSourceDescriptor = sourceDescriptor->as<LogicalStreamSourceDescriptor>();
        auto logicalStreamSerializedSourceDescriptor =
            SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor();
        logicalStreamSerializedSourceDescriptor.set_streamname(logicalStreamSourceDescriptor->getStreamName());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(logicalStreamSourceDescriptor->getSchema(),
                                                 logicalStreamSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(logicalStreamSerializedSourceDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << sourceDescriptor->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
    return sourceDetails;
}

SourceDescriptorPtr
OperatorSerializationUtil::deserializeSourceDescriptor(SerializableOperator_SourceDetails* serializedSourceDetails) {
    // de-serialize source details and all its properties to a SourceDescriptor
    NES_TRACE("OperatorSerializationUtil:: de-serialized SourceDescriptor id=" << serializedSourceDetails->DebugString());
    const auto& serializedSourceDescriptor = serializedSourceDetails->sourcedescriptor();

    if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as ZmqSourceDescriptor");
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&zmqSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(zmqSerializedSourceDescriptor.release_sourceschema());
        auto ret =
            ZmqSourceDescriptor::create(schema, zmqSerializedSourceDescriptor.host(), zmqSerializedSourceDescriptor.port());
        return ret;
    }
#ifdef ENABLE_MQTT_BUILD
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor>()) {
        // de-serialize mqtt source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as MQTTSourceDescriptor");
        auto mqttSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&mqttSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(mqttSerializedSourceDescriptor.release_sourceschema());
        auto ret = MQTTSourceDescriptor::create(schema, mqttSerializedSourceDescriptor.serveraddress(),
                                                mqttSerializedSourceDescriptor.clientid(), mqttSerializedSourceDescriptor.user(),
                                                mqttSerializedSourceDescriptor.topic());
        return ret;
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor>()) {
        // de-serialize opc source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as OPCSourceDescriptor");
        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&opcSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(opcSerializedSourceDescriptor.release_sourceschema());
        char* ident = (char*) UA_malloc(sizeof(char) * opcSerializedSourceDescriptor.identifier().length() + 1);
        memcpy(ident, opcSerializedSourceDescriptor.identifier().data(), opcSerializedSourceDescriptor.identifier().length());
        ident[opcSerializedSourceDescriptor.identifier().length()] = '\0';
        UA_NodeId nodeId = UA_NODEID_STRING(opcSerializedSourceDescriptor.namespaceindex(), ident);
        auto ret = OPCSourceDescriptor::create(schema, opcSerializedSourceDescriptor.url(), nodeId,
                                               opcSerializedSourceDescriptor.user(), opcSerializedSourceDescriptor.password());
        return ret;
    }
#endif
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as NetworkSourceDescriptor");
        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&networkSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(networkSerializedSourceDescriptor.release_sourceschema());
        Network::NesPartition nesPartition{networkSerializedSourceDescriptor.nespartition().queryid(),
                                           networkSerializedSourceDescriptor.nespartition().operatorid(),
                                           networkSerializedSourceDescriptor.nespartition().partitionid(),
                                           networkSerializedSourceDescriptor.nespartition().subpartitionid()};
        auto ret = Network::NetworkSourceDescriptor::create(schema, nesPartition);
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor>()) {
        // de-serialize default stream source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as DefaultSourceDescriptor");
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&defaultSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(defaultSerializedSourceDescriptor.release_sourceschema());
        auto ret = DefaultSourceDescriptor::create(schema, defaultSerializedSourceDescriptor.numbufferstoprocess(),
                                                   defaultSerializedSourceDescriptor.frequency());
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor>()) {
        // de-serialize binary source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as BinarySourceDescriptor");
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&binarySerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(binarySerializedSourceDescriptor.release_sourceschema());
        auto ret = BinarySourceDescriptor::create(schema, binarySerializedSourceDescriptor.filepath());
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor>()) {
        // de-serialize csv source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as CsvSourceDescriptor");
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&csvSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(csvSerializedSourceDescriptor.release_sourceschema());
        auto ret = CsvSourceDescriptor::create(
            schema, csvSerializedSourceDescriptor.filepath(), csvSerializedSourceDescriptor.delimiter(),
            csvSerializedSourceDescriptor.numberoftuplestoproduceperbuffer(), csvSerializedSourceDescriptor.numbufferstoprocess(),
            csvSerializedSourceDescriptor.frequency(), csvSerializedSourceDescriptor.skipheader());
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor>()) {
        // de-serialize sense source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as SenseSourceDescriptor");
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&senseSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(senseSerializedSourceDescriptor.release_sourceschema());
        return SenseSourceDescriptor::create(schema, senseSerializedSourceDescriptor.udfs());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor>()) {
        // de-serialize logical stream source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as LogicalStreamSourceDescriptor");
        auto logicalStreamSerializedSourceDescriptor =
            SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&logicalStreamSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(logicalStreamSerializedSourceDescriptor.release_sourceschema());
        SourceDescriptorPtr logicalStreamSourceDescriptor =
            LogicalStreamSourceDescriptor::create(logicalStreamSerializedSourceDescriptor.streamname());
        logicalStreamSourceDescriptor->setSchema(schema);
        return logicalStreamSourceDescriptor;
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << serializedSourceDescriptor.type_url());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}
SerializableOperator_SinkDetails*
OperatorSerializationUtil::serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor,
                                                   SerializableOperator_SinkDetails* sinkDetails) {
    // serialize a sink descriptor and all its properties depending of its type
    NES_DEBUG("OperatorSerializationUtil:: serialized SinkDescriptor ");
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        // serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor();
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<NullOutputSinkDescriptor>()) {
        // serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableNullOutputSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNullOutputSinkDescriptor();
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        // serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor");
        auto zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        serializedSinkDescriptor.set_port(zmqSinkDescriptor->getPort());
        serializedSinkDescriptor.set_isinternal(zmqSinkDescriptor->isInternal());
        serializedSinkDescriptor.set_host(zmqSinkDescriptor->getHost());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    }
#ifdef ENABLE_OPC_BUILD
    else if (sinkDescriptor->instanceOf<OPCSinkDescriptor>()) {
        // serialize opc sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor");
        auto opcSinkDescriptor = sinkDescriptor->as<OPCSinkDescriptor>();
        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
        char* ident = (char*) UA_malloc(sizeof(char) * opcSinkDescriptor->getNodeId().identifier.string.length + 1);
        memcpy(ident, opcSinkDescriptor->getNodeId().identifier.string.data,
               opcSinkDescriptor->getNodeId().identifier.string.length);
        ident[opcSinkDescriptor->getNodeId().identifier.string.length] = '\0';
        opcSerializedSinkDescriptor.set_identifier(ident);
        free(ident);
        opcSerializedSinkDescriptor.set_url(opcSinkDescriptor->getUrl());
        opcSerializedSinkDescriptor.set_namespaceindex(opcSinkDescriptor->getNodeId().namespaceIndex);
        opcSerializedSinkDescriptor.set_identifiertype(opcSinkDescriptor->getNodeId().identifierType);
        opcSerializedSinkDescriptor.set_user(opcSinkDescriptor->getUser());
        opcSerializedSinkDescriptor.set_password(opcSinkDescriptor->getPassword());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(opcSerializedSinkDescriptor);
    }
#endif
    else if (sinkDescriptor->instanceOf<MQTTSinkDescriptor>()) {
        // serialize MQTT sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor");
        auto mqttSinkDescriptor = sinkDescriptor->as<MQTTSinkDescriptor>();
        auto mqttSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor();
        mqttSerializedSinkDescriptor.set_address(mqttSinkDescriptor->getAddress());
        mqttSerializedSinkDescriptor.set_clientid(mqttSinkDescriptor->getClientId());
        mqttSerializedSinkDescriptor.set_topic(mqttSinkDescriptor->getTopic());
        mqttSerializedSinkDescriptor.set_user(mqttSinkDescriptor->getUser());
        mqttSerializedSinkDescriptor.set_maxbufferedmsgs(mqttSinkDescriptor->getMaxBufferedMSGs());
        mqttSerializedSinkDescriptor.set_timeunit(
            (SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor_TimeUnits) mqttSinkDescriptor->getTimeUnit());
        mqttSerializedSinkDescriptor.set_msgdelay(mqttSinkDescriptor->getMsgDelay());
        mqttSerializedSinkDescriptor.set_asynchronousclient(mqttSinkDescriptor->getAsynchronousClient());

        sinkDetails->mutable_sinkdescriptor()->PackFrom(mqttSerializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        // serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
        //set details of NesPartition
        auto serializedNesPartition = serializedSinkDescriptor.mutable_nespartition();
        auto nesPartition = networkSinkDescriptor->getNesPartition();
        serializedNesPartition->set_queryid(nesPartition.getQueryId());
        serializedNesPartition->set_operatorid(nesPartition.getOperatorId());
        serializedNesPartition->set_partitionid(nesPartition.getPartitionId());
        serializedNesPartition->set_subpartitionid(nesPartition.getSubpartitionId());
        //set details of NodeLocation
        auto serializedNodeLocation = serializedSinkDescriptor.mutable_nodelocation();
        auto nodeLocation = networkSinkDescriptor->getNodeLocation();
        serializedNodeLocation->set_nodeid(nodeLocation.getNodeId());
        serializedNodeLocation->set_hostname(nodeLocation.getHostname());
        serializedNodeLocation->set_port(nodeLocation.getPort());
        // set reconnection details
        auto s = std::chrono::duration_cast<std::chrono::seconds>(networkSinkDescriptor->getWaitTime());
        serializedSinkDescriptor.set_waittime(s.count());
        serializedSinkDescriptor.set_retrytimes(networkSinkDescriptor->getRetryTimes());
        //pack to output
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        // serialize file sink descriptor. The file sink has different types which have to be set correctly
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableFileSinkDescriptor");
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();

        serializedSinkDescriptor.set_filepath(fileSinkDescriptor->getFileName());
        serializedSinkDescriptor.set_append(fileSinkDescriptor->getAppend());

        auto format = fileSinkDescriptor->getSinkFormatAsString();
        if (format == "JSON_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("JSON_FORMAT");
        } else if (format == "CSV_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("CSV_FORMAT");
        } else if (format == "NES_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("NES_FORMAT");
        } else if (format == "TEXT_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("TEXT_FORMAT");
        } else {
            NES_ERROR("serializeSinkDescriptor: format not supported");
        }
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - " << sinkDescriptor->toString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
    return sinkDetails;
}

SinkDescriptorPtr OperatorSerializationUtil::deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails) {
    // de-serialize a sink descriptor and all its properties to a SinkDescriptor.
    NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor " << sinkDetails->DebugString());
    const auto& deserializedSinkDescriptor = sinkDetails->sinkdescriptor();
    if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as PrintSinkDescriptor");
        return PrintSinkDescriptor::create();
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableNullOutputSinkDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as PrintSinkDescriptor");
        return NullOutputSinkDescriptor::create();
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as ZmqSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return ZmqSinkDescriptor::create(serializedSinkDescriptor.host(), serializedSinkDescriptor.port(),
                                         serializedSinkDescriptor.isinternal());
    }
#ifdef ENABLE_OPC_BUILD
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor>()) {
        // de-serialize opc sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as OPCSinkDescriptor");
        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&opcSerializedSinkDescriptor);
        char* ident = (char*) UA_malloc(sizeof(char) * opcSerializedSinkDescriptor.identifier().length() + 1);
        memcpy(ident, opcSerializedSinkDescriptor.identifier().data(), opcSerializedSinkDescriptor.identifier().length());
        ident[opcSerializedSinkDescriptor.identifier().length()] = '\0';
        UA_NodeId nodeId = UA_NODEID_STRING(opcSerializedSinkDescriptor.namespaceindex(), ident);
        return OPCSinkDescriptor::create(opcSerializedSinkDescriptor.url(), nodeId, opcSerializedSinkDescriptor.user(),
                                         opcSerializedSinkDescriptor.password());
    }
#endif
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor>()) {
        // de-serialize MQTT sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as MQTTSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return MQTTSinkDescriptor::create(serializedSinkDescriptor.address(), serializedSinkDescriptor.topic(),
                                          serializedSinkDescriptor.user(), serializedSinkDescriptor.maxbufferedmsgs(),
                                          (MQTTSinkDescriptor::TimeUnits) serializedSinkDescriptor.timeunit(),
                                          serializedSinkDescriptor.msgdelay(),
                                          (MQTTSinkDescriptor::ServiceQualities) serializedSinkDescriptor.qualityofservice(),
                                          serializedSinkDescriptor.asynchronousclient(), serializedSinkDescriptor.clientid());
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as NetworkSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        Network::NesPartition nesPartition{
            serializedSinkDescriptor.nespartition().queryid(), serializedSinkDescriptor.nespartition().operatorid(),
            serializedSinkDescriptor.nespartition().partitionid(), serializedSinkDescriptor.nespartition().subpartitionid()};
        Network::NodeLocation nodeLocation{serializedSinkDescriptor.nodelocation().nodeid(),
                                           serializedSinkDescriptor.nodelocation().hostname(),
                                           serializedSinkDescriptor.nodelocation().port()};
        auto waitTime = std::chrono::seconds(serializedSinkDescriptor.waittime());
        return Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, waitTime,
                                                      serializedSinkDescriptor.retrytimes());
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableFileSinkDescriptor>()) {
        // de-serialize file sink descriptor
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as FileSinkDescriptor");
        return FileSinkDescriptor::create(serializedSinkDescriptor.filepath(), serializedSinkDescriptor.sinkformat(),
                                          serializedSinkDescriptor.append() == true ? "APPEND" : "OVERWRITE");
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown sink Descriptor Type " << sinkDetails->DebugString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

SerializableOperator_WatermarkStrategyDetails
OperatorSerializationUtil::serializeWatermarkAssignerOperator(WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize watermark assigner operator ");

    auto watermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
    auto watermarkStrategyDescriptor = watermarkAssignerOperator->getWatermarkStrategyDescriptor();
    serializeWatermarkStrategyDescriptor(watermarkStrategyDescriptor, &watermarkStrategyDetails);
    return watermarkStrategyDetails;
}

SerializableOperator_WatermarkStrategyDetails* OperatorSerializationUtil::serializeWatermarkStrategyDescriptor(
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor,
    SerializableOperator_WatermarkStrategyDetails* watermarkStrategyDetails) {
    NES_TRACE("OperatorSerializationUtil:: serialize watermark strategy ");

    if (auto eventTimeWatermarkStrategyDescriptor =
            std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor)) {
        auto serializedWatermarkStrategyDescriptor =
            SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        ExpressionSerializationUtil::serializeExpression(eventTimeWatermarkStrategyDescriptor->getOnField().getExpressionNode(),
                                                         serializedWatermarkStrategyDescriptor.mutable_onfield());
        serializedWatermarkStrategyDescriptor.set_allowedlateness(
            eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime());
        serializedWatermarkStrategyDescriptor.set_multiplier(eventTimeWatermarkStrategyDescriptor->getTimeUnit().getMultiplier());
        watermarkStrategyDetails->mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    } else if (std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor)) {
        auto serializedWatermarkStrategyDescriptor =
            SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor();
        watermarkStrategyDetails->mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Watermark Strategy Descriptor Type");
    }
    return watermarkStrategyDetails;
}
Windowing::WatermarkStrategyDescriptorPtr OperatorSerializationUtil::deserializeWatermarkStrategyDescriptor(
    SerializableOperator_WatermarkStrategyDetails* watermarkStrategyDetails) {
    NES_TRACE("OperatorSerializationUtil:: de-serialize watermark strategy ");
    const auto& deserializedWatermarkStrategyDescriptor = watermarkStrategyDetails->strategy();
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized WatermarkStrategy as EventTimeWatermarkStrategyDescriptor");
        auto serializedEventTimeWatermarkStrategyDescriptor =
            SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        deserializedWatermarkStrategyDescriptor.UnpackTo(&serializedEventTimeWatermarkStrategyDescriptor);

        auto onField =
            ExpressionSerializationUtil::deserializeExpression(serializedEventTimeWatermarkStrategyDescriptor.mutable_onfield())
                ->as<FieldAccessExpressionNode>();
        NES_DEBUG("OperatorSerializationUtil:: deserialized field name " << onField->getFieldName());
        auto eventTimeWatermarkStrategyDescriptor = Windowing::EventTimeWatermarkStrategyDescriptor::create(
            Attribute(onField->getFieldName()),
            Windowing::TimeMeasure(serializedEventTimeWatermarkStrategyDescriptor.allowedlateness()),
            Windowing::TimeUnit(serializedEventTimeWatermarkStrategyDescriptor.multiplier()));
        return eventTimeWatermarkStrategyDescriptor;
    } else if (deserializedWatermarkStrategyDescriptor
                   .Is<SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor>()) {
        return Windowing::IngestionTimeWatermarkStrategyDescriptor::create();
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Serialized Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Serialized Watermark Strategy Descriptor Type");
    }
}
}// namespace NES