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

#include <memory>
#include <optional>
#include <vector>

#include <InputFormatters/FormatScanPhysicalOperator.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalProjection.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include "InputFormatters/InputFormatterProvider.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto handlerId = getNextOperatorHandlerId();
    auto inputSchema = projectionLogicalOperator.getInputSchemas()[0];
    auto outputSchema = projectionLogicalOperator.getOutputSchema();
    auto bufferSize = conf.pageSize.getValue();

    // LogicalOperator
    const auto sourceOperators
        = projectionLogicalOperator.getChildren()
        | std::views::filter([](const auto& childOperator)
                             { return childOperator.template tryGet<SourceDescriptorLogicalOperator>().has_value(); })
        | std::views::transform(
              [](const auto& sourceChildOperator)
              { return sourceChildOperator.template tryGet<SourceDescriptorLogicalOperator>().value().getSourceDescriptor(); })
        | std::ranges::to<std::vector>();
    const bool isFirstOperatorAfterSource = not(sourceOperators.empty());
    auto inputFormatterTaskPipeline = [isFirstOperatorAfterSource, &inputSchema, &sourceOperators]()
    {
        if (isFirstOperatorAfterSource)
        {
            // Todo: how to handle multiple sources? --> should all have the same format, otherwise they should not have the same projection
            // -> could add invariant
            return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
                OriginId(OriginId::INITIAL), inputSchema, sourceOperators.front().getParserConfig());
        }
        return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
            OriginId(OriginId::INITIAL), inputSchema, ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""});
    }();
    auto scan = FormatScanPhysicalOperator(
        outputSchema.getFieldNames(), std::move(inputFormatterTaskPipeline), bufferSize, isFirstOperatorAfterSource);
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);

    auto emitLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(bufferSize, outputSchema);
    auto emitMemoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(emitLayout);
    auto emit = EmitPhysicalOperator(handlerId, emitMemoryProvider);
    auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
        emit,
        outputSchema,
        outputSchema,
        handlerId,
        std::make_shared<EmitOperatorHandler>(),
        PhysicalOperatorWrapper::PipelineLocation::EMIT,
        std::vector{scanWrapper});

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(projectionLogicalOperator.getChildren().size(), scanWrapper);
    return {.root = emitWrapper, .leafs = {scanWrapper}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterProjectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalProjection>(argument.conf);
}
}
