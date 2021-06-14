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

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/NamespaceDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Runtime/SharedPointerGen.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ContinueStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipelineStage.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/WindowActions/BaseJoinActionDescriptor.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

namespace NES {
namespace QueryCompilation {
CCodeGenerator::CCodeGenerator() : CodeGenerator(), compiler(Compiler::create()) {}

StructDeclaration CCodeGenerator::getStructDeclarationFromSchema(std::string structName, SchemaPtr schema) {
    /* struct definition for tuples */
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    NES_DEBUG("Converting Schema: " << schema->toString());
    NES_DEBUG("Define Struct : " << structName);

    for (uint64_t i = 0; i < schema->getSize(); ++i) {
        structDeclarationTuple.addField(VariableDeclaration::create(schema->get(i)->getDataType(), schema->get(i)->getName()));
        NES_DEBUG("Field " << i << ": " << schema->get(i)->getDataType()->toString() << " " << schema->get(i)->getName());
    }
    return structDeclarationTuple;
}

const VariableDeclarationPtr getVariableDeclarationForField(const StructDeclaration& structDeclaration,
                                                            const AttributeFieldPtr field) {
    if (structDeclaration.getField(field->getName()))
        return std::make_shared<VariableDeclaration>(structDeclaration.getVariableDeclaration(field->getName()));
    else {
        return VariableDeclarationPtr();
    }
}

const std::string toString(void*, DataTypePtr) {
    //     if(type->)
    return "";
}

CodeGeneratorPtr CCodeGenerator::create() { return std::make_shared<CCodeGenerator>(); }

bool CCodeGenerator::generateCodeForScanSetup(PipelineContextPtr context) {
    auto tf = getTypeFactory();
    auto pipelineExecutionContextType = tf->createAnonymusDataType("NodeEngine::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    return true;
}

bool CCodeGenerator::generateCodeForScan(SchemaPtr inputSchema, SchemaPtr outputSchema, PipelineContextPtr context) {

    context->inputSchema = outputSchema->copy();
    auto code = context->code;
    switch (context->arity) {
        case PipelineContext::Unary: {
            // it is assumed that the input item is the first element!
            // so place to front
            // todo remove this assumtion
            code->structDeclarationInputTuples.insert(code->structDeclarationInputTuples.begin(),
                                                      getStructDeclarationFromSchema("InputTuple", inputSchema));
            NES_DEBUG("arity unary generate scan for input=" << inputSchema->toString()
                                                             << " output=" << outputSchema->toString());
            break;
        }
        case PipelineContext::BinaryLeft: {
            code->structDeclarationInputTuples.emplace_back(getStructDeclarationFromSchema("InputTupleLeft", inputSchema));
            NES_DEBUG("arity binaryleft generate scan for input=" << inputSchema->toString()
                                                                  << " output=" << outputSchema->toString());
            break;
        }
        case PipelineContext::BinaryRight: {
            code->structDeclarationInputTuples.emplace_back(getStructDeclarationFromSchema("InputTupleRight", inputSchema));
            NES_DEBUG("arity binaryright generate scan for input=" << inputSchema->toString()
                                                                   << " output=" << outputSchema->toString());
            break;
        }
    }

    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = outputSchema;
    code->structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", outputSchema);
    auto tf = getTypeFactory();
    /* === declarations === */
    auto tupleBufferType = tf->createAnonymusDataType("NES::NodeEngine::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("NodeEngine::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("NodeEngine::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    auto varDeclarationResultBuffer = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;
    code->varDeclarationResultBuffer = varDeclarationResultBuffer;

    /* declaration of record index; */
    code->varDeclarationRecordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "recordIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());
    /* ExecutionResult ret = Ok; */
    // TODO probably it's not safe that we can mix enum values with int32 but it is a good hack for me :P
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"))
            .copy());

    code->varDeclarationInputTuples =
        VariableDeclaration::create(tf->createPointer(tf->createUserDefinedType(code->structDeclarationInputTuples[0])),
                                    "inputTuples");

    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    /*  tuples = (InputTuple *)input_buffer.getBuffer()*/
    code->variableInitStmts.push_back(
        VarDeclStatement(varDeclarationResultBuffer).assign(allocateTupleBuffer(varDeclarationPipelineExecutionContext)).copy());

    code->variableInitStmts.push_back(
        VarDeclStatement(code->varDeclarationInputTuples)
            .assign(getTypedBuffer(code->varDeclarationInputBuffer, code->structDeclarationInputTuples[0]))
            .copy());

    /* for (uint64_t recordIndex = 0; recordIndex < tuple_buffer_1->num_tuples; ++id) */
    // input_buffer.getNumberOfTuples()
    auto numberOfRecords = VarRef(varDeclarationInputBuffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall);
    code->forLoopStmt = std::make_shared<FOR>(code->varDeclarationRecordIndex,
                                              (VarRef(code->varDeclarationRecordIndex) < (numberOfRecords)).copy(),
                                              (++VarRef(code->varDeclarationRecordIndex)).copy());

    code->currentCodeInsertionPoint = code->forLoopStmt->getCompoundStatement();
    if (context->arity != PipelineContext::Unary) {
        NES_DEBUG("adding in scan for schema=" << inputSchema->toString() << " context=" << context->inputSchema->toString());
    }

    auto recordHandler = context->getRecordHandler();
    for (AttributeFieldPtr field : inputSchema->fields) {
        auto variable = getVariableDeclarationForField(code->structDeclarationInputTuples[0], field);

        auto fieldRefStatement =
            VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                VarRef(variable));

        recordHandler->registerAttribute(field->getName(), fieldRefStatement.copy());
    }

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());
    return true;
}

bool CCodeGenerator::generateCodeForProjection(std::vector<ExpressionNodePtr> projectExpressions, PipelineContextPtr context) {
    auto recordHandler = context->getRecordHandler();
    for (auto expression : projectExpressions) {
        if (expression->instanceOf<FieldRenameExpressionNode>()) {
            // if rename expression add the attribute to the record handler.
            auto fieldRenameExpression = expression->as<FieldRenameExpressionNode>();
            auto originalAttribute = fieldRenameExpression->getOriginalField();
            if (!recordHandler->hasAttribute(originalAttribute->getFieldName())) {
                NES_FATAL_ERROR("CCodeGenerator: projection: the original attribute"
                                << originalAttribute->getFieldName() << " is not registered so we can not access it.");
            }
            // register the attribute with the new name in the record handler
            auto referenceToOriginalValue = recordHandler->getAttribute(originalAttribute->getFieldName());
            recordHandler->registerAttribute(fieldRenameExpression->getNewFieldName(), referenceToOriginalValue);
        } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
            // it is a field access expression, so we just check if the record exists.
            auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
            if (!recordHandler->hasAttribute(fieldAccessExpression->getFieldName())) {
                NES_FATAL_ERROR("CCodeGenerator: projection: the attribute" << fieldAccessExpression->getFieldName()
                                                                            << " is not registered so we can not access it.");
            }
        }
    }
    return true;
}

/**
 * generates code for predicates
 * @param pred - defined predicate for the query
 * @param context - includes the context of the used fields
 * @param out - sending some other information if wanted
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForFilter(PredicatePtr pred, PipelineContextPtr context) {

    // create predicate expression from filter predicate
    auto predicateExpression = pred->generateCode(context->code, context->getRecordHandler());
    // create if statement
    auto ifStatement = IF(*predicateExpression);
    // update current compound_stmt
    // first, add the head and brackets of the if-statement
    context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());
    // second, move insertion point. the rest of the pipeline will be generated within the brackets of the if-statement
    context->code->currentCodeInsertionPoint = ifStatement.getCompoundStatement();

    return true;
}

/**
 * generates code for a mapper with an defined AttributeField and a PredicatePtr
 * @param field - existing or new created field that includes the mapped function
 * @param pred - mapping function as a predicate tree for easy single lined functions.
 * @param context - includes the context of the used fields
 * @param out - sending some other information if wanted
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForMap(AttributeFieldPtr field, LegacyExpressionPtr pred, PipelineContextPtr context) {

    auto code = context->code;
    auto tf = getTypeFactory();

    // Check if the assignment value is new or if we have to create it
    auto mapExpression = pred->generateCode(code, context->getRecordHandler());
    auto recordHandler = context->getRecordHandler();
    if (recordHandler->hasAttribute(field->getName())) {
        // Get the attribute variable from the field and assign a new value to it.
        auto attributeVariable = recordHandler->getAttribute(field->getName());
        auto assignedMap = attributeVariable->assign(mapExpression);
        code->currentCodeInsertionPoint->addStatement(assignedMap.copy());
    } else {
        // Create a new attribute variable and assign the new value to it.
        auto variableDeclaration = VariableDeclaration::create(field->getDataType(), field->getName());
        auto attributeVariable = VarDeclStatement(variableDeclaration);
        auto assignedMap = attributeVariable.assign(mapExpression).copy();
        recordHandler->registerAttribute(field->getName(), VarRef(variableDeclaration).copy());
        code->currentCodeInsertionPoint->addStatement(assignedMap);
    }
    return true;
}

bool CCodeGenerator::generateCodeForEmit(SchemaPtr sinkSchema, PipelineContextPtr context) {
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for Sink.");
    auto code = context->code;
    // set result schema to context
    context->resultSchema = sinkSchema;
    // generate result tuple struct
    auto structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", sinkSchema);
    // add type declaration for the result tuple
    code->typeDeclarations.push_back(structDeclarationResultTuple);

    auto varDeclResultTuple =
        VariableDeclaration::create(tf->createPointer(tf->createUserDefinedType(structDeclarationResultTuple)), "resultTuples");

    /* ResultTuple* resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();*/
    code->variableInitStmts.push_back(VarDeclStatement(varDeclResultTuple)
                                          .assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple))
                                          .copy());

    auto recordHandler = context->getRecordHandler();
    for (auto field : context->resultSchema->fields) {

        auto resultRecordFieldVariableDeclaration = getVariableDeclarationForField(structDeclarationResultTuple, field);
        if (!resultRecordFieldVariableDeclaration) {
            NES_FATAL_ERROR("CodeGenerator: Could not extract field " << field->toString() << " from result record struct "
                                                                      << structDeclarationResultTuple.getTypeName());
        }

        // check if record handler has current field
        if (!recordHandler->hasAttribute(field->getName())) {
            NES_FATAL_ERROR("CCodeGenerator: field: " + field->toString()
                            + " is part of the output schema, "
                              "but not registered in the record handler.");
        }

        // Get current field from record handler.
        auto currentFieldVariableReference = recordHandler->getAttribute(field->getName());

        // use regular assing for types which are not arrays, as fixed char arrays support
        // assignment by operator.
        auto const copyFieldStatement = VarRef(varDeclResultTuple)[VarRef(code->varDeclarationNumberOfResultTuples)]
                                            .accessRef(VarRef(resultRecordFieldVariableDeclaration))
                                            .assign(currentFieldVariableReference);

        code->currentCodeInsertionPoint->addStatement(copyFieldStatement.copy());
    }

    // increment number of tuples in buffer -> ++numberOfResultTuples;
    code->currentCodeInsertionPoint->addStatement((++VarRef(code->varDeclarationNumberOfResultTuples)).copy());

    // generate logic to check if tuple buffer is already full. If so we emit the current one and pass it to the runtime.
    generateTupleBufferSpaceCheck(context, varDeclResultTuple, structDeclarationResultTuple);

    // Generate final logic to emit the last buffer to the runtime
    // 1. set the number of tuples to the buffer
    code->cleanupStmts.push_back(
        setNumberOfTuples(code->varDeclarationResultBuffer, code->varDeclarationNumberOfResultTuples).copy());

    // 2. copy watermark
    code->cleanupStmts.push_back(setWatermark(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 3. copy origin id
    code->cleanupStmts.push_back(setOriginId(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 4. emit the buffer to the runtime.
    code->cleanupStmts.push_back(
        emitTupleBuffer(code->varDeclarationExecutionContext, code->varDeclarationResultBuffer, code->varDeclarationWorkerContext)
            .copy());

    return true;
}

bool CCodeGenerator::generateCodeForWatermarkAssigner(Windowing::WatermarkStrategyPtr watermarkStrategy,
                                                      PipelineContextPtr context) {
    auto recordHandler = context->getRecordHandler();
    if (watermarkStrategy->getType() == Windowing::WatermarkStrategy::EventTimeWatermark) {
        auto eventTimeWatermarkStrategy = watermarkStrategy->as<Windowing::EventTimeWatermarkStrategy>();
        auto watermarkFieldName = eventTimeWatermarkStrategy->getField()->getFieldName();
        NES_ASSERT(recordHandler->hasAttribute(watermarkFieldName),
                   "CCodeGenerator: watermark assigner could not get field \"" << watermarkFieldName << "\" from struct");

        auto attribute = AttributeField::create(watermarkFieldName, DataTypeFactory::createUInt64());

        auto tf = getTypeFactory();

        // initiate maxWatermark variable
        // auto maxWatermark = 0;
        auto maxWatermarkVariableDeclaration =
            VariableDeclaration::create(tf->createDataType(attribute->getDataType()), "maxWatermark");
        auto maxWatermarkInitStatement =
            VarDeclStatement(maxWatermarkVariableDeclaration)
                .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) 0))));
        context->code->variableInitStmts.push_back(maxWatermarkInitStatement.copy());

        NES_ASSERT(context->code->structDeclarationInputTuples.size() >= 1, "invalid size of struct input tuples");
        // get the value for current watermark
        // uint64_t currentWatermark = record[index].ts;
        auto currentWatermarkVariableDeclaration =
            VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), "currentWatermark");
        auto tsVariableDeclaration = recordHandler->getAttribute(attribute->getName());

        auto calculateMaxTupleStatement = (*tsVariableDeclaration)
                * Constant(tf->createValueType(DataTypeFactory::createBasicValue(eventTimeWatermarkStrategy->getMultiplier())))
            - Constant(tf->createValueType(DataTypeFactory::createBasicValue(eventTimeWatermarkStrategy->getAllowedLateness())));
        auto currentWatermarkStatement = VarDeclStatement(currentWatermarkVariableDeclaration).assign(calculateMaxTupleStatement);
        context->code->currentCodeInsertionPoint->addStatement(currentWatermarkStatement.copy());

        /*
         * //we have to have this case to make sure that we don't get negative here when allowed lateness is larger that minwatermark
         * if(minWatermark < allowedLateness)
         *      minWatermark = 0
         */
        auto zero = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "0");
        auto setWatermarkToZero = IF(VarRef(currentWatermarkVariableDeclaration) < Constant(tf->createValueType(
                                         DataTypeFactory::createBasicValue(eventTimeWatermarkStrategy->getAllowedLateness()))),
                                     VarRef(currentWatermarkVariableDeclaration).assign(VarRef(zero)));
        context->code->currentCodeInsertionPoint->addStatement(setWatermarkToZero.createCopy());

        // Check and update max watermark if current watermark is greater than maximum watermark
        // if (currentWatermark > maxWatermark) {
        //     maxWatermark = currentWatermark;
        // };
        auto updateMaxWatermarkStatement =
            VarRef(maxWatermarkVariableDeclaration).assign(VarRef(currentWatermarkVariableDeclaration));
        auto ifStatement = IF(VarRef(currentWatermarkVariableDeclaration) > VarRef(maxWatermarkVariableDeclaration),
                              updateMaxWatermarkStatement);
        context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());

        // set the watermark of input buffer based on maximum watermark
        // inputTupleBuffer.setWatermark(maxWatermark);
        auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
        setWatermarkFunctionCall.addParameter(VarRef(maxWatermarkVariableDeclaration));
        auto setWatermarkStatement = VarRef(context->code->varDeclarationInputBuffer).accessRef(setWatermarkFunctionCall);
        context->code->cleanupStmts.push_back(setWatermarkStatement.createCopy());
    } else if (watermarkStrategy->getType() == Windowing::WatermarkStrategy::IngestionTimeWatermark) {
        // get the watermark from attribute field
        // auto watermark_ts = NES::Windowing::getTsFromClock()
        auto tf = getTypeFactory();
        auto watermarkTsVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "watermark_ts");
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(watermarkTsVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));

        // set the watermark
        // inputTupleBuffer.setWatermark(watermark_ts);
        auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
        //TODO currently we do not support allowed lateness for ingestion time windows
        setWatermarkFunctionCall.addParameter(VarRef(watermarkTsVariableDeclaration));
        auto setWatermarkStatement = VarRef(context->code->varDeclarationInputBuffer).accessRef(setWatermarkFunctionCall);

        context->code->currentCodeInsertionPoint->addStatement(setWatermarkStatement.createCopy());
    } else {
        NES_ERROR("CCodeGenerator: cannot generate code for watermark strategy " << watermarkStrategy);
    }

    return true;
}

void CCodeGenerator::generateCodeForWatermarkUpdaterWindow(PipelineContextPtr context, VariableDeclaration handler) {
    auto updateAllWatermarkTsFunctionCall = FunctionCallStatement("updateMaxTs");
    updateAllWatermarkTsFunctionCall.addParameter(getWatermark(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    auto updateAllWatermarkTsFunctionCallStatement = VarRef(handler).accessPtr(updateAllWatermarkTsFunctionCall);

    context->code->cleanupStmts.push_back(updateAllWatermarkTsFunctionCallStatement.createCopy());
}

void CCodeGenerator::generateCodeForWatermarkUpdaterJoin(PipelineContextPtr context, VariableDeclaration handler, bool leftSide) {
    auto updateAllWatermarkTsFunctionCall = FunctionCallStatement("updateMaxTs");
    updateAllWatermarkTsFunctionCall.addParameter(getWatermark(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    auto tf = getTypeFactory();

    if (leftSide) {
        auto leftSideDecl = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "true");
        updateAllWatermarkTsFunctionCall.addParameter(VarRef(leftSideDecl));
    } else {
        auto leftSideDecl = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "false");
        updateAllWatermarkTsFunctionCall.addParameter(VarRef(leftSideDecl));
    }

    auto updateAllWatermarkTsFunctionCallStatement = VarRef(handler).accessPtr(updateAllWatermarkTsFunctionCall);

    context->code->cleanupStmts.push_back(updateAllWatermarkTsFunctionCallStatement.createCopy());
}

void CCodeGenerator::generateTupleBufferSpaceCheck(PipelineContextPtr context,
                                                   VariableDeclaration varDeclResultTuple,
                                                   StructDeclaration structDeclarationResultTuple) {
    NES_DEBUG("CCodeGenerator: Generate code for tuple buffer check");

    auto code = context->code;
    auto tf = getTypeFactory();

    // calculate of the maximal number of tuples per buffer -> (buffer size / tuple size) - 1
    // int64_t maxTuple = (resultTupleBuffer.getBufferSize() / 39) - 1;
    // 1. get the size of one result tuple
    auto resultTupleSize = context->getResultSchema()->getSchemaSizeInBytes();
    // 2. initialize max tuple
    auto maxTupleDeclaration = VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()), "maxTuple");
    // 3. create calculation statement
    auto calculateMaxTupleStatement = getBufferSize(code->varDeclarationResultBuffer)
        / Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) resultTupleSize)));
    auto calculateMaxTupleAssignment = VarDeclStatement(maxTupleDeclaration).assign(calculateMaxTupleStatement);
    // 4. add statement
    code->currentCodeInsertionPoint->addStatement(calculateMaxTupleAssignment.copy());

    // Check if maxTuple is reached. -> maxTuple <= numberOfResultTuples
    auto ifStatement = IF((VarRef(code->varDeclarationNumberOfResultTuples)) >= VarRef(maxTupleDeclaration));
    // add if statement to current code block
    code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());
    // add tuple emit logic to then statement, which is executed if the condition is met.
    // In this case we 1. emit the buffer and 2. allocate a new buffer.
    auto thenStatement = ifStatement.getCompoundStatement();
    // 1.1 set the number of tuples to the output buffer -> resultTupleBuffer.setNumberOfTuples(numberOfResultTuples);
    thenStatement->addStatement(
        setNumberOfTuples(code->varDeclarationResultBuffer, code->varDeclarationNumberOfResultTuples).copy());

    // 1.1 set the origin id to the output buffer -> resultTupleBuffer.setOriginId(numberOfResultTuples);

    thenStatement->addStatement(setOriginId(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());
    // 1.1 set the watermar to the output buffer -> resultTupleBuffer.setWatermark(numberOfResultTuples);
    thenStatement->addStatement(setWatermark(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 1.2 emit the output buffers to the runtime -> pipelineExecutionContext.emitBuffer(resultTupleBuffer);
    thenStatement->addStatement(
        emitTupleBuffer(code->varDeclarationExecutionContext, code->varDeclarationResultBuffer, code->varDeclarationWorkerContext)
            .copy());
    // 2.1 reset the numberOfResultTuples to 0 -> numberOfResultTuples = 0;
    thenStatement->addStatement(VarRef(code->varDeclarationNumberOfResultTuples)
                                    .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) 0))))
                                    .copy());
    // 2.2 allocate a new buffer -> resultTupleBuffer = pipelineExecutionContext.allocateTupleBuffer();
    thenStatement->addStatement(
        VarRef(code->varDeclarationResultBuffer).assign(allocateTupleBuffer(code->varDeclarationExecutionContext)).copy());
    // 2.2 get typed result buffer from resultTupleBuffer -> resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();
    thenStatement->addStatement(
        VarRef(varDeclResultTuple).assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple)).copy());
}

/**
 * Code Generation for the window operator
 * @param window windowdefinition
 * @param context pipeline context
 * @param out
 * @return
 */
bool CCodeGenerator::generateCodeForCompleteWindow(
    Windowing::LogicalWindowDefinitionPtr window,
    QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto windowOperatorHandlerDeclaration =
        getWindowOperatorHandler(context, context->code->varDeclarationExecutionContext, windowOperatorIndex);
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
    auto keyStamp = window->isKeyed() ? window->getOnKey()->getStamp() : window->getWindowAggregation()->on()->getStamp();
    auto getWindowHandlerStatement =
        getAggregationWindowHandler(windowOperatorHandlerDeclaration, keyStamp, window->getWindowAggregation());
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto constStatement = Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) 0)));

    if (context->pipelineName != "SlicingWindowType") {
        context->pipelineName = "CompleteWindowType";
    }

    auto debugDecl = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), context->pipelineName);
    auto debState =
        VarDeclStatement(debugDecl).assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) 0))));
    context->code->variableInitStmts.push_back(debState.copy());

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");

    NES_ASSERT(!window->getWindowAggregation()->getInputStamp()->isUndefined(), "window input type is undefined");
    NES_ASSERT((!window->getWindowAggregation()->getPartialAggregateStamp()->isUndefined()
                || window->getWindowAggregation()->getType() == Windowing::WindowAggregationDescriptor::Avg),
               "window partial type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());

    // get allowed lateness
    //    auto allowedLateness = windowManager->getAllowedLateness();
    auto latenessHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "allowedLateness");
    auto getAllowedLatenessStateVariable = FunctionCallStatement("getAllowedLateness");
    auto allowedLatenessHandlerVariableStatement = VarRef(windowManagerVarDeclaration).accessPtr(getAllowedLatenessStateVariable);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(latenessHandlerVariableDeclaration).assign(allowedLatenessHandlerVariableStatement).copy());

    // Read key value from record
    auto keyVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "key");
    auto recordHandler = context->getRecordHandler();
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration = recordHandler->getAttribute(window->getOnKey()->getFieldName());
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(keyVariableAttributeDeclaration);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration)
                                        .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue((int64_t) 0))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(defaultKeyAssignment));
    }

    // get key handle for current key
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");

    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    // set the default value for window state based on the aggregation function:
    // max: initialize with the lower bound of the data type
    // min: initialize with the upper bound of the data type
    // count & sum: initialize with 0
    // avg : initialize with an empty AVGPartialType
    switch (window->getWindowAggregation()->getType()) {
        case Windowing::WindowAggregationDescriptor::Min: {
            if (auto intType = DataType::as<Integer>(window->getWindowAggregation()->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(intType, std::to_string(intType->upperBound)))));
            } else if (auto floatType = DataType::as<Float>(window->getWindowAggregation()->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(floatType, std::to_string(floatType->upperBound)))));
            }
            break;
        }
        case Windowing::WindowAggregationDescriptor::Max: {
            if (auto intType = DataType::as<Integer>(window->getWindowAggregation()->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(intType, std::to_string(intType->lowerBound)))));
            } else if (auto floatType = DataType::as<Float>(window->getWindowAggregation()->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(floatType, std::to_string(floatType->lowerBound)))));
            }
            break;
        }
        case Windowing::WindowAggregationDescriptor::Sum: {
            getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
            break;
        }
        case Windowing::WindowAggregationDescriptor::Count: {
            getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
            break;
        }
        case Windowing::WindowAggregationDescriptor::Avg: {
            auto aggregationInputType = tf->createDataType(window->getWindowAggregation()->getInputStamp());
            auto avgInitialValueDeclaration = VariableDeclaration::create(
                tf->createAnonymusDataType("Windowing::AVGPartialType<" + aggregationInputType->getCode()->code_ + ">"),
                "initialValue");
            context->code->currentCodeInsertionPoint->addStatement(VarDeclStatement(avgInitialValueDeclaration).createCopy());
            getValueFromKeyHandle.addParameter(VarRefStatement(avgInitialValueDeclaration));
            break;
        }
        default: {
            NES_FATAL_ERROR("CCodeGenerator: Window Handler - could not cast aggregation type");
        }
    }

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));

    context->code->currentCodeInsertionPoint->addStatement(windowStateVariableStatement.copy());

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (window->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        NES_ASSERT(context->code->structDeclarationInputTuples.size() >= 1, "invalid number of input tuples");
        auto timeCharacteristicField = window->getWindowType()->getTimeCharacteristic()->getField()->getName();
        auto tsVariableDeclaration = recordHandler->getAttribute(timeCharacteristicField);

        /**
         * calculateUnitMultiplier => cal to ms
         */
        auto multiplier = window->getWindowType()->getTimeCharacteristic()->getTimeUnit().getMultiplier();
        //In this case we need to multiply the ts with the multiplier to get ms
        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(tsVariableDeclaration)
            * Constant(tf->createValueType(DataTypeFactory::createBasicValue(multiplier)));
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    //within the loop
    //get min watermark
    auto minWatermarkVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "minWatermark");
    auto getMinWatermarkStateVariable = FunctionCallStatement("getMinWatermark");
    auto minWatermarkHandlerVariableStatement =
        VarDeclStatement(minWatermarkVariableDeclaration)
            .assign(VarRef(windowHandlerVariableDeclration).accessPtr(getMinWatermarkStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(minWatermarkHandlerVariableStatement));

    auto ifStatementSmallerMinWatermark =
        IF(VarRef(currentTimeVariableDeclaration) < VarRef(minWatermarkVariableDeclaration), Continue());
    context->code->currentCodeInsertionPoint->addStatement(ifStatementSmallerMinWatermark.createCopy());

    // lock slice
    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    //only in debug mode add the key for debugging
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    context->code->currentCodeInsertionPoint->addStatement(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream).copy());

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(currentSliceIndexVariableStatement.copy());

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    auto partialAggregatesVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(assignment.copy());

    // update partial aggregate
    auto partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    NES_ASSERT(context->code->structDeclarationInputTuples.size() >= 1, "invalid number of input tuples");
    generatableWindowAggregation->compileLiftCombine(context->code->currentCodeInsertionPoint,
                                                     partialRef,
                                                     context->getRecordHandler());

    // get the slice metadata aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getSliceMetadata = FunctionCallStatement("getSliceMetadata");
    auto getSliceMetadataCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceMetadata);
    auto sliceMetadataDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "sliceMetaData");
    auto sliceAssigment = VarDeclStatement(sliceMetadataDeclaration).assign(getSliceMetadataCall);
    context->code->currentCodeInsertionPoint->addStatement(sliceAssigment.copy());

    auto getSliceCall = FunctionCallStatement("incrementRecordsPerSlice");
    auto updateSliceStatement = VarRef(sliceMetadataDeclaration)[current_slice_ref].accessRef(getSliceCall);
    context->code->currentCodeInsertionPoint->addStatement(updateSliceStatement.createCopy());

    // windowHandler->trigger();
    switch (window->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnRecord: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->currentCodeInsertionPoint->addStatement(call);
            break;
        }
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterWindow(context, windowHandlerVariableDeclration);
    return true;
}

bool CCodeGenerator::generateCodeForSlicingWindow(
    Windowing::LogicalWindowDefinitionPtr window,
    QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorId) {
    NES_DEBUG("CCodeGenerator::generateCodeForSlicingWindow with " << window << " pipeline " << context);
    //NOTE: the distinction currently only happens in the trigger
    context->pipelineName = "SlicingWindowType";
    return generateCodeForCompleteWindow(window, generatableWindowAggregation, context, windowOperatorId);
}

uint64_t CCodeGenerator::generateJoinSetup(Join::LogicalJoinDefinitionPtr join, PipelineContextPtr context, uint64_t id) {
    if (context->arity == PipelineContext::BinaryLeft) {
        return 0;
    }

    auto tf = getTypeFactory();
    NES_ASSERT(join, "invalid join definition");
    NES_ASSERT(!join->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!join->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(join->getLeftJoinKey()->getStamp()->isEquals(join->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(join->getLeftStreamType() != nullptr && !join->getLeftStreamType()->fields.empty(), "left join type is undefined");
    NES_ASSERT(join->getRightStreamType() != nullptr && !join->getRightStreamType()->fields.empty(),
               "right join type is undefined");
    NES_ASSERT(context->arity != PipelineContext::Unary, "unary operator detected but join codegen invoked");

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);
    auto handlers = context->getOperatorHandlers();
    NES_ASSERT(handlers.size() == 1, "invalid size");
    int64_t joinOperatorHandlerIndex = 0;
    Join::JoinOperatorHandlerPtr joinOperatorHandler = std::dynamic_pointer_cast<Join::JoinOperatorHandler>(handlers[0]);
    NES_ASSERT(joinOperatorHandler != nullptr, "invalid join handler");

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex =
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto joinDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinDefinition");
    auto getWindowDefinitionCall = call("getJoinDefinition");
    auto windowDefinitionStatement =
        VarDeclStatement(joinDefDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());

    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement =
        VarDeclStatement(resultSchemaDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyType = tf->createDataType(join->getLeftJoinKey()->getStamp());
    auto policy = join->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
        NES_WARNING("This mode is not supported anymore");
    } else if (policy->getPolicyType() == Windowing::triggerOnWatermarkChange) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnWatermarkChangeTriggerPolicy::create");
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
    }
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto action = join->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Join::JoinActionType::LazyNestedLoopJoin) {
        auto createTriggerActionCall = call("Join::ExecutableNestedLoopJoinTriggerAction<" + keyType->getCode()->code_
                                            + ", InputTupleLeft, InputTupleRight>::create");
        createTriggerActionCall->addParameter(VarRef(joinDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
    }

    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto joinHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto createAggregationWindowHandlerCall =
        call("Join::JoinHandler<" + keyType->getCode()->code_ + ", InputTupleLeft, InputTupleRight>::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(joinDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    createAggregationWindowHandlerCall->addParameter(VarRef(idParam));

    auto windowHandlerStatement = VarDeclStatement(joinHandler).assign(createAggregationWindowHandlerCall);
    setupScope->addStatement(windowHandlerStatement.copy());

    // windowOperatorHandler->setWindowHandler(windowHandler);
    auto setWindowHandlerCall = call("setJoinHandler");
    setWindowHandlerCall->addParameter(VarRef(joinHandler));
    auto setWindowHandlerStatement = VarRef(windowOperatorHandlerDeclaration).accessPtr(setWindowHandlerCall);
    setupScope->addStatement(setWindowHandlerStatement.copy());

    // setup window handler
    auto getSharedFromThis = call("shared_from_this");
    auto setUpWindowHandlerCall = call("setup");
    setUpWindowHandlerCall->addParameter(VarRef(context->code->varDeclarationExecutionContext).accessRef(getSharedFromThis));

    auto setupWindowHandlerStatement = VarRef(joinHandler).accessPtr(setUpWindowHandlerCall);
    setupScope->addStatement(setupWindowHandlerStatement.copy());
    return joinOperatorHandlerIndex;
}

uint64_t CCodeGenerator::generateCodeForJoinSinkSetup(Join::LogicalJoinDefinitionPtr join,
                                                      PipelineContextPtr context,
                                                      uint64_t id,
                                                      Join::JoinOperatorHandlerPtr joinOperatorHandler) {
    auto tf = getTypeFactory();
    NES_ASSERT(join, "invalid join definition");
    NES_ASSERT(!join->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!join->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(join->getLeftJoinKey()->getStamp()->isEquals(join->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(join->getLeftStreamType() != nullptr && !join->getLeftStreamType()->fields.empty(), "left join type is undefined");
    NES_ASSERT(join->getRightStreamType() != nullptr && !join->getRightStreamType()->fields.empty(),
               "right join type is undefined");

    auto rightTypeStruct = getStructDeclarationFromSchema("InputTupleRight", join->getRightStreamType());
    auto leftTypeStruct = getStructDeclarationFromSchema("InputTupleLeft", join->getLeftStreamType());
    context->code->structDeclarationInputTuples.emplace_back(leftTypeStruct);

    auto pipelineExecutionContextType = tf->createAnonymusDataType("NodeEngine::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    int64_t joinOperatorHandlerIndex = context->registerOperatorHandler(joinOperatorHandler);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex =
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto joinDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinDefinition");
    auto getWindowDefinitionCall = call("getJoinDefinition");
    auto windowDefinitionStatement =
        VarDeclStatement(joinDefDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());

    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement =
        VarDeclStatement(resultSchemaDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyType = tf->createDataType(join->getLeftJoinKey()->getStamp());
    auto policy = join->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
        NES_WARNING("This mode is not supported anymore");
    } else if (policy->getPolicyType() == Windowing::triggerOnWatermarkChange) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnWatermarkChangeTriggerPolicy::create");
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
    }
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto action = join->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Join::JoinActionType::LazyNestedLoopJoin) {
        auto createTriggerActionCall = call("Join::ExecutableNestedLoopJoinTriggerAction<" + keyType->getCode()->code_
                                            + ", InputTupleLeft, InputTupleRight>::create");
        createTriggerActionCall->addParameter(VarRef(joinDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
    }

    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto joinHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto createAggregationWindowHandlerCall =
        call("Join::JoinHandler<" + keyType->getCode()->code_ + ", InputTupleLeft, InputTupleRight>::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(joinDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    createAggregationWindowHandlerCall->addParameter(VarRef(idParam));

    auto windowHandlerStatement = VarDeclStatement(joinHandler).assign(createAggregationWindowHandlerCall);
    setupScope->addStatement(windowHandlerStatement.copy());

    // windowOperatorHandler->setWindowHandler(windowHandler);
    auto setWindowHandlerCall = call("setJoinHandler");
    setWindowHandlerCall->addParameter(VarRef(joinHandler));
    auto setWindowHandlerStatement = VarRef(windowOperatorHandlerDeclaration).accessPtr(setWindowHandlerCall);
    setupScope->addStatement(setWindowHandlerStatement.copy());

    // setup window handler
    auto getSharedFromThis = call("shared_from_this");
    auto setUpWindowHandlerCall = call("setup");
    setUpWindowHandlerCall->addParameter(VarRef(context->code->varDeclarationExecutionContext).accessRef(getSharedFromThis));

    auto setupWindowHandlerStatement = VarRef(joinHandler).accessPtr(setUpWindowHandlerCall);
    setupScope->addStatement(setupWindowHandlerStatement.copy());
    return joinOperatorHandlerIndex;
}

bool CCodeGenerator::generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef,
                                         PipelineContextPtr context,
                                         uint64_t operatorHandlerIndex) {
    NES_DEBUG("join input=" << context->inputSchema->toString() << " aritiy=" << context->arity
                            << " out=" << joinDef->getOutputSchema()->toString());

    auto tf = getTypeFactory();

    if (context->arity == PipelineContext::BinaryLeft) {
        auto rightTypeStruct = getStructDeclarationFromSchema("InputTupleRight", joinDef->getRightStreamType());
        context->code->structDeclarationInputTuples.emplace_back(rightTypeStruct);
    } else {
        auto leftTypeStruct = getStructDeclarationFromSchema("InputTupleLeft", joinDef->getLeftStreamType());
        context->code->structDeclarationInputTuples.emplace_back(leftTypeStruct);
    }

    NES_ASSERT(joinDef, "invalid join definition");
    NES_ASSERT(!joinDef->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!joinDef->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(joinDef->getLeftJoinKey()->getStamp()->isEquals(joinDef->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(joinDef->getLeftStreamType() != nullptr && !joinDef->getLeftStreamType()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(joinDef->getRightStreamType() != nullptr && !joinDef->getRightStreamType()->fields.empty(),
               "right join type is undefined");
    NES_ASSERT(context->arity != PipelineContext::Unary, "unary operator detected but join codegen invoked");

    auto code = context->code;

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");
    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowJoinVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");

    auto windowOperatorHandlerDeclaration =
        getJoinOperatorHandler(context, context->code->varDeclarationExecutionContext, operatorHandlerIndex);

    auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration,
                                                        joinDef->getLeftJoinKey()->getStamp(),
                                                        "InputTupleLeft",
                                                        "InputTupleRight");
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());

    //-------------------------

    auto getWindowManagerStatement = getWindowManager(windowJoinVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    if (context->arity == PipelineContext::BinaryLeft) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side left");
        auto getWindowStateStatement = getLeftJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    } else if (context->arity == PipelineContext::BinaryRight) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side right");
        auto getWindowStateStatement = getRightJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    }

    /**
    * within the loop
    */
    // Read key value from record
    // int64_t key = windowTuples[recordIndex].key;
    //TODO this is an ugly hack because we cannot create empty VariableDeclaration and we want it outide the if/else
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "_");
    auto recordHandler = context->getRecordHandler();

    if (context->arity == PipelineContext::BinaryLeft) {
        auto joinKeyFieldName = joinDef->getLeftJoinKey()->getFieldName();
        keyVariableDeclaration =
            VariableDeclaration::create(tf->createDataType(joinDef->getLeftJoinKey()->getStamp()), joinKeyFieldName);

        NES_ASSERT2_FMT(recordHandler->hasAttribute(joinKeyFieldName),
                        "join key is not defined on input tuple << " << joinKeyFieldName);

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);

        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto joinKeyFieldName = joinDef->getRightJoinKey()->getFieldName();

        keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(joinDef->getRightJoinKey()->getStamp()),
                                                             joinDef->getRightJoinKey()->getFieldName());

        NES_ASSERT(recordHandler->hasAttribute(joinKeyFieldName), "join key is not defined on iput tuple");

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    }

    // get key handle for current key
    // auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());

    // access window slice state from state variable via key
    // auto windowState = key_value_handle.value();
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(windowStateVariableStatement.copy());

    // get current timestamp
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (joinDef->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        //      auto current_ts = NES::Windowing::getTsFromClock();
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        //      auto current_ts = inputTuples[recordIndex].time //the time value of the key
        //TODO: this has to be changed once we close #1543 and thus we would have 2 times the attribute
        //Extract the name of the window field used for time characteristics
        std::string windowTimeStampFieldName = joinDef->getWindowType()->getTimeCharacteristic()->getField()->getName();
        if (context->arity == PipelineContext::BinaryRight) {
            NES_DEBUG("windowTimeStampFieldName bin right=" << windowTimeStampFieldName);

            //Extract the schema of the right side
            auto rightSchema = joinDef->getRightStreamType();
            //Extract the field name without attribute name resolution
            auto trimmedWindowFieldName =
                windowTimeStampFieldName.substr(windowTimeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR),
                                                windowTimeStampFieldName.length());
            //Extract the first field from right schema and trim it to find the schema qualifier for the right side
            //TODO: I know I know this is really not nice but we will fix in with the other issue above
            bool found = false;
            for (auto& field : rightSchema->fields) {
                if (field->getName().find(trimmedWindowFieldName) != std::string::npos) {
                    windowTimeStampFieldName = field->getName();
                    found = true;
                }
            }
            NES_ASSERT(found, " right schema does not contain a timestamp attribute");
        } else {
            NES_DEBUG("windowTimeStampFieldName bin left=" << windowTimeStampFieldName);
        }

        auto tsVariableReference = recordHandler->getAttribute(windowTimeStampFieldName);

        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(tsVariableReference);
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

    // update slices
    // windowManager->sliceStream(current_ts, windowState);
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    // uint64_t current_slice_index = windowState->getSliceIndexByTs(current_ts);
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(currentSliceIndexVariableStatement));

    // append to the join state
    auto joinStateCall = FunctionCallStatement("append");
    joinStateCall.addParameter(VarRef(currentSliceIndexVariableDeclaration));
    joinStateCall.addParameter(
        VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)]);
    auto getJoinStateCall = VarRef(windowStateVariableDeclaration).accessPtr(joinStateCall);
    context->code->currentCodeInsertionPoint->addStatement(getJoinStateCall.copy());

    // joinHandler->trigger();
    switch (joinDef->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowJoinVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);
    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterJoin(context, windowJoinVariableDeclration, context->arity == PipelineContext::BinaryLeft);
    return true;
}

bool CCodeGenerator::generateCodeForJoinBuild(Join::LogicalJoinDefinitionPtr joinDef,
                                              PipelineContextPtr context,
                                              Join::JoinOperatorHandlerPtr joinOperatorHandler,
                                              QueryCompilation::JoinBuildSide buildSide) {
    NES_DEBUG("join input=" << context->inputSchema->toString() << " aritiy=" << buildSide
                            << " out=" << joinDef->getOutputSchema()->toString());

    auto tf = getTypeFactory();

    if (buildSide == QueryCompilation::JoinBuildSide::Left) {
        auto rightTypeStruct = getStructDeclarationFromSchema("InputTupleRight", joinDef->getRightStreamType());
        context->code->structDeclarationInputTuples.emplace_back(rightTypeStruct);
    } else {
        auto leftTypeStruct = getStructDeclarationFromSchema("InputTupleLeft", joinDef->getLeftStreamType());
        context->code->structDeclarationInputTuples.emplace_back(leftTypeStruct);
    }

    NES_ASSERT(joinDef, "invalid join definition");
    NES_ASSERT(!joinDef->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!joinDef->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(joinDef->getLeftJoinKey()->getStamp()->isEquals(joinDef->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(joinDef->getLeftStreamType() != nullptr && !joinDef->getLeftStreamType()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(joinDef->getRightStreamType() != nullptr && !joinDef->getRightStreamType()->fields.empty(),
               "right join type is undefined");

    auto code = context->code;

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");
    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowJoinVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto operatorHandlerIndex = context->registerOperatorHandler(joinOperatorHandler);
    auto windowOperatorHandlerDeclaration =
        getJoinOperatorHandler(context, context->code->varDeclarationExecutionContext, operatorHandlerIndex);

    if (buildSide == QueryCompilation::Left) {
        auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration,
                                                            joinDef->getLeftJoinKey()->getStamp(),
                                                            "InputTuple",
                                                            "InputTupleRight");
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());
    } else {
        auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration,
                                                            joinDef->getLeftJoinKey()->getStamp(),
                                                            "InputTupleLeft",
                                                            "InputTuple");
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());
    }

    //-------------------------

    auto getWindowManagerStatement = getWindowManager(windowJoinVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    if (buildSide == QueryCompilation::JoinBuildSide::Left) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side left");
        auto getWindowStateStatement = getLeftJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    } else if (buildSide == QueryCompilation::JoinBuildSide::Right) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side right");
        auto getWindowStateStatement = getRightJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    }

    /**
    * within the loop
    */
    // Read key value from record
    // int64_t key = windowTuples[recordIndex].key;
    //TODO this is an ugly hack because we cannot create empty VariableDeclaration and we want it outide the if/else
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "_");
    auto recordHandler = context->getRecordHandler();

    if (buildSide == QueryCompilation::JoinBuildSide::Left) {
        auto joinKeyFieldName = joinDef->getLeftJoinKey()->getFieldName();
        keyVariableDeclaration =
            VariableDeclaration::create(tf->createDataType(joinDef->getLeftJoinKey()->getStamp()), joinKeyFieldName);

        NES_ASSERT2_FMT(recordHandler->hasAttribute(joinKeyFieldName),
                        "join key is not defined on input tuple << " << joinKeyFieldName);

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);

        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto joinKeyFieldName = joinDef->getRightJoinKey()->getFieldName();

        keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(joinDef->getRightJoinKey()->getStamp()),
                                                             joinDef->getRightJoinKey()->getFieldName());

        NES_ASSERT(recordHandler->hasAttribute(joinKeyFieldName), "join key is not defined on iput tuple");

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    }

    // get key handle for current key
    // auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());

    // access window slice state from state variable via key
    // auto windowState = key_value_handle.value();
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(windowStateVariableStatement.copy());

    // get current timestamp
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (joinDef->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        //      auto current_ts = NES::Windowing::getTsFromClock();
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        //      auto current_ts = inputTuples[recordIndex].time //the time value of the key
        //TODO: this has to be changed once we close #1543 and thus we would have 2 times the attribute
        //Extract the name of the window field used for time characteristics
        std::string windowTimeStampFieldName = joinDef->getWindowType()->getTimeCharacteristic()->getField()->getName();
        if (buildSide == QueryCompilation::JoinBuildSide::Right) {
            NES_DEBUG("windowTimeStampFieldName bin right=" << windowTimeStampFieldName);

            //Extract the schema of the right side
            auto rightSchema = joinDef->getRightStreamType();
            //Extract the field name without attribute name resolution
            auto trimmedWindowFieldName =
                windowTimeStampFieldName.substr(windowTimeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR),
                                                windowTimeStampFieldName.length());
            //Extract the first field from right schema and trim it to find the schema qualifier for the right side
            //TODO: I know I know this is really not nice but we will fix in with the other issue above
            bool found = false;
            for (auto& field : rightSchema->fields) {
                if (field->getName().find(trimmedWindowFieldName) != std::string::npos) {
                    windowTimeStampFieldName = field->getName();
                    found = true;
                }
            }
            NES_ASSERT(found, " right schema does not contain a timestamp attribute");
        } else {
            NES_DEBUG("windowTimeStampFieldName bin left=" << windowTimeStampFieldName);
        }

        auto tsVariableReference = recordHandler->getAttribute(windowTimeStampFieldName);

        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(tsVariableReference);
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

    // update slices
    // windowManager->sliceStream(current_ts, windowState);
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    // uint64_t current_slice_index = windowState->getSliceIndexByTs(current_ts);
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(currentSliceIndexVariableStatement));

    // append to the join state
    auto joinStateCall = FunctionCallStatement("append");
    joinStateCall.addParameter(VarRef(currentSliceIndexVariableDeclaration));
    joinStateCall.addParameter(
        VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)]);
    auto getJoinStateCall = VarRef(windowStateVariableDeclaration).accessPtr(joinStateCall);
    context->code->currentCodeInsertionPoint->addStatement(getJoinStateCall.copy());

    // joinHandler->trigger();
    switch (joinDef->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowJoinVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);
    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterJoin(context,
                                        windowJoinVariableDeclration,
                                        buildSide == QueryCompilation::JoinBuildSide::Left);
    return true;
}

bool CCodeGenerator::generateCodeForCombiningWindow(
    Windowing::LogicalWindowDefinitionPtr window,
    QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for combine window " << window);
    auto code = context->code;

    if (window->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Type::Combining) {
        context->pipelineName = "combiningWindowType";
    } else {
        context->pipelineName = "sliceMergingWindowType";
    }

    auto debugDecl = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), context->pipelineName);
    auto debState =
        VarDeclStatement(debugDecl).assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) 0))));
    context->code->variableInitStmts.push_back(debState.copy());

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowOperatorHandlerDeclaration =
        getWindowOperatorHandler(context, context->code->varDeclarationExecutionContext, windowOperatorIndex);
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");

    auto keyStamp = window->isKeyed() ? window->getOnKey()->getStamp() : window->getWindowAggregation()->on()->getStamp();

    auto getWindowHandlerStatement =
        getAggregationWindowHandler(windowOperatorHandlerDeclaration, keyStamp, window->getWindowAggregation());
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());

    /**
   * within the loop
   */

    //get min watermark
    auto minWatermarkVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "minWatermark");
    auto getMinWatermarkStateVariable = FunctionCallStatement("getMinWatermark");
    auto minWatermarkHandlerVariableStatement =
        VarDeclStatement(minWatermarkVariableDeclaration)
            .assign(VarRef(windowHandlerVariableDeclration).accessPtr(getMinWatermarkStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(minWatermarkHandlerVariableStatement.copy());

    //        NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>* state_variable = (NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>*) state_var;
    auto stateVariableDeclaration =
        VariableDeclaration::create(tf->createPointer(tf->createAnonymusDataType(
                                        "NES::NodeEngine::StateVariable<int64_t, NES::Windowing::WindowSliceStore<int64_t>*>")),
                                    "state_variable");

    auto stateVarDeclarationStatement =
        VarDeclStatement(stateVariableDeclaration)
            .assign(TypeCast(VarRef(windowStateVarDeclaration), stateVariableDeclaration.getDataType()));
    context->code->currentCodeInsertionPoint->addStatement(stateVarDeclarationStatement.copy());

    // Read key value from record
    //        int64_t key = windowTuples[recordIndex].key;

    //TODO this is not nice but we cannot create an empty one or a ptr
    auto keyVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "key");
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration = context->getRecordHandler()->getAttribute(window->getOnKey()->getFieldName());
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(keyVariableAttributeDeclaration);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto defaultKeyAssignment =
            VarDeclStatement(keyVariableDeclaration).assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(0L))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(defaultKeyAssignment));
    }

    // get key handle for current key
    //        auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(stateVariableDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());

    //    auto windowState = key_value_handle.valueOrDefault(0);
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(0L))));
    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(windowStateVariableStatement));

    // get current timestamp
    auto currentTimeVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start");
    auto recordStartAttributeRef = context->getRecordHandler()->getAttribute(
        context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start");

    if (window->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(recordStartAttributeRef);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        currentTimeVariableDeclaration = VariableDeclaration::create(
            tf->createAnonymusDataType("auto"),
            context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start");
        auto tsVariableDeclarationStatement =
            VarDeclStatement(currentTimeVariableDeclaration).assign(recordStartAttributeRef->copy());
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    //        if (ts < minWatermark)
    //          {continue;}
    auto ifStatementSmallerMinWatermark =
        IF(VarRef(currentTimeVariableDeclaration) < VarRef(minWatermarkVariableDeclaration), Continue());
    context->code->currentCodeInsertionPoint->addStatement(ifStatementSmallerMinWatermark.createCopy());

    // get current timestamp
    auto currentCntVariable =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "cnt");
    auto recordCntFieldRef = context->getRecordHandler()->getAttribute(
        context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "cnt");
    auto getCurrentCntStatement = VarDeclStatement(currentCntVariable).assign(recordCntFieldRef);
    context->code->currentCodeInsertionPoint->addStatement(getCurrentCntStatement.copy());

    // lock slice
    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto sliceStreamStatement = VarRef(windowManagerVarDeclaration).accessPtr(sliceStream).copy();
    context->code->currentCodeInsertionPoint->addStatement(sliceStreamStatement);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(currentSliceIndexVariableStatement.copy());

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    generatableWindowAggregation->compileLiftCombine(context->code->currentCodeInsertionPoint,
                                                     partialRef,
                                                     context->getRecordHandler());

    // get the slice metadata aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getSliceMetadata = FunctionCallStatement("getSliceMetadata");
    auto getSliceMetadataCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceMetadata);
    VariableDeclaration sliceMetadataDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "sliceMetaData");
    auto sliceAssigment = VarDeclStatement(sliceMetadataDeclaration).assign(getSliceMetadataCall);
    context->code->currentCodeInsertionPoint->addStatement(sliceAssigment.copy());

    auto getSliceCall = FunctionCallStatement("incrementRecordsPerSliceByValue");
    getSliceCall.addParameter(VarRef(currentCntVariable));
    auto updateSliceStatement = VarRef(sliceMetadataDeclaration)[current_slice_ref].accessRef(getSliceCall);
    context->code->currentCodeInsertionPoint->addStatement(updateSliceStatement.createCopy());

    // windowHandler->trigger();
    switch (window->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnRecord: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = VarRef(windowHandlerVariableDeclration).accessPtr(trigger).copy();
            context->code->currentCodeInsertionPoint->addStatement(call);
            break;
        }
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = VarRef(windowHandlerVariableDeclration).accessPtr(trigger).copy();
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterWindow(context, windowHandlerVariableDeclration);
    return true;
}

/*
 * Generates the code for initializing partialAggregate and executable aggregation depending on its type.
 * For example, the sum aggregate with int
 * auto partialAggregateInitialValue = 0;
 * Windowing::ExecutableSumAggregation<int64_t>::create()
 */
void CCodeGenerator::generateCodeForAggregationInitialization(BlockScopeStatementPtr setupScope,
                                                              VariableDeclaration executableAggregation,
                                                              VariableDeclaration partialAggregateInitialValue,
                                                              GeneratableDataTypePtr aggregationInputType,
                                                              Windowing::WindowAggregationDescriptorPtr aggregation) {
    FunctionCallStatementPtr createAggregateCall;
    auto tf = getTypeFactory();

    // If the the aggregate is Avg, we initialize the partialAggregate with an empty AVGPartialType<InputType>
    if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Avg) {
        auto partialAggregateInitStatement =
            VarDeclStatement(partialAggregateInitialValue)
                .assign(call("Windowing::AVGPartialType<" + aggregationInputType->getCode()->code_ + ">"));
        setupScope->addStatement(partialAggregateInitStatement.copy());
        createAggregateCall = call("Windowing::ExecutableAVGAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
    } else {
        // If the the aggregate is Avg, we initialize the partialAggregate with 0
        auto partialAggregateInitStatement =
            VarDeclStatement(partialAggregateInitialValue)
                .assign(Constant(
                    tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))));

        if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Sum) {
            createAggregateCall =
                call("Windowing::ExecutableSumAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Count) {
            createAggregateCall =
                call("Windowing::ExecutableCountAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Min) {
            // If the the aggregate is Min, we initialize the partialAggregate with the upper bound of the type of the aggregated field
            std::string upperBoundstr;
            if (auto intType = DataType::as<Integer>(aggregation->getPartialAggregateStamp())) {
                upperBoundstr = std::to_string(intType->upperBound);
            } else if (auto floatType = DataType::as<Float>(aggregation->getPartialAggregateStamp())) {
                upperBoundstr = std::to_string(floatType->upperBound);
            }
            partialAggregateInitStatement =
                VarDeclStatement(partialAggregateInitialValue)
                    .assign(Constant(
                        tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), upperBoundstr))));

            createAggregateCall =
                call("Windowing::ExecutableMinAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Max) {
            // If the the aggregate is Max, we initialize the partialAggregate with the lower bound of the type of the aggregated field
            std::string lowerBoundStr;
            if (auto intType = DataType::as<Integer>(aggregation->getPartialAggregateStamp())) {
                lowerBoundStr = std::to_string(intType->lowerBound);
            } else if (auto floatType = DataType::as<Float>(aggregation->getPartialAggregateStamp())) {
                lowerBoundStr = std::to_string(floatType->lowerBound);
            }
            partialAggregateInitStatement =
                VarDeclStatement(partialAggregateInitialValue)
                    .assign(Constant(
                        tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), lowerBoundStr))));

            createAggregateCall =
                call("Windowing::ExecutableMaxAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else {
            NES_FATAL_ERROR("Aggregation Handler: aggregation=" << aggregation->getType() << " not implemented");
        }
        // add the partial aggregation initialization to the code
        setupScope->addStatement(partialAggregateInitStatement.copy());
    }

    // add the executable aggregation initialization to the code
    auto statement = VarDeclStatement(executableAggregation).assign(createAggregateCall);
    setupScope->addStatement(statement.copy());
}

uint64_t CCodeGenerator::generateWindowSetup(Windowing::LogicalWindowDefinitionPtr window,
                                             SchemaPtr,
                                             PipelineContextPtr context,
                                             uint64_t id,
                                             Windowing::WindowOperatorHandlerPtr windowOperatorHandler) {
    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);
    auto windowOperatorIndex = context->registerOperatorHandler(windowOperatorHandler);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::WindowOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());

    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement =
        VarDeclStatement(resultSchemaDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyStamp = window->isKeyed() ? window->getOnKey()->getStamp() : window->getWindowAggregation()->on()->getStamp();
    auto keyType = tf->createDataType(keyStamp);

    auto aggregation = window->getWindowAggregation();
    auto aggregationInputType = tf->createDataType(aggregation->getInputStamp());

    // If aggregation is AVG, initiate an AVGPartialType
    std::string partialAggregateTypeCode;
    if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Avg) {
        partialAggregateTypeCode = "Windowing::AVGPartialType<" + aggregationInputType->getCode()->code_ + ">";
    } else {
        // otherwise, get the code directly from the partialAggregateStamp
        auto partialAggregateType = tf->createDataType(aggregation->getPartialAggregateStamp());
        partialAggregateTypeCode = partialAggregateType->getCode()->code_;
    }

    auto partialAggregateInitialValue =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partialAggregateInitialValue");
    auto finalAggregateType = tf->createDataType(aggregation->getFinalAggregateStamp());
    auto executableAggregation = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "aggregation");

    // generate code for aggregation initialization
    generateCodeForAggregationInitialization(setupScope,
                                             executableAggregation,
                                             partialAggregateInitialValue,
                                             aggregationInputType,
                                             aggregation);

    auto policy = window->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
        NES_WARNING("This mode is not supported anymore");
    } else if (policy->getPolicyType() == Windowing::triggerOnWatermarkChange) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnWatermarkChangeTriggerPolicy::create");
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
    }

    auto action = window->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Windowing::WindowAggregationTriggerAction) {
        auto createTriggerActionCall = call("Windowing::ExecutableCompleteAggregationTriggerAction<" + keyType->getCode()->code_
                                            + "," + aggregationInputType->getCode()->code_ + "," + partialAggregateTypeCode + ","
                                            + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        createTriggerActionCall->addParameter(VarRef(partialAggregateInitialValue));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else if (action->getActionType() == Windowing::SliceAggregationTriggerAction) {
        auto createTriggerActionCall = call("Windowing::ExecutableSliceAggregationTriggerAction<" + keyType->getCode()->code_
                                            + "," + aggregationInputType->getCode()->code_ + "," + partialAggregateTypeCode + ","
                                            + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
    }

    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto windowHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
    auto createAggregationWindowHandlerCall =
        call("Windowing::AggregationWindowHandler<" + keyType->getCode()->code_ + "," + aggregationInputType->getCode()->code_
             + "," + partialAggregateTypeCode + "," + finalAggregateType->getCode()->code_ + ">::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(windowDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableAggregation));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    createAggregationWindowHandlerCall->addParameter(VarRef(idParam));
    createAggregationWindowHandlerCall->addParameter(VarRef(partialAggregateInitialValue));

    auto windowHandlerStatement = VarDeclStatement(windowHandler).assign(createAggregationWindowHandlerCall);
    setupScope->addStatement(windowHandlerStatement.copy());

    // windowOperatorHandler->setWindowHandler(windowHandler);
    auto setWindowHandlerCall = call("setWindowHandler");
    setWindowHandlerCall->addParameter(VarRef(windowHandler));
    auto setWindowHandlerStatement = VarRef(windowOperatorHandlerDeclaration).accessPtr(setWindowHandlerCall);
    setupScope->addStatement(setWindowHandlerStatement.copy());

    // setup window handler
    auto getSharedFromThis = call("shared_from_this");
    auto setUpWindowHandlerCall = call("setup");
    setUpWindowHandlerCall->addParameter(VarRef(context->code->varDeclarationExecutionContext).accessRef(getSharedFromThis));

    auto setupWindowHandlerStatement = VarRef(windowHandler).accessPtr(setUpWindowHandlerCall);
    setupScope->addStatement(setupWindowHandlerStatement.copy());

    return windowOperatorIndex;
}

std::string CCodeGenerator::generateCode(PipelineContextPtr context) {
    auto code = context->code;
    // FunctionDeclaration main_function =
    auto tf = getTypeFactory();

    auto setupFunction = FunctionDefinition::create("setup")
                             ->addParameter(code->varDeclarationExecutionContext)
                             ->returns(tf->createDataType(DataTypeFactory::createUInt32()));

    for (auto setupScope : context->getSetupScopes()) {
        setupFunction->addStatement(setupScope);
    }

    setupFunction->addStatement(ReturnStatement::create(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0))))
            .createCopy()));

    auto startFunction = FunctionDefinition::create("start")
                             ->addParameter(code->varDeclarationExecutionContext)
                             ->returns(tf->createDataType(DataTypeFactory::createUInt32()));

    for (auto startScope : context->getStartScopes()) {
        startFunction->addStatement(startScope);
    }

    startFunction->addStatement(ReturnStatement::create(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0))))
            .createCopy()));

    auto functionBuilder = FunctionDefinition::create("execute")
                               ->returns(tf->createAnonymusDataType("ExecutionResult"))
                               ->addParameter(code->varDeclarationInputBuffer)
                               ->addParameter(code->varDeclarationExecutionContext)
                               ->addParameter(code->varDeclarationWorkerContext);
    code->variableDeclarations.push_back(code->varDeclarationNumberOfResultTuples);
    for (auto& variableDeclaration : code->variableDeclarations) {
        functionBuilder->addVariableDeclaration(variableDeclaration);
    }
    for (auto& variableStatement : code->variableInitStmts) {
        functionBuilder->addStatement(variableStatement);
    }

    /* here comes the code for the processing loop */
    functionBuilder->addStatement(code->forLoopStmt);

    /* add statements executed after the for loop, for example cleanup code */
    for (auto& stmt : code->cleanupStmts) {
        functionBuilder->addStatement(stmt);
    }

    /* add return statement */
    functionBuilder->addStatement(code->returnStmt);

    FileBuilder fileBuilder = FileBuilder::create("query.cpp");
    /* add core declarations */
    for (auto& decl : code->structDeclarationInputTuples) {
        fileBuilder.addDeclaration(decl.copy());
    }

    /* add generic declarations by operators*/
    for (auto& typeDeclaration : code->typeDeclarations) {
        fileBuilder.addDeclaration(typeDeclaration.copy());
    }

    // define param to use in the ctor of pipeline to determine its arity.
    ExpressionStatmentPtr arityStatement;
    switch (context->arity) {
        case PipelineContext::Unary: {
            arityStatement = std::make_shared<ConstantExpressionStatement>(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "Unary")));
            break;
        }
        case PipelineContext::BinaryLeft: {
            arityStatement = std::make_shared<ConstantExpressionStatement>(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "BinaryLeft")));
            break;
        }
        case PipelineContext::BinaryRight: {
            arityStatement = std::make_shared<ConstantExpressionStatement>(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "BinaryRight")));
            break;
        }
    }

    auto ctorFunction = ConstructorDefinition::create("ExecutablePipelineStage" + context->pipelineName, true)
                            ->addInitializer("NodeEngine::Execution::ExecutablePipelineStage", arityStatement);

    auto executablePipeline = ClassDefinition::create("ExecutablePipelineStage" + context->pipelineName);
    executablePipeline->addBaseClass("NodeEngine::Execution::ExecutablePipelineStage");
    executablePipeline->addMethod(ClassDefinition::Public, functionBuilder);
    executablePipeline->addMethod(ClassDefinition::Public, setupFunction);
    executablePipeline->addConstructor(ctorFunction);

    auto executablePipelineDeclaration = executablePipeline->getDeclaration();
    auto pipelineNamespace = NamespaceDefinition::create("NES");
    pipelineNamespace->addDeclaration(executablePipelineDeclaration);

    auto createFunction = FunctionDefinition::create("create");

    auto returnStatement = ReturnStatement::create(SharedPointerGen::makeShared(executablePipelineDeclaration->getType()));
    createFunction->addStatement(returnStatement);

    createFunction->returns(SharedPointerGen::createSharedPtrType(
        GeneratableTypesFactory().createAnonymusDataType("NodeEngine::Execution::ExecutablePipelineStage")));
    pipelineNamespace->addDeclaration(createFunction->getDeclaration());
    CodeFile file = fileBuilder.addDeclaration(pipelineNamespace->getDeclaration()).build();

    return file.code;
}

NodeEngine::Execution::ExecutablePipelineStagePtr CCodeGenerator::compile(PipelineContextPtr code) {
    std::string src = generateCode(code);
    auto compiledCode = compiler->compile(src);
    PipelineStageArity arity;
    switch (code->arity) {
        case PipelineContext::Unary: arity = Unary; break;
        case PipelineContext::BinaryLeft: arity = BinaryLeft; break;
        case PipelineContext::BinaryRight: arity = BinaryRight; break;
    }
    return CompiledExecutablePipelineStage::create(compiledCode, arity, src);
}

BinaryOperatorStatement CCodeGenerator::allocateTupleBuffer(VariableDeclaration pipelineContext) {
    auto allocateTupleBuffer = FunctionCallStatement("allocateTupleBuffer");
    return VarRef(pipelineContext).accessRef(allocateTupleBuffer);
}

BinaryOperatorStatement CCodeGenerator::getBufferSize(VariableDeclaration tupleBufferVariable) {
    auto getBufferSizeFunctionCall = FunctionCallStatement("getBufferSize");
    return VarRef(tupleBufferVariable).accessRef(getBufferSizeFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setNumberOfTuples(VariableDeclaration tupleBufferVariable,
                                                          VariableDeclaration numberOfResultTuples) {
    auto setNumberOfTupleFunctionCall = FunctionCallStatement("setNumberOfTuples");
    setNumberOfTupleFunctionCall.addParameter(VarRef(numberOfResultTuples));
    /* set number of output tuples to result buffer */
    return VarRef(tupleBufferVariable).accessRef(setNumberOfTupleFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setWatermark(VariableDeclaration tupleBufferVariable,
                                                     VariableDeclaration inputBufferVariable) {
    auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
    setWatermarkFunctionCall.addParameter(getWatermark(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setOriginId(VariableDeclaration tupleBufferVariable,
                                                    VariableDeclaration inputBufferVariable) {
    auto setOriginIdFunctionCall = FunctionCallStatement("setOriginId");
    setOriginIdFunctionCall.addParameter(getOriginId(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setOriginIdFunctionCall);
}

CCodeGenerator::~CCodeGenerator(){};

BinaryOperatorStatement CCodeGenerator::emitTupleBuffer(VariableDeclaration pipelineContext,
                                                        VariableDeclaration tupleBufferVariable,
                                                        VariableDeclaration workerContextVariable) {
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(tupleBufferVariable));
    emitTupleBuffer.addParameter(VarRef(workerContextVariable));
    return VarRef(pipelineContext).accessRef(emitTupleBuffer);
}
BinaryOperatorStatement CCodeGenerator::getBuffer(VariableDeclaration tupleBufferVariable) {
    auto getBufferFunctionCall = FunctionCallStatement("getBuffer");
    return VarRef(tupleBufferVariable).accessRef(getBufferFunctionCall);
}
BinaryOperatorStatement CCodeGenerator::getWatermark(VariableDeclaration tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getWatermark");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::getOriginId(VariableDeclaration tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getOriginId");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

#define TO_CODE(type) tf->createDataType(type)->getCode()->code_

BinaryOperatorStatement
CCodeGenerator::getAggregationWindowHandler(VariableDeclaration pipelineContextVariable,
                                            DataTypePtr keyType,
                                            Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor) {
    auto tf = getTypeFactory();
    // determine the partialAggregate parameter based on the aggregation type
    // Avg aggregation uses AVGPartialType, other aggregates use their getPartialAggregateStamp
    std::string partialAggregateCode;
    if (windowAggregationDescriptor->getType() == Windowing::WindowAggregationDescriptor::Avg) {
        partialAggregateCode = "Windowing::AVGPartialType<" + TO_CODE(windowAggregationDescriptor->getInputStamp()) + ">";
    } else {
        auto partialAggregateType = tf->createDataType(windowAggregationDescriptor->getPartialAggregateStamp());
        partialAggregateCode = partialAggregateType->getCode()->code_;
    }
    auto call =
        FunctionCallStatement(std::string("getWindowHandler<NES::Windowing::AggregationWindowHandler, ") + TO_CODE(keyType) + ", "
                              + TO_CODE(windowAggregationDescriptor->getInputStamp()) + "," + partialAggregateCode + ","
                              + TO_CODE(windowAggregationDescriptor->getFinalAggregateStamp()) + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getJoinWindowHandler(VariableDeclaration pipelineContextVariable,
                                                             DataTypePtr keyType,
                                                             std::string leftType,
                                                             std::string rightType) {

    auto tf = getTypeFactory();
    auto call = FunctionCallStatement(std::string("getJoinHandler<NES::Join::JoinHandler, ") + TO_CODE(keyType) + "," + leftType
                                      + "," + rightType + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getStateVariable(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getTypedWindowState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getLeftJoinState(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getLeftJoinState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getRightJoinState(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getRightJoinState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getWindowManager(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getWindowManager");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

TypeCastExprStatement CCodeGenerator::getTypedBuffer(VariableDeclaration tupleBufferVariable,
                                                     StructDeclaration structDeclaration) {
    auto tf = getTypeFactory();
    return TypeCast(getBuffer(tupleBufferVariable), tf->createPointer(tf->createUserDefinedType(structDeclaration)));
}
VariableDeclaration CCodeGenerator::getWindowOperatorHandler(PipelineContextPtr context,
                                                             VariableDeclaration tupleBufferVariable,
                                                             uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto executionContextRef = VarRefStatement(tupleBufferVariable);
    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::WindowOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    return windowOperatorHandlerDeclaration;
}

VariableDeclaration CCodeGenerator::getJoinOperatorHandler(PipelineContextPtr context,
                                                           VariableDeclaration tupleBufferVariable,
                                                           uint64_t joinOperatorIndex) {
    auto tf = getTypeFactory();
    auto executionContextRef = VarRefStatement(tupleBufferVariable);
    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    return windowOperatorHandlerDeclaration;
}
}// namespace QueryCompilation
}// namespace NES
