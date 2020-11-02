#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <utility>

namespace NES {

// TODO this might change across OS
static constexpr auto mangledEntryPoint = "_Z14compiled_queryRN3NES11TupleBufferEPvPNS_9Windowing13WindowManagerERNS_24PipelineExecutionContextERNS_13WorkerContextE";

CompiledExecutablePipeline::CompiledExecutablePipeline(CompiledCodePtr compiled_code)
    : ExecutablePipeline(false), compiledCode(std::move(compiled_code)), pipelineFunc(compiledCode->getFunctionPointer<PipelineFunctionPtr>(mangledEntryPoint)) {
    // nop
}

CompiledExecutablePipeline::CompiledExecutablePipeline(PipelineFunctionPtr func) : ExecutablePipeline(true), compiledCode(nullptr), pipelineFunc(func) {
    // nop
}

uint32_t CompiledExecutablePipeline::execute(TupleBuffer& inputBuffer,
                                             void* state,
                                             Windowing::WindowManagerPtr windowManager,
                                             QueryExecutionContextPtr context,
                                             WorkerContextRef wctx) {
    return (*pipelineFunc)(inputBuffer, state, windowManager.get(), *context.get(), wctx);
}

ExecutablePipelinePtr CompiledExecutablePipeline::create(const CompiledCodePtr compiledCode) {
    return std::make_shared<CompiledExecutablePipeline>(compiledCode);
}

}// namespace NES