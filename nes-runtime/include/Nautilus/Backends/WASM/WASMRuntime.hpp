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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMRUNTIME_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMRUNTIME_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/Backends/WASM/WASMExecutionContext.hpp>
#include <utility>
#include <wasmtime.hh>
#include <unordered_map>

namespace NES::Nautilus::Backends::WASM {

struct BufferInfo {
    std::shared_ptr<Runtime::TupleBuffer> tupleBuffer;
    uint64_t memoryIndex = 0;
    bool copied = false;
    BufferInfo(std::shared_ptr<Runtime::TupleBuffer> tb, uint64_t index, bool cp)
        : tupleBuffer(std::move(tb)), memoryIndex(index), copied(cp){};
    BufferInfo() = default;
};

class WASMRuntime {
  public:
    explicit WASMRuntime(std::shared_ptr<WASMExecutionContext>  ctx) : context(std::move(ctx)) {};
    void setup();
    int32_t run();
    void close();
    std::vector<BufferInfo> getTupleBuffers() { return tupleBuffers; }

  private:
    std::shared_ptr<WASMExecutionContext> context;
    std::shared_ptr<wasmtime::Engine> engine = nullptr;
    std::shared_ptr<wasmtime::Linker> linker = nullptr;
    std::shared_ptr<wasmtime::Store> store = nullptr;
    std::shared_ptr<wasmtime::WasiConfig> wasiConfig = std::make_shared<wasmtime::WasiConfig>();
    wasmtime::Config config;
    std::shared_ptr<wasmtime::Module> pyModule = nullptr;
    std::shared_ptr<wasmtime::Func> execute = nullptr;
    std::vector<BufferInfo> tupleBuffers;
    //std::unordered_map<BufferInfo> tupleB;

    const char* cpythonFilePath = "/home/victor/wanes-engine/python/python3.11.wasm";
    const std::string proxyFunctionModule = "ProxyFunction";
    const std::string functionName = "execute";
    std::string parseWATFile(const char* fileName);
    void prepareCPython();
    void allocateBufferProxy();
    void host_emitBufferProxy();
    void host_NES__Runtime__TupleBuffer__getBuffer();
    void host_NES__Runtime__TupleBuffer__getBufferSize();
    void host_NES__Runtime__TupleBuffer__getNumberOfTuples();
    void host_NES__Runtime__TupleBuffer__setNumberOfTuples();
};

}// namespace NES::Nautilus::Backends::WASM

#endif//NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMRUNTIME_HPP_