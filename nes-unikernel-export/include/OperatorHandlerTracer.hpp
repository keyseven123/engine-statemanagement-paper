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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_UNIKERNEL_OPERATORHANDLERTRACER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_UNIKERNEL_OPERATORHANDLERTRACER_HPP_

#include <API/Schema.hpp>
#include <Identifiers.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Util/Logger/Logger.hpp>
#include <any>
#include <cstdint>
#include <ranges>
#include <string>
#include <variant>
#include <vector>

#ifdef UNIKERNEL_EXPORT

#define VA_ARGS(...) , ##__VA_ARGS__
#define TRACE_OPERATOR_HANDLER(className, headerPath, ...)                                                                       \
    do {                                                                                                                         \
        NES_INFO("OperatorHandler Tracer: {} size: {}, align: {}", className, sizeof(*this), alignof(*this));                    \
        OperatorHandler::trace((className), (headerPath), sizeof(*this), alignof(*this) VA_ARGS(__VA_ARGS__));                   \
    } while (0)
#else
#define TRACE_OPERATOR_HANDLER(name, ...)
#endif

namespace NES::Runtime::Unikernel {
enum OperatorHandlerParameterType {
    INT32,
    UINT32,
    INT64,
    UINT64,
    VECTOR,
    FLOAT32,
    FLOAT64,
    ENUM_CONSTANT,
    SHARED_PTR,
    BATCH_JOIN_DEFINITION,
    PAGED_VECTOR
};

struct OperatorHandlerParameterDescriptor {
    OperatorHandlerParameterType type;
    std::any value;

    template<typename T>
    static OperatorHandlerParameterDescriptor of(const T& value);

    template<typename T>
    static OperatorHandlerParameterDescriptor of(const std::vector<T>& value);
};

template<typename T>
OperatorHandlerParameterDescriptor OperatorHandlerParameterDescriptor::of(const std::vector<T>& value) {
    std::vector<OperatorHandlerParameterDescriptor> parameters;
    for (auto& v : value) {
        parameters.push_back(OperatorHandlerParameterDescriptor::of(v));
    }

    return {OperatorHandlerParameterType::VECTOR, parameters};
}
using GlobalOperatorHandlerIndex = size_t;
struct OperatorHandlerDescriptor {
    OperatorHandlerDescriptor(std::string className,
                              std::string headerPath,
                              size_t classSize,
                              size_t alignment,
                              std::vector<OperatorHandlerParameterDescriptor> parameters);

    OperatorHandlerDescriptor() = default;

    std::string className;
    std::string headerPath;
    size_t size;
    size_t alignment;
    GlobalOperatorHandlerIndex handlerId;
    std::vector<OperatorHandlerParameterDescriptor> parameters;
};

using EitherSharedOrLocal = std::variant<size_t, OperatorHandlerDescriptor>;
class OperatorHandlerTracer {
  public:
    static OperatorHandlerTracer* get();
    static GlobalOperatorHandlerIndex currentHandlerId;
    template<typename... Args>
    static void traceOperatorHandlerInstantiation(const std::string& className,
                                                  const std::string& headerPath,
                                                  size_t classSize,
                                                  size_t alignment,
                                                  Args... args) {
        NES_DEBUG("Trace Operator: {}", className);
        get()->descriptors.emplace_back(
            className,
            headerPath,
            classSize,
            alignment,
            std::vector<OperatorHandlerParameterDescriptor>{OperatorHandlerParameterDescriptor::of(args)...});
    }

    static void reset();

    static std::vector<OperatorHandlerDescriptor> getDescriptors();

  private:
    std::vector<OperatorHandlerDescriptor> descriptors;
};

}// namespace NES::Runtime::Unikernel

#endif//NES_RUNTIME_INCLUDE_RUNTIME_UNIKERNEL_OPERATORHANDLERTRACER_HPP_