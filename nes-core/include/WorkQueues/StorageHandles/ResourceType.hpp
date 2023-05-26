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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_STORAGEHANDLERESOURCETYPE_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_STORAGEHANDLERESOURCETYPE_HPP_
#include <cstdint>

namespace NES {

/**
 * @brief This class is used by coordinator side requests to indicate which data structures they need to access to
 * perform an operation so they can acquire locks before starting to execute the request logic.
 */
enum class ResourceType : uint8_t {
    Topology,
    QueryCatalogService,
    SourceCatalog,
    GlobalExecutionPlan,
    GlobalQueryPlan,
    UdfCatalog,
};
}// namespace NES

#endif//NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_STORAGEHANDLERESOURCETYPE_HPP_
