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

#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_

#include <cstdint>

namespace NES::NodeEngine {
enum ReconfigurationType : uint8_t {
    // use Initialize for reconfiguration tasks that initialize a reconfigurable instance
    Initialize,
    // use Destroy for reconfiguration tasks that cleans up a reconfigurable instance
    Destroy,
    // use EndOfStream for reconfiguration tasks that communicate the end of stream event for a given query
    SoftEndOfStream,
    // use EndOfStream for reconfiguration tasks that communicate the end of stream event for a given query
    HardEndOfStream,
    // use QueryReconfiguration for reconfiguration tasks that reconfigure query execution plans of query
    QueryReconfiguration,
    // use StopViaReconfiguration for stopping QEP without propagating End Of Stream Messages (i.e) locally stop a QEP
    StopViaReconfiguration,
    // use ReplaceDataEmitter for replacing data emitter successor pipelines for Datasources
    ReplaceDataEmitter
};
}

#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_
