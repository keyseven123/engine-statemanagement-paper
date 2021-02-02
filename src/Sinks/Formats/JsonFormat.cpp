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
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

JsonFormat::JsonFormat(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {}

std::vector<NodeEngine::TupleBuffer> JsonFormat::getData(NodeEngine::TupleBuffer&) { NES_NOT_IMPLEMENTED(); }

std::optional<NodeEngine::TupleBuffer> JsonFormat::getSchema() { NES_NOT_IMPLEMENTED(); }

std::string JsonFormat::toString() { return "JSON_FORMAT"; }
SinkFormatTypes JsonFormat::getSinkFormat() { return JSON_FORMAT; }

}// namespace NES