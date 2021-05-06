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

#ifndef NES_INCLUDE_NODES_UTIL_CONSOLE_DUMP_HANDLER_HPP_
#define NES_INCLUDE_NODES_UTIL_CONSOLE_DUMP_HANDLER_HPP_

#include <Nodes/Util/DumpHandler.hpp>
#include <memory>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;
/**
 * @brief Converts query plans and pipeline plans to the .nesviz format and dumps them to a file.m
 */
class ConsoleDumpHandler : public DumpHandler {

  public:
    static std::shared_ptr<ConsoleDumpHandler> create(std::ostream& out);
    ConsoleDumpHandler(std::ostream& out);
    /**
    * Dump the specific node and its children.
    */
    void dump(const NodePtr node) override;

    /**
    * Dump the specific node and its children with details in multiple lines.
    */
    void multilineDump(const NodePtr node);

    /**
     * @brief Dump a query plan with a specific context and scope.
     * @param context the context
     * @param scope the scope
     * @param plan the query plan
     */
    void dump(std::string context, std::string scope, QueryPlanPtr plan) override;

    /**
     * @brief Dump a pipeline query plan with a specific context and scope.
     * @param context the context
     * @param scope the scope
     * @param plan the query plan
     */
    void dump(std::string context, std::string scope, QueryCompilation::PipelineQueryPlanPtr pipelineQueryPlan) override;

  private:
    std::ostream& out;
    void dumpHelper(const NodePtr op, uint64_t depth, uint64_t indent, std::ostream& out) const;
    void multilineDumpHelper(const NodePtr op, uint64_t depth, uint64_t indent, std::ostream& out) const;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_CONSOLE_DUMP_HANDLER_HPP_
