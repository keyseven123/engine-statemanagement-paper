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

#include <cerrno>
#include <cstring>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <ostream>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <YAML/YAMLBinder.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <NebuLI.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include "SQLQueryParser/AntlrSQLQueryParser.hpp"
#include "SQLQueryParser/StatementBinder.hpp"


int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);
        using argparse::ArgumentParser;
        ArgumentParser program("nesql");
        program.add_argument("-l", "--log-level").default_value("ERROR").help("Set the log level");
        program.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");
        program.add_argument("-i", "--input")
            .default_value("-")
            .help("From where to read SQL statements. Use - for stdin which is the default");
        program.add_argument("-o", "--output").default_value("-").help("To where to write the output. Use - for stdout");

        program.parse_args(argc, argv);

        auto logLevel = magic_enum::enum_cast<NES::LogLevel>(program.get<std::string>("-l"));
        if (not logLevel.has_value())
        {
            NES_ERROR("Invalid log level: {}", program.get<std::string>("-l"));
            return 1;
        }
        NES::Logger::getInstance()->changeLogLevel(logLevel.value());

        if (not program.is_used("-s"))
        {
            NES_ERROR("No server address specified");
            return 1;
        }

        auto sourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();
        auto optimizer = NES::CLI::Optimizer{sourceCatalog};
        auto binder = NES::StatementBinder{sourceCatalog, std::bind(NES::AntlrSQLQueryParser::bindLogicalQueryPlan, std::placeholders::_1)};

        auto client = std::make_shared<const GRPCClient>(CreateChannel(program.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
        NES::CLI::Nebuli nebuli{client};





        bool handled = false;
        for (const auto& [name, fn] : std::initializer_list<std::pair<std::string_view, void (NES::CLI::Nebuli::*)(NES::QueryId)>>{
                 {"start", &NES::CLI::Nebuli::startQuery},
                 {"unregister", &NES::CLI::Nebuli::unregisterQuery},
                 {"stop", &NES::CLI::Nebuli::stopQuery}})
        {
            if (program.is_subcommand_used(name))
            {
                auto& parser = program.at<ArgumentParser>(name);
                auto serverUri = parser.get<std::string>("-s");
                auto client = std::make_shared<const GRPCClient>(CreateChannel(serverUri, grpc::InsecureChannelCredentials()));
                NES::CLI::Nebuli nebuli{client};
                auto queryId = NES::QueryId{parser.get<size_t>("queryId")};
                (nebuli.*fn)(queryId);
                handled = true;
                break;
            }
        }

        if (handled)
        {
            return 0;
        }

        auto sourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();
        auto yamlBinder = NES::CLI::YAMLBinder{sourceCatalog};
        auto optimizer = NES::CLI::Optimizer{sourceCatalog};

        const std::string command = program.is_subcommand_used("register") ? "register" : "dump";
        auto input = program.at<argparse::ArgumentParser>(command).get("-i");
        NES::CLI::BoundQueryConfig boundConfig;
        if (input == "-")
        {
            boundConfig = yamlBinder.parseAndBind(std::cin);
        }
        else
        {
            std::ifstream file{input};
            if (!file)
            {
                throw NES::QueryDescriptionNotReadable(std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
            }
            boundConfig = yamlBinder.parseAndBind(file);
        }

        const std::shared_ptr<NES::DecomposedQueryPlan> decomposedQueryPlan = optimizer.optimize(boundConfig.plan);

        std::string output;
        NES::SerializableDecomposedQueryPlan serialized;
        NES::DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(*decomposedQueryPlan, &serialized);
        google::protobuf::TextFormat::PrintToString(serialized, &output);
        NES_INFO("GRPC QueryPlan: {}", output);
        if (program.is_subcommand_used("dump"))
        {
            auto& dumpArgs = program.at<ArgumentParser>("dump");
            auto outputPath = dumpArgs.get<std::string>("-o");
            std::ostream* output = nullptr;
            std::ofstream file;
            if (outputPath == "-")
            {
                output = &std::cout;
            }
            else
            {
                file = std::ofstream(outputPath);
                if (!file)
                {
                    NES_FATAL_ERROR("Could not open output file: {}", outputPath);
                    return 1;
                }
                output = &file;
            }
            if (!serialized.SerializeToOstream(output))
            {
                NES_FATAL_ERROR("Failed to write message to file.");
                return -1;
            }

            if (outputPath == "-")
            {
                NES_INFO("Wrote protobuf to {}", outputPath);
            }
        }
        else if (program.is_subcommand_used("register"))
        {
            auto& registerArgs = program.at<ArgumentParser>("register");
            auto client = std::make_shared<const GRPCClient>(
                grpc::CreateChannel(registerArgs.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
            NES::CLI::Nebuli nebuli{client};
            auto queryId = nebuli.registerQuery(decomposedQueryPlan);
            if (registerArgs.is_used("-x"))
            {
                nebuli.startQuery(queryId);
            }
            std::cout << queryId.getRawValue();
        }

        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentExceptionCode();
    }
}
