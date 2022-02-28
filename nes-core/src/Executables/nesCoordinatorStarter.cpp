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

#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Util/Logger.hpp>
#include <Version/version.hpp>
#include <filesystem>
#include <iostream>
#include <map>
#include <vector>

using namespace NES;
using namespace std;

const string logo = "\n"
                    "███╗░░██╗███████╗██████╗░██╗░░░██╗██╗░░░░░░█████╗░░██████╗████████╗██████╗░███████╗░█████╗░███╗░░░███╗\n"
                    "████╗░██║██╔════╝██╔══██╗██║░░░██║██║░░░░░██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗░████║\n"
                    "██╔██╗██║█████╗░░██████╦╝██║░░░██║██║░░░░░███████║╚█████╗░░░░██║░░░██████╔╝█████╗░░███████║██╔████╔██║\n"
                    "██║╚████║██╔══╝░░██╔══██╗██║░░░██║██║░░░░░██╔══██║░╚═══██╗░░░██║░░░██╔══██╗██╔══╝░░██╔══██║██║╚██╔╝██║\n"
                    "██║░╚███║███████╗██████╦╝╚██████╔╝███████╗██║░░██║██████╔╝░░░██║░░░██║░░██║███████╗██║░░██║██║░╚═╝░██║\n"
                    "╚═╝░░╚══╝╚══════╝╚═════╝░░╚═════╝░╚══════╝╚═╝░░╚═╝╚═════╝░░░░╚═╝░░░╚═╝░░╚═╝╚══════╝╚═╝░░╚═╝╚═╝░░░░░╚═╝";

const string coordinator = "\n"
                           "▒█▀▀█ █▀▀█ █▀▀█ █▀▀█ █▀▀▄ ░▀░ █▀▀▄ █▀▀█ ▀▀█▀▀ █▀▀█ █▀▀█ \n"
                           "▒█░░░ █░░█ █░░█ █▄▄▀ █░░█ ▀█▀ █░░█ █▄▄█ ░░█░░ █░░█ █▄▄▀ \n"
                           "▒█▄▄█ ▀▀▀▀ ▀▀▀▀ ▀░▀▀ ▀▀▀░ ▀▀▀ ▀░░▀ ▀░░▀ ░░▀░░ ▀▀▀▀ ▀░▀▀";

int main(int argc, const char* argv[]) {
    try {
        std::cout << logo << std::endl;
        std::cout << coordinator << " v" << NES_VERSION << std::endl;

        map<string, string> commandLineParams;
        for (int i = 1; i < argc; ++i) {
            commandLineParams.insert(std::make_pair<string, string>(
                string(argv[i]).substr(0, string(argv[i]).find('=')),
                string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
        }

        std::string logPath;
        auto logPathEntry = commandLineParams.find("--logPath");
        if (logPathEntry != commandLineParams.end()) {
            logPath = logPathEntry->second + std::filesystem::path::preferred_separator + "nesCoordinator.log";
        } else {
            logPath = std::filesystem::current_path().string() + std::filesystem::path::preferred_separator + "nesCoordinator.log";
        }

        NES::setupLogging(logPath, NES::getDebugLevelFromString("LOG_DEBUG"));

        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        auto configPath = commandLineParams.find("--configPath");
        if (configPath != commandLineParams.end()) {
            coordinatorConfig->overwriteConfigWithYAMLFileInput(configPath->second);
        }

        if (argc > 1) {
            coordinatorConfig->overwriteConfigWithCommandLineInput(commandLineParams);
        }
        NES::setLogLevel(coordinatorConfig->logLevel.getValue());

        NES_INFO("start coordinator with " << coordinatorConfig->toString());

        NES_INFO("creating coordinator");
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);

        NES_INFO("Starting Coordinator");
        crd->startCoordinator(/**blocking**/ true);//This is a blocking call
        NES_INFO("Stopping Coordinator");
        crd->stopCoordinator(true);
    } catch (std::exception& exp) {
        NES_ERROR("Problem with coordinator: " << exp.what());
        return 1;
    } catch (...) {
        NES_ERROR("Unknown exception was thrown");
        try {
            std::rethrow_exception(std::current_exception());
        } catch (std::exception& ex) {
            NES_ERROR(ex.what());
        }
        return 1;
    }
}