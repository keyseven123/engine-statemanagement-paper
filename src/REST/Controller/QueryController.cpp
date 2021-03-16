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

#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/Controller/QueryController.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;
using namespace std;

namespace NES {

QueryController::QueryController(QueryServicePtr queryService, QueryCatalogPtr queryCatalog, TopologyPtr topology,
                                 GlobalExecutionPlanPtr globalExecutionPlan)
    : topology(topology), queryService(queryService), queryCatalog(queryCatalog), globalExecutionPlan(globalExecutionPlan) {}

void QueryController::handleGet(vector<utility::string_t> path, http_request request) {

    auto parameters = getParameters(request);

    if (path[1] == "execution-plan") {
        NES_INFO("QueryController:: GET execution-plan");
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            // get the queryId from user input
            QueryId queryId = std::stoi(param->second);
            NES_DEBUG("QueryController:: execution-plan requested queryId: " << queryId);
            // get the execution-plan for given query id
            auto executionPlanJson = PlanJsonGenerator::getExecutionPlanAsJson(globalExecutionPlan, queryId);
            NES_DEBUG("QueryController:: execution-plan: " << executionPlanJson.serialize());
            //Prepare the response
            successMessageImpl(request, executionPlanJson);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }

    } else if (path[1] == "query-plan") {
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            // get the queryId from user input
            QueryId queryId = std::stoi(param->second);

            //Call the service
            NES_DEBUG("UtilityFunctions: Get the registered query");
            if (!queryCatalog->queryExists(queryId)) {
                throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId)
                                             + " in query catalog.");
            }
            QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

            NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
            auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getQueryPlan());

            //Prepare the response
            successMessageImpl(request, basePlan);
            return;
        } catch (const std::exception& exc) {
            NES_ERROR("QueryController: handleGet -query-plan: Exception occurred while building the query plan for user "
                      "request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }
    } else {
        resourceNotFoundImpl(request);
    }
}

void QueryController::handlePost(vector<utility::string_t> path, http_request message) {

    if (path[1] == "execute-query" || path[1] == "execute-pattern") {

        if (path[1] == "execute-query") {
            NES_DEBUG(" QueryController: Trying to execute query");
        } else if (path[1] == "execute-pattern") {
            NES_DEBUG(" QueryController: Trying to execute pattern as stream query");
        }

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    string userRequest(body.begin(), body.end());
                    NES_DEBUG("QueryController: handlePost -execute-query: Request body: " << userRequest
                                                                                           << "try to parse query");
                    json::value req = json::value::parse(userRequest);
                    NES_DEBUG("QueryController: handlePost -execute-query: get user query");
                    string userQuery = "";
                    if (req.has_field("userQuery")) {
                        userQuery = req.at("userQuery").as_string();
                    } else if (req.has_field("pattern")) {
                        userQuery = req.at("pattern").as_string();
                    } else {
                        NES_ERROR("QueryController: handlePost -execute-query: Wrong key word for user query or pattern. Use "
                                  "either 'userQuery' or 'pattern'.");
                    }

                    string optimizationStrategyName = req.at("strategyName").as_string();
                    NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= " << userQuery << ", strategyName= "
                                                                                                << optimizationStrategyName);
                    QueryId queryId = queryService->validateAndQueueAddRequest(userQuery, optimizationStrategyName);

                    //Prepare the response
                    json::value restResponse{};
                    restResponse["queryId"] = json::value::number(queryId);
                    successMessageImpl(message, restResponse);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("QueryController: handlePost -execute-query: Exception occurred while building the query plan for "
                              "user request:"
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
    }
}

void QueryController::handleDelete(std::vector<utility::string_t> path, http_request request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "stop-query") {
        NES_DEBUG("QueryController: Request received for stoping a query");
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            //Prepare Input query from user string
            QueryId queryId = std::stoi(param->second);

            bool success = queryService->validateAndQueueStopRequest(queryId);
            //Prepare the response
            json::value result{};
            result["success"] = json::value::boolean(success);
            successMessageImpl(request, result);
            return;
        } catch (QueryNotFoundException& exc) {
            NES_ERROR("QueryCatalogController: handleDelete -query: Exception occurred while building the query plan for "
                      "user request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (InvalidQueryStatusException& exc) {
            NES_ERROR("QueryCatalogController: handleDelete -query: Exception occurred while building the query plan for "
                      "user request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }
    } else {
        resourceNotFoundImpl(request);
    }
}

}// namespace NES
