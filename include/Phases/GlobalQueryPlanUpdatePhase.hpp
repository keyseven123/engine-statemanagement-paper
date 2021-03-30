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

#ifndef NES_GLOBALQUERYPLANUPDATEPHASE_HPP
#define NES_GLOBALQUERYPLANUPDATEPHASE_HPP

#include <memory>
#include <vector>

namespace z3 {
class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {

namespace Optimizer {
class Z3SignatureInferencePhase;
typedef std::shared_ptr<Z3SignatureInferencePhase> SignatureInferencePhasePtr;

class QueryMergerPhase;
typedef std::shared_ptr<QueryMergerPhase> QueryMergerPhasePtr;
}// namespace Optimizer

class QueryCatalogEntry;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class GlobalQueryPlanUpdatePhase;
typedef std::shared_ptr<GlobalQueryPlanUpdatePhase> GlobalQueryPlanUpdatePhasePtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**
 * @brief This class is responsible for accepting a batch of query requests and then updating the Global Query Plan accordingly.
 */
class GlobalQueryPlanUpdatePhase {
  public:
    /**
     * @brief Create an instance of the GlobalQueryPlanUpdatePhase
     * @param queryCatalog: the catalog of queries
     * @param streamCatalog: the catalog of streams
     * @param globalQueryPlan: the input global query plan
     * @param enableQueryMerging: enable or disable query merging
     * @return Shared pointer for the GlobalQueryPlanUpdatePhase
     */
    static GlobalQueryPlanUpdatePhasePtr create(QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
                                                GlobalQueryPlanPtr globalQueryPlan, z3::ContextPtr z3Context,
                                                bool enableQueryMerging, std::string queryMergerRule);

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<QueryCatalogEntry>& queryRequests);

  private:
    explicit GlobalQueryPlanUpdatePhase(QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
                                        GlobalQueryPlanPtr globalQueryPlan, z3::ContextPtr z3Context, bool enableQueryMerging,
                                        std::string queryMergerRule);

    bool enableQueryMerging;
    QueryCatalogPtr queryCatalog;
    StreamCatalogPtr streamCatalog;
    GlobalQueryPlanPtr globalQueryPlan;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    Optimizer::QueryMergerPhasePtr queryMergerPhase;
    Optimizer::SignatureInferencePhasePtr signatureInferencePhase;
    z3::ContextPtr z3Context;
    std::string queryMergerRule;
};
}// namespace NES

#endif//NES_GLOBALQUERYPLANUPDATEPHASE_HPP
