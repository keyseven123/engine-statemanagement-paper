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

#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacement.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::ASP {

AbstractSynopsesPtr AbstractSynopsis::create(Parsing::SynopsisConfiguration& arguments,
                                             Parsing::SynopsisAggregationConfig& aggregationConfig) {

    // TODO For now this is okay, but later on we want to have a separate factory class, see issue #3734
    if (arguments.type.getValue() == Parsing::Synopsis_Type::SRSWoR) {
        return std::make_shared<RandomSampleWithoutReplacement>(aggregationConfig, arguments.width.getValue());
    } else {
        NES_NOT_IMPLEMENTED();
    }
}


AbstractSynopsis::AbstractSynopsis(Parsing::SynopsisAggregationConfig& aggregationConfig)
    : aggregationFunction(aggregationConfig.createAggregationFunction()),
      aggregationValue(aggregationConfig.createAggregationValue()), aggregationType(aggregationConfig.type),
      fieldNameAggregation(aggregationConfig.fieldNameAggregation), fieldNameApproximate(aggregationConfig.fieldNameApproximate),
      inputSchema(aggregationConfig.inputSchema), outputSchema(aggregationConfig.outputSchema) {}

} // namespace NES::ASP