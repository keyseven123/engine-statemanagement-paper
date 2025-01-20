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

#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <private/Optimizer/Cost/CostFunctions.hpp>

#include <BaseUnitTest.hpp>

namespace NES::Optimizer {

class CostTest : public Testing::BaseUnitTest
{

public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("CostTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup CostTest test class.");
    }
};

TEST_F(CostTest, testHarcodedFunctions)
{
    auto statCatalog = StatisticsCatalog{};
    auto cardEstimator = RateEstimator<TupleTraitSet<QueryForSubtree>, StatisticsCatalog::RateStore>{statCatalog.getRateStore()};
    PlacementCost<TupleTraitSet<QueryForSubtree>, decltype(cardEstimator)> placementCost
        = PlacementCost<TupleTraitSet<QueryForSubtree>, decltype(cardEstimator)>{cardEstimator};


    RecursiveTupleTraitSet<QueryForSubtree, Placement, Children> child = RecursiveTupleTraitSet{QueryForSubtree{"child"}, Placement{4}, Children{}};
    std::vector<decltype(child)> children {child};
    RecursiveTupleTraitSet<QueryForSubtree, Placement, Children> ts = RecursiveTupleTraitSet{children, QueryForSubtree{"test"}, Placement{5}, Children{}};


    auto cost = placementCost(ts);

    NES_INFO("Placement Cost {}", cost);
}
}
