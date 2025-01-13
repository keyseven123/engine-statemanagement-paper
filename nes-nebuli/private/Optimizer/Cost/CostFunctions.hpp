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

#pragma once
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>



template <typename T>
concept Trait = requires (T trait, T other)
{
    //placeholder condition
    { trait.atNode() } -> std::same_as<bool>;
    { trait == other };
};

template<Trait... T>
class TraitSet
{
    bool operator==(const TraitSet& other) const
    {
        return this == &other;
    }
};

template<Trait... T>
struct std::hash<TraitSet<T...>>
{
    std::size_t operator()(const TraitSet<T...>) const
    {
        return 2;
    }
};
// template <typename TS, typename... T>
// concept TraitSet = requires(TS traitSet, T... traits, TS other)
// {
//     { traitSet == other} -> std::same_as<bool>;
// }
// && (Trait<T> && ...);

template <typename SV>
concept StatisticsValue = requires(SV statisticsValue)
{
    std::equality_comparable<SV>;
};

template <typename C, typename SV, typename... T>
concept CostFunction = requires (C function, SV)
{
  { function(TraitSet<T...>()) } -> std::same_as<SV>;
}
&& StatisticsValue<SV>
//&& TraitSet<TS>
&& (Trait<T> && ...);

template <typename C, typename SV, typename... T>
concept OptionalCostFunction = requires (C function)
{
  { function(TraitSet<T...>()) } -> std::same_as<std::optional<SV>>;
}
&& StatisticsValue<SV>
//&& TraitSet<TS>
&& (Trait<T> && ...);


class QueryForSubtree
{
public:
    bool operator==(const QueryForSubtree&) const { return true; }
    bool atNode() { return false; }
};

static_assert(Trait<QueryForSubtree>);

class Placement
{
public:
    std::string nodeName;
    bool operator==(const Placement& other) const { return nodeName == other.nodeName; }
    static constexpr bool atNode() { return true; }
};

class StatisticsCatalog
{

    std::unordered_map<TraitSet<QueryForSubtree>, int> cardinalities;
    std::unordered_map<TraitSet<Placement>, float> memoryUsage;
public:




    std::optional<int> getCardinality(TraitSet<QueryForSubtree> ts)
    {
        if (cardinalities.contains(ts))
        {
            return cardinalities.at(ts);
        }
        return std::nullopt;
    }


    std::optional<float> getMemoryUsage(TraitSet<Placement> ts)
    {
        if (memoryUsage.contains(ts))
        {
            return memoryUsage.at(ts);
        }
        return std::nullopt;
    }
};

template <OptionalCostFunction StatisticsCatalogCost>
class CardinalityEstimator
{
    StatisticsCatalogCost statisticsFunction;
    std::function<std::optional<int>(TraitSet<QueryForSubtree>)> baseFunction;

    CardinalityEstimator(StatisticsCatalog& statisticsCatalog): statisticsFunction(statisticsCatalog.getCardinality){}

private:
    int estimate(TraitSet<QueryForSubtree> ts)
    {
        if (const std::optional<int> statistic = statisticsFunction(ts))
        {
            return *statistic;
        }
        return 20;
    }
};
