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
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "../../../../nes-runtime/include/Util/Core.hpp"

#include <Util/Logger/Logger.hpp>
#include <absl/container/internal/layout.h>

namespace NES::Optimizer
{
template <typename T, typename = std::void_t<>>
struct is_std_hashable : std::false_type
{
};

template <typename T>
struct is_std_hashable<T, std::void_t<decltype(std::declval<std::hash<T>>()(std::declval<T>()))>> : std::true_type
{
};

template <typename T>
constexpr bool is_std_hashable_v = is_std_hashable<T>::value;

template <typename T>
concept Trait = requires(T trait, T other) {
    //placeholder condition
    { T::atNode() } -> std::same_as<bool>;
    { trait == other };
    { T::getName() } -> std::same_as<std::string>;
};

template <typename T>
concept RecursiveTrait = requires() {
    //Not going to be easy to make it work how I'd like it.
    //Ideally, we could just name a container type in the RecursiveTrait and use this as the container for the children.
    //But, the container type only gets fully specified when specifying a trait set with some parameters, because
    //the other traits need to be saved too.
    //There are two ways around this:
    //Always only have collections with pointers to TraitSets (I dislike this solution because now our plan parts are spread all across memory).
    //In my opinion, the better solution would be to allow for a finite set of options in this RecursiveTrait concept
    //and translate them into specific container types.
    //That way, all possible container types are known to the trait set concept.
    //
    //IN THEORY, it should be possible for a compiler to do the original idea, but it would require the compiler to
    //understand when to stop expanding.
    //Maybe this is possible by swapping in a "stop" type after one expansion with "using"
    //
    //In addition, we could also model other nested data structures as trait sets, like predicates.
    //These Traits could contain a definition of what traitset they contain, e.g. a predicate would only contain other predicates.
    //This would give us a unified representation of "relnodes and rexnodes" in calcite-lingo
    { T::recursive() };
};

//Traits with compile-time finite instances (enums or booleans like isGPU) could be added like
template <typename T>
concept FiniteTrait = requires() {
    { T::getInstances() } -> std::same_as<std::set<T>>;
} && Trait<T>;


class VirtualTrait
{
public:
    virtual std::string getName() = 0;
};

template <Trait T>
class VirtualTraitImpl : VirtualTrait
{
    T trait;

public:
    std::string getName() override { return trait.getName(); }
};

template <typename TS, typename T>
T get(TS) = delete;



template <typename TS, typename T>
concept hasGetter = requires(TS ts) {
    { get<TS, T>(ts) } -> std::same_as<T>;
} || requires(TS ts)
{
    { ts.template get<T>()} -> std::same_as<T>;
}
|| requires(TS ts)
{
    {static_cast<T>(ts)} -> std::same_as<T>;
};


template <typename TS, typename... T>
concept TraitSet = requires(TS ts) {
    // (requires() {
    //     { get<T>(ts) } -> std::same_as<T>;
    // } && ...);

    ((!RecursiveTrait<T> || requires() {
         { ts.getChildren() } -> std::same_as<std::vector<TS>>;
     }) && ...);
} && (hasGetter<TS, T> && ...);

class Children
{
public:
    explicit Children() { }
    bool operator==(const Children&) const { return true; }
    static constexpr bool atNode() { return true; }
    static constexpr void recursive() { return; }

    static std::string getName() { return "Children"; }
};

static_assert(RecursiveTrait<Children>);

template <Trait... T>
class TupleTraitSet
{
    std::tuple<T...> underlying;

public:
    explicit TupleTraitSet(T... args) : underlying(args...) { }

    bool operator==(const TupleTraitSet& other) const { return this == &other; }

    template <Trait O>
    requires(std::same_as<O, T> || ...)
    O get()
    {
        return std::get<O>(underlying);
    }
};


class VirtualTraitSet
{
public:
    virtual void* get(VirtualTrait*) = 0;
};

template <Trait... T>
class DynamicTraitSet : VirtualTraitSet
{
    std::unordered_map<VirtualTrait, void*> traits;

public:
    template <Trait... O>
    DynamicTraitSet(DynamicTraitSet<O...> other) : traits(other.traits)
    {
    }
};


template <Trait... T>
class RecursiveTupleTraitSet
{
    std::tuple<T...> underlying;
    std::vector<RecursiveTupleTraitSet<T...>> children;

public:
    explicit RecursiveTupleTraitSet(T... args) : underlying(args...) { }
    explicit RecursiveTupleTraitSet(std::vector<RecursiveTupleTraitSet<T...>>& children, T... args)
        : underlying(args...), children(children)
    {
    }

    bool operator==(const RecursiveTupleTraitSet& other) const { return this == &other; }

    template <Trait O>
    requires(std::same_as<O, T> || ...)
    O get()
    {
        return std::get<O>(underlying);
    }

    std::vector<RecursiveTupleTraitSet> getChildren() { return children; }
};


// template <Trait... T>
// class TraitSetView
// {
// public:
//     template <Trait... TE>
//     explicit TraitSetView()
// };


// template <typename TS, typename... T>
// concept TraitSet = requires(TS traitSet, T... traits, TS other)
// {
//     { traitSet == other} -> std::same_as<bool>;
// }
// && (Trait<T> && ...);

template <typename SV>
concept StatisticsValue = requires(SV statisticsValue) { std::equality_comparable<SV>; };

//Make pack over trait sets instead
template <typename C, typename SV, typename TS>
concept CostFunction = requires(C function, TS ts) {
    { function(ts) } -> std::same_as<SV>;
    // { function.derive(TraitSet<TOs...>()) } -> std::same_as<TraitSet<TIs...>>;
} && StatisticsValue<SV> && TraitSet<TS>;
// && (TraitSet<TIs> && ...)
// && (Trait<TOs> && ...);

// template <StatisticsValue SV, Trait... TIs, Trait... TOs, CostFunction<SV, TIs, TOs> C>
// SV C::operator()(TraitSet<TIs...> ts)
// {
//     operator()(derive(ts));
// }

template <typename C, typename SV, typename TS>
concept OptionalCostFunction = requires(C function, TS ts) {
    { function(ts) } -> std::same_as<std::optional<SV>>;
} && StatisticsValue<SV> && TraitSet<TS>;
// && (Trait<T> && ...);
}

template <NES::Optimizer::Trait... T>
struct std::hash<NES::Optimizer::TupleTraitSet<T...>>
{
    std::size_t operator()(const NES::Optimizer::TupleTraitSet<T...>) const { return 2; }
};

namespace NES::Optimizer
{
class QueryForSubtree
{
    const std::string str;

public:
    explicit QueryForSubtree(std::string str) : str(std::move(str)) { }
    bool operator==(const QueryForSubtree&) const { return true; }
    static bool atNode() { return false; }

    static std::string getName() { return "QueryForSubtree"; }

    const std::string& getQuery() const { return str; }
};

static_assert(Trait<QueryForSubtree>);

class Placement
{
    const int nodeID;

public:
    explicit Placement(int nodeID) : nodeID(nodeID) { }
    bool operator==(const Placement& other) const { return nodeID == other.nodeID; }
    static constexpr bool atNode() { return true; }
    static std::string getName() { return "Placement"; }
};

static_assert(Trait<Placement>);

struct TestOperator
{
    const Placement placement;
    // Placement get() { return Placement{1}; };
    explicit operator Placement() const {
        return placement;
    }
};

// template <>
// inline Placement get<TestOperator, Placement>(TestOperator op)
// {
//     return op.placement;
// }

static_assert(hasGetter<TestOperator, Placement>);


static_assert(TraitSet<TupleTraitSet<Placement>, Placement>);
static_assert(TraitSet<TestOperator, Placement>);

// static_assert(hasGetter<TupleTraitSet<Placement>, Placement>::value::value);

class Operator
{
public:
    bool operator==(const Operator&) const { return true; }
    static constexpr bool atNode() { return true; }
    static std::string getName() { return "Operator"; }
};

static_assert(Trait<Operator>);

// template <typename T, typename... Ts>
// struct PackContains
// {
//     static constexpr bool value{(std::is_same_v<T, Ts> || ...)};
// };
//
// //Needs to be more controllable
// template <typename TI, typename TO>
// TO derive(TI);
//
// template <Trait... TIs, Trait... TOs>
// TraitSet<TOs...> derive(TIs... tis)
// {
//     return TraitSet<TOs...>{(PackContains<TOs, TIs...>::value, ...)};
// }
//
//
// template <StatisticsValue SV, Trait... TIs, CostFunction<SV, TIs...> C, Trait... TOs>
// SV C::operator()(TraitSet<TIs...> tis)
// {
//     C(derive(tis));
// }


class StatisticsCatalog
{
    std::unordered_map<TupleTraitSet<QueryForSubtree>, int> rates;
    std::unordered_map<TupleTraitSet<Placement>, float> memoryUsage;

public:
    explicit StatisticsCatalog() { };


    class RateStore
    {
        friend StatisticsCatalog;
        std::unordered_map<TupleTraitSet<QueryForSubtree>, int>& rates;
        explicit RateStore(std::unordered_map<TupleTraitSet<QueryForSubtree>, int>& rates) : rates(rates) { }

    public:
        std::optional<int> operator()(TupleTraitSet<QueryForSubtree> ts) const
        {
            if (rates.contains(ts))
            {
                return rates.at(ts);
            }
            return std::nullopt;
        }
    };

private:
    RateStore rateStore = RateStore{rates};

public:
    RateStore& getRateStore() { return rateStore; }


    std::optional<float> getMemoryUsage(TupleTraitSet<Placement> ts)
    {
        if (memoryUsage.contains(ts))
        {
            return memoryUsage.at(ts);
        }
        return std::nullopt;
    }
};

static_assert(OptionalCostFunction<StatisticsCatalog::RateStore, int, TupleTraitSet<QueryForSubtree>>);


template <TraitSet<QueryForSubtree> TS, OptionalCostFunction<int, TS> StatisticsCatalogCost>
class RateEstimator
{
    StatisticsCatalogCost statisticsFunction;

public:
    explicit RateEstimator(StatisticsCatalogCost statisticsBaseFunction) : statisticsFunction(statisticsBaseFunction) { }

    int operator()(TS ts)
    {
        if (const std::optional<int> statistic = statisticsFunction(ts))
        {
            return *statistic;
        }
        return 20;
    }
};

template <TraitSet<QueryForSubtree> TS, CostFunction<int, TS> RateEstimator>
class PlacementCost
{
    RateEstimator rateEstimator;

public:
    explicit PlacementCost(RateEstimator rateEstimator) : rateEstimator(rateEstimator) { }

    template <TraitSet<QueryForSubtree, Placement, Children> TSI>
    int operator()(TSI ts)
    {
        for (TSI child : ts.getChildren())
        {
            NES_INFO("Child query: {}", child.template get<QueryForSubtree>().getQuery());
        }
        auto derived = TupleTraitSet<QueryForSubtree>{ts.template get<QueryForSubtree>()};
        int rate = rateEstimator(derived);
        return rate;
    }
};

class AbstractRewriteRule
{
    virtual VirtualTraitSet* apply(VirtualTraitSet*);
};

template <Trait... T>
class TypedAbstractRewriteRule : AbstractRewriteRule
{
    VirtualTraitSet* apply(VirtualTraitSet* inputTS) override { applyTyped(DynamicTraitSet<T...>{inputTS}); };

    virtual DynamicTraitSet<T...>* applyTyped(DynamicTraitSet<T...>*) = 0;
};

class LowerToPhysicalSelection : TypedAbstractRewriteRule<QueryForSubtree, Operator>
{
    DynamicTraitSet<QueryForSubtree, Operator>* applyTyped(DynamicTraitSet<QueryForSubtree, Operator>*) override { return nullptr; };
};


}
