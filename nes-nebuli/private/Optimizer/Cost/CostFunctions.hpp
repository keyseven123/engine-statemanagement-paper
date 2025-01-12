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
#include <set>
#include <optional>


template <typename T>
concept Trait = requires (T trait)
{
    { trait.atNode() } -> std::same_as<bool>;
};

template <typename TS, typename... T>
concept TraitSet = requires(TS traitset, T... traits)
{
    { traitset.equals(traitset)} -> std::same_as<bool>;
}
&& (Trait<T> && ...)
;

class StatisticsSubject
{
};

template <typename C, typename TS, typename... T>
concept CostFunction = requires (T function, TS)
{
  { function.apply(std::set<Trait>()) } -> std::same_as<StatisticsSubject>;
};

template <typename T>
concept OptionalCostFunction = requires (T function)
{
  { function.apply(std::set<Trait>()) } -> std::same_as<std::optional<StatisticsSubject>>;
};
