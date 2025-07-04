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
namespace NES
{

/// Base class for all local operator states. This state is valid for one pipeline invocation, i.e., over one tuple buffer
/// One common use-case is to store / cache non-changing nautilus::val<> such that they must not be retrieved / computed for every tuple
/// but rather once in the open()
class OperatorState
{
public:
    virtual ~OperatorState() = default;
};
}
