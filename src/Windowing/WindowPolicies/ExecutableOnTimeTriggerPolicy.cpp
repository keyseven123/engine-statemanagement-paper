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

#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>
#include <memory>

namespace NES::Windowing {

ExecutableOnTimeTriggerPolicy::~ExecutableOnTimeTriggerPolicy() { NES_WARNING("~ExecutableOnTimeTriggerPolicy()"); }

bool ExecutableOnTimeTriggerPolicy::start(AbstractWindowHandlerPtr windowHandler) {
    std::unique_lock lock(runningTriggerMutex);
    if (this->running) {
        NES_WARNING("ExecutableOnTimeTriggerPolicy::start already started");
        return true;
    }

    this->running = true;
    NES_DEBUG("ExecutableOnTimeTriggerPolicy started thread " << this << " handler=" << windowHandler->toString()
                                                              << " with ms=" << triggerTimeInMs);
    std::string handlerName = windowHandler->toString();
    thread = std::make_shared<std::thread>([handlerName, windowHandler, this]() {
        setThreadName("whdlr-%d", handlerName.c_str());
        while (this->running) {
            NES_DEBUG("ExecutableOnTimeTriggerPolicy:: trigger policy now");
            std::this_thread::sleep_for(std::chrono::milliseconds(triggerTimeInMs));
            if (windowHandler != nullptr) {
                windowHandler->trigger();
            }
        }
    });
    return true;
}

bool ExecutableOnTimeTriggerPolicy::start(Join::AbstractJoinHandlerPtr joinHandler) {
    std::unique_lock lock(runningTriggerMutex);
    if (this->running) {
        NES_WARNING("ExecutableOnTimeTriggerPolicy::start already started");
        return true;
    }

    this->running = true;
    NES_DEBUG("ExecutableOnTimeTriggerPolicy started thread " << this << " handler=" << joinHandler->toString()
                                                              << " with ms=" << triggerTimeInMs);
    std::string handlerName = joinHandler->toString();
    thread = std::make_shared<std::thread>([handlerName, joinHandler, this]() {
        setThreadName("whdlr-%d", handlerName.c_str());
        while (this->running) {
            NES_DEBUG("ExecutableOnTimeTriggerPolicy:: trigger policy now");
            std::this_thread::sleep_for(std::chrono::milliseconds(triggerTimeInMs));
            if (joinHandler != nullptr) {
                joinHandler->trigger();
            }
        }
    });
    return true;
}

bool ExecutableOnTimeTriggerPolicy::stop() {
    std::unique_lock lock(runningTriggerMutex);
    NES_DEBUG("ExecutableOnTimeTriggerPolicy " << this << ": Stop called");
    if (!this->running) {
        NES_DEBUG("ExecutableOnTimeTriggerPolicy " << this << ": Stop called but was already not running");
        return true;
    }
    this->running = false;

    if (thread && thread->joinable()) {
        thread->join();
        NES_DEBUG("ExecutableOnTimeTriggerPolicy " << this << ": Thread joinded");
    }
    thread.reset();
    // TODO what happens to the content of the window that it is still in the state?
    return true;
}

ExecutableOnTimeTriggerPolicy::ExecutableOnTimeTriggerPolicy(uint64_t triggerTimeInMs)
    : triggerTimeInMs(triggerTimeInMs), running(false), runningTriggerMutex() {}

ExecutableOnTimeTriggerPtr ExecutableOnTimeTriggerPolicy::create(uint64_t triggerTimeInMs) {
    return std::make_shared<ExecutableOnTimeTriggerPolicy>(triggerTimeInMs);
}

}// namespace NES::Windowing
