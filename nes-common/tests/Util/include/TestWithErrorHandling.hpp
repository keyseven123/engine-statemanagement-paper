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

#ifndef NES_NES_COMMON_TESTS_UTIL_TESTWITHERRORHANDLING_HPP_
#define NES_NES_COMMON_TESTS_UTIL_TESTWITHERRORHANDLING_HPP_
#include <Exceptions/ErrorListener.hpp>
#include <atomic>
#include <future>
#include <gtest/gtest.h>
#include <thread>
namespace NES::Testing {

namespace detail {
class TestWaitingHelper {
  public:
    TestWaitingHelper();
    void startWaitingThread(std::string testName);
    void completeTest();
    void failTest();

  private:
    std::unique_ptr<std::thread> waitThread;
    std::shared_ptr<std::promise<bool>> testCompletion;
    std::atomic<bool> testCompletionSet{false};
    static constexpr uint64_t WAIT_TIME_SETUP = 5;
};
}// namespace detail

class TestWithErrorHandling : public testing::Test, public Exceptions::ErrorListener, public detail::TestWaitingHelper {
    struct Deleter {
        void operator()(void*) {}
    };

  public:
    void SetUp() override;
    void TearDown() override;
    virtual void onFatalError(int signalNumber, std::string callstack) override;
    virtual void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override;

  private:
    std::shared_ptr<Exceptions::ErrorListener> self{nullptr};
};

}// namespace NES::Testing

#endif//NES_NES_COMMON_TESTS_UTIL_TESTWITHERRORHANDLING_HPP_
