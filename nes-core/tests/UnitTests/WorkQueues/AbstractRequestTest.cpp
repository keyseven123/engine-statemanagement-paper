#include <NesBaseTest.hpp>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <gtest/gtest.h>

namespace NES {

class DummyResponse : public AbstractRequestResponse {
  public:
    explicit DummyResponse(uint32_t number) : number(number){};
    uint32_t number;
};

class DummyRequest : public AbstractRequest<DummyResponse> {
  public:
    DummyRequest(RequestId requestId,
                 const std::vector<ResourceType>& requiredResources,
                 uint8_t maxRetries,
                 std::promise<DummyResponse> responsePromise, uint32_t responseValue)
        : AbstractRequest(requestId, requiredResources, maxRetries, std::move(responsePromise)), responseValue(responseValue){};
    DummyRequest(DummyRequest&& other) noexcept : AbstractRequest(std::move(other)), responseValue(other.responseValue) {};

    void executeRequestLogic(NES::StorageHandler&) override {
        responsePromise.set_value(DummyResponse(responseValue));
    }

    void rollBack(const RequestExecutionException&, StorageHandler&) override {}

  protected:
    void preRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
    void postRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
    void postExecution(StorageHandler&) override {}
  private:
    uint32_t responseValue;
};

class DummyStorageHandler : public StorageHandler {
  public:
    explicit DummyStorageHandler() = default;
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle(RequestId) override {return nullptr; };

    TopologyHandle getTopologyHandle(RequestId) override {return nullptr; };

    QueryCatalogServiceHandle getQueryCatalogServiceHandle(RequestId) override {return nullptr; };

    GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId) override {return nullptr; };

    SourceCatalogHandle getSourceCatalogHandle(RequestId) override {return nullptr; };

    UDFCatalogHandle getUDFCatalogHandle(RequestId) override {return nullptr; };
};

class AbstractRequestTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("AbstractRequestTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(AbstractRequestTest, testPromise) {
    constexpr uint32_t responseValue = 20;
    std::promise<DummyResponse> promise;
    auto future = promise.get_future();
    RequestId requestId = 1;
    std::vector<ResourceType> requiredResources;
    uint8_t maxRetries = 1;
    DummyRequest request(requestId, requiredResources, maxRetries, std::move(promise), responseValue);
    auto thread = std::make_shared<std::thread>([&request]() {
        DummyStorageHandler storageHandler;
        request.executeRequestLogic(storageHandler);
    });
    thread->join();
    ASSERT_EQ(future.get().number, responseValue);
}

}// namespace NES
