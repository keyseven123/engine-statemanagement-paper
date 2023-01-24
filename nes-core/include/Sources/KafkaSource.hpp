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

#ifndef NES_INCLUDE_SOURCES_KAFKASOURCE_HPP_
#define NES_INCLUDE_SOURCES_KAFKASOURCE_HPP_
#include <cstdint>
#include <memory>
#include <string>
#include <Runtime/BufferManager.hpp>
#include <Sources/DataSource.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

class KafkaSource : public DataSource {

  public:
    KafkaSource(SchemaPtr schema,
                Runtime::BufferManagerPtr bufferManager,
                Runtime::QueryManagerPtr queryManager,
                std::string brokers,
                std::string topic,
                std::string groupId,
                bool autoCommit,
                uint64_t kafkaConsumerTimeout,
                OperatorId operatorId,
                size_t numSourceLocalBuffers);

    /**
     * @brief Get source type
     */
    SourceType getType() const override;
    ~KafkaSource() override;
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the kafka source
     * @return returns string describing the kafka source
     */
    std::string toString() const override;

    /**
     * @brief Get kafka brokers
     */
    std::string getBrokers() const;

    /**
     * @brief Get kafka topic
     */
    std::string getTopic() const;

    /**
     * @brief Get kafka group id
     */
    std::string getGroupId() const;

    /**
     * @brief If kafka offset is to be committed automatically
     */
    bool isAutoCommit() const;

    /**
     * @brief Get kafka configuration
     */
    const cppkafka::Configuration& getConfig() const;

    /**
     * @brief Get kafka connection timeout
     */
    const std::chrono::milliseconds& getKafkaConsumerTimeout() const;
    const std::unique_ptr<cppkafka::Consumer>& getConsumer() const;

  private:
    void _connect();

    std::string brokers;
    std::string topic;
    std::string groupId;
    bool autoCommit;
    cppkafka::Configuration config;
    std::chrono::milliseconds kafkaConsumerTimeout;
    std::unique_ptr<cppkafka::Consumer> consumer;
};

typedef std::shared_ptr<KafkaSource> KafkaSourcePtr;
}// namespace NES
#endif// NES_INCLUDE_SOURCES_KAFKASOURCE_HPP_
