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

#ifndef NES_TESTS_UTIL_PROTOBUF_MESSAGE_FACTORY_HPP_
#define NES_TESTS_UTIL_PROTOBUF_MESSAGE_FACTORY_HPP_

#include <Catalogs/UDF/UdfCatalog.hpp>
#include <UdfCatalogService.pb.h>
#include <API/AttributeField.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>

namespace NES {

/**
 * @brief Factory for Protobuf messages used in test code.
 */
class ProtobufMessageFactory {
  public:
    /**
     * @brief Construct a RegisterJavaUdfRequest protobuf message.
     * @see UdfCatalog::registerJavaUdf
     */
    static RegisterJavaUdfRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const Catalogs::UDF::JavaSerializedInstance& serializedInstance,
                                                               const Catalogs::UDF::JavaUdfByteCodeList& byteCodeList,
                                                               const SchemaPtr& outputSchema) {
        auto request = RegisterJavaUdfRequest{};
        // Set udfName
        request.set_udf_name(udfName);
        auto* udfDescriptor = request.mutable_java_udf_descriptor();
        // Set udfClassName, methodName, and serializedInstance
        udfDescriptor->set_udf_class_name(udfClassName);
        udfDescriptor->set_udf_method_name(methodName);
        udfDescriptor->set_serialized_instance(serializedInstance.data(), serializedInstance.size());
        // Set byteCodeList
        for (const auto& [className, byteCode] : byteCodeList) {
            auto* javaClass = udfDescriptor->add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(std::string{byteCode.begin(), byteCode.end()});
        }
        // Set outputSchema
        SchemaSerializationUtil::serializeSchema(outputSchema, udfDescriptor->mutable_outputschema());
        return request;
    }

    static RegisterJavaUdfRequest createDefaultRegisterJavaUdfRequest() {
        return createRegisterJavaUdfRequest("my_udf",
                                           "some_package.my_udf",
                                           "udf_method",
                                           {1},
                                           {{"some_package.my_udf", {1}}},
                                           std::make_shared<Schema>()->addField("field",
                                                                             DataTypeFactory::createUInt64()));

    }
};

}// namespace NES
#endif// NES_TESTS_UTIL_PROTOBUF_MESSAGE_FACTORY_HPP_
