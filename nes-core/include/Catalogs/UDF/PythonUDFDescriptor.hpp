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

#ifndef NES_CORE_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_

#include <Catalogs/UDF/UDFDescriptor.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <memory>
#include <string>

namespace NES::Catalogs::UDF {

class PythonUDFDescriptor;
using PythonUDFDescriptorPtr = std::shared_ptr<PythonUDFDescriptor>;

class PythonUDFDescriptor : public UDFDescriptor {
  public:
    PythonUDFDescriptor(const std::string& methodName, int numberOfArgs, DataTypePtr& returnType);

    static PythonUDFDescriptorPtr create(const std::string& methodName, int numberOfArgs, DataTypePtr& returnType) {
        return std::make_shared<PythonUDFDescriptor>(methodName, numberOfArgs, returnType);
    }

    /**
 * @brief Return the number of arguments for the UDF.
 * @return The number of arguments of the UDF method.
 */
    [[nodiscard]] int getNumberOfArgs() const { return numberOfArgs; }

  private:
    const int numberOfArgs;
    const DataTypePtr returnType;
};
}// namespace NES::Catalogs::UDF
#endif// NES_CORE_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_
