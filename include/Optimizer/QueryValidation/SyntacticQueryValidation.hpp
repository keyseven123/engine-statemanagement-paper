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

#ifndef NES_OPTIMIZE_SYNTACTIC_QUERY_VALIDATION_HPP
#define NES_OPTIMIZE_SYNTACTIC_QUERY_VALIDATION_HPP

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <memory>

namespace NES {

/**
 * @brief This class is responsible for Syntactic Query Validation
 */
class SyntacticQueryValidation {
  private:
    /**
         * @brief Throws InvalidQueryException with formatted exception message
         */
    void handleException(const std::exception& ex);

  public:
    /**
         * @brief Checks the syntactic validity of a Query string
         */
    void checkValidity(std::string inputQuery);

    /**
         * @brief Checks the syntactic validity of a Query string and returns the created Query object
         */
    QueryPtr checkValidityAndGetQuery(std::string inputQuery);
};

typedef std::shared_ptr<SyntacticQueryValidation> SyntacticQueryValidationPtr;

}// namespace NES

#endif//NES_OPTIMIZE_SYNTACTIC_QUERY_VALIDATION_HPP