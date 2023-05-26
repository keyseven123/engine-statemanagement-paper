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

#ifndef NES_CORE_INCLUDE_UTIL_UTILITYFUNCTIONS_HPP_
#define NES_CORE_INCLUDE_UTIL_UTILITYFUNCTIONS_HPP_

#include <Common/Identifiers.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <any>
#include <functional>
#include <map>
#include <set>
#include <string>

/**
 * @brief a collection of shared utility functions
 */
namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Query;
using QueryPtr = std::shared_ptr<Query>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Query

}// namespace Catalogs

namespace Util {
namespace detail {
/**
* @brief set of helper functions for splitting for different types
* @return splitting function for a given type
*/
template<typename T>
struct SplitFunctionHelper {};

template<>
struct SplitFunctionHelper<std::string> {
    static constexpr auto FUNCTION = [](std::string x) {
        return x;
    };
};

template<>
struct SplitFunctionHelper<uint64_t> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return uint64_t(std::atoll(str.c_str()));
    };
};

template<>
struct SplitFunctionHelper<uint32_t> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return uint32_t(std::atoi(str.c_str()));
    };
};

template<>
struct SplitFunctionHelper<int> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return std::atoi(str.c_str());
    };
};

template<>
struct SplitFunctionHelper<double> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return std::atof(str.c_str());
    };
};

}// namespace detail
/**
 * @brief escapes all non text characters in a input string, such that the string could be processed as json.
 * @param s input string.
 * @return result sing.
 */
std::string escapeJson(const std::string& str);

/**
 * @brief removes leading and trailing whitespaces
 */
std::string trim(std::string s);

/**
 * @brief removes leading and trailing characters of trimFor
 */
std::string trim(std::string s, char trimFor);

/**
 * @brief Checks if a string ends with a given string.
 * @param fullString
 * @param ending
 * @return true if it ends with the given string, else false
 */
bool endsWith(const std::string& fullString, const std::string& ending);

/**
 * @brief Checks if a string starts with a given string.
 * @param fullString
 * @param start
 * @return true if it ends with the given string, else false
 */
uint64_t numberOfUniqueValues(std::vector<uint64_t>& values);

/**
 * @brief Get number of unique elements
 * @param fullString
 * @param start
 * @return true if it ends with the given string, else false
 */
bool startsWith(const std::string& fullString, const std::string& ending);

/**
 * @brief transforms the string to an upper case version
 * @param string
 * @return string
 */
std::string toUpperCase(std::string string);

/**
* @brief splits a string given a delimiter into multiple substrings stored in a T vector
* the delimiter is allowed to be a string rather than a char only.
* @param data - the string that is to be split
* @param delimiter - the string that is to be split upon e.g. / or -
* @param fromStringtoT - the function that converts a string to an arbitrary type T
* @return
*/
template<typename T>
std::vector<T> splitWithStringDelimiter(const std::string& inputString,
                                        const std::string& delim,
                                        std::function<T(std::string)> fromStringToT = detail::SplitFunctionHelper<T>::FUNCTION) {
    std::string copy = inputString;
    size_t pos = 0;
    std::vector<T> elems;
    while ((pos = copy.find(delim)) != std::string::npos) {
        elems.push_back(fromStringToT(copy.substr(0, pos)));
        copy.erase(0, pos + delim.length());
    }
    if (!copy.substr(0, pos).empty()) {
        elems.push_back(fromStringToT(copy.substr(0, pos)));
    }

    return elems;
}

/**
* @brief Outputs a tuple buffer in text format
* @param buffer the tuple buffer
* @return string of tuple buffer
*/
std::string printTupleBufferAsText(Runtime::TupleBuffer& buffer);

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);

/**
* @brief this method checks if the object is null
* @return pointer to the object
*/
template<typename T>
std::shared_ptr<T> checkNonNull(std::shared_ptr<T> ptr, const std::string& errorMessage) {
    NES_ASSERT(ptr, errorMessage);
    return ptr;
}

/**
 * @brief function to replace all string occurrences
 * @param data input string will be replaced in-place
 * @param toSearch search string
 * @param replaceStr replace string
 */
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

/**
 * @brief method to get the schema as a csv string
 * @param schema
 * @return schema as csv string
 */
std::string toCSVString(const SchemaPtr& schema);

//TODO: remove this when folly gets updated
std::string base64Encode(std::string inputBuffer);
std::string base64Decode(std::string inputBuffer);

/**
 * @brief Returns the next free operator id
 * @return operator id
 */
OperatorId getNextOperatorId();

/**
* @brief Returns the next free pipeline id
* @return node id
*/
uint64_t getNextPipelineId();

/**
 * @brief This function replaces the first occurrence of search term in a string with the replace term.
 * @param origin - The original string that is to be manipulated
 * @param search - The substring/term which we want to have replaced
 * @param replace - The string that is replacing the search term.
 * @return
 */
std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace);

/**
 *
 * @param queryPlan queryIdAndCatalogEntryMapping to which the properties are assigned
 * @param properties properties to assign
 * @return true if the assignment success, and false otherwise
 */
bool assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan, std::vector<std::map<std::string, std::any>> properties);

/**
 * @brief: Update the source names by sorting and then concatenating the source names from the sub- and query plan
 * @param string consumed sources of the current queryPlan
 * @param string consumed sources of the subQueryPlan
 * @return string with new source name
 */
std::string updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed);

/**
 * @brief Truncates the file and then writes the header string as is to the file
 * @param csvFileName
 * @param header
 */
void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header);

/**
 * @brief Appends the row as is to the csv file
 * @param csvFileName
 * @param row
 */
void writeRowToCsvFile(const std::string& csvFileName, const std::string& row);

/**
 * @brief Creates multiple TupleBuffers from the csv file until the lastTimeStamp has been read
 * @param csvFile
 * @param schema
 * @param timeStampFieldName
 * @param lastTimeStamp
 * @param bufferManager
 * @return Vector of TupleBuffers
 */
[[maybe_unused]] std::vector<Runtime::TupleBuffer> createBuffersFromCSVFile(const std::string& csvFile,
                                                                            const SchemaPtr& schema,
                                                                            Runtime::BufferManagerPtr bufferManager,
                                                                            const std::string& timeStampFieldName,
                                                                            uint64_t lastTimeStamp);

/**
 * @brief Returns the physical types of all fields of the schema
 * @param schema
 * @return PhysicalTypes of the schema's field
 */
std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema);

/**
 * Partition a vector in n chunks, e.g., ([1, 2, 3, 4, 5], 3) -> [[1, 2], [3, 4], [5]]
 * @param input the vector
 * @param n the chunks
 * @return the chunked vector
 */
template<typename T>
std::vector<std::vector<T>> partition(const std::vector<T>& vec, size_t n) {
    std::vector<std::vector<T>> outVec;
    size_t length = vec.size() / n;
    size_t remain = vec.size() % n;

    size_t begin = 0;
    size_t end = 0;
    for (size_t i = 0; i < std::min(n, vec.size()); ++i) {
        end += (remain > 0) ? (length + !!(remain--)) : length;
        outVec.push_back(std::vector<T>(vec.begin() + begin, vec.begin() + end));
        begin = end;
    }
    return outVec;
}

/**
 * @brief appends newValue until the vector contains a minimum of newSize elements
 * @tparam T
 * @param vector the vector
 * @param newSize the size of the padded vector
 * @param newValue the value that should be added
 */
template<typename T>
void padVectorToSize(std::vector<T>& vector, size_t newSize, T newValue) {
    while (vector.size() < newSize) {
        vector.push_back(newValue);
    }
}
};// namespace Util
}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_UTILITYFUNCTIONS_HPP_
