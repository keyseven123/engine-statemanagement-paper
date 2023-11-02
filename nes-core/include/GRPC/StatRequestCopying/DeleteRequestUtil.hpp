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

#ifndef NES_NES_CORE_INCLUDE_GRPC_STATREQUESTCOPYING_DELETEREQUESTUTIL_HPP_
#define NES_NES_CORE_INCLUDE_GRPC_STATREQUESTCOPYING_DELETEREQUESTUTIL_HPP_

namespace NES {

namespace Experimental::Statistics {
class DeleteRequestParamObj;
}

class DeleteStat;

class DeleteRequestUtil {
  public:
    static void packDeleteRequest(const Experimental::Statistics::DeleteRequestParamObj& deleteRequestParamObj,
                                  DeleteStat* deleteRequest);

    static Experimental::Statistics::DeleteRequestParamObj unpackDeleteRequest(const DeleteStat* deleteRequest);
};

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_GRPC_STATREQUESTCOPYING_DELETEREQUESTUTIL_HPP_
