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

#ifndef NES_LOCATIONCATALOG_H
#define NES_LOCATIONCATALOG_H

#include <map>

#include "GeoNode.h"

namespace NES {

    class LocationCatalog {

      private:
        std::map<int, GeoNode> nodes;

      public:
        LocationCatalog()  = default;
        void addNode(int nodeId);
        void updateNodeLocation(int nodeId, GeoLocation location);
        bool contains(int nodeId);
        GeoNode getNode(int nodeId);
        uint64_t size();
    };
}

#endif//NES_LOCATIONCATALOG_H
