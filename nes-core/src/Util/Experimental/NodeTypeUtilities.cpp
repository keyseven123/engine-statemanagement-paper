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
#include <Util/Experimental/NodeTypeUtilities.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Spatial::Util {

Experimental::SpatialType NodeTypeUtilities::stringToNodeType(const std::string nodeTypeString) {
    if (nodeTypeString == "NO_LOCATION") {
        return Experimental::SpatialType::NO_LOCATION;
    } else if (nodeTypeString == "FIXED_LOCATION") {
        return Experimental::SpatialType::FIXED_LOCATION;
    } else if (nodeTypeString == "MOBILE_NODE") {
        return Experimental::SpatialType::MOBILE_NODE;
    }
    return Experimental::SpatialType::INVALID;
}

Experimental::SpatialType NodeTypeUtilities::protobufEnumToNodeType(NES::Spatial::Protobuf::SpatialType nodeType) {
    switch (nodeType) {
        case NES::Spatial::Protobuf::SpatialType::NO_LOCATION: return Experimental::SpatialType::NO_LOCATION;
        case NES::Spatial::Protobuf::SpatialType::FIXED_LOCATION: return Experimental::SpatialType::FIXED_LOCATION;
        case NES::Spatial::Protobuf::SpatialType::MOBILE_NODE: return Experimental::SpatialType::MOBILE_NODE;
        case NES::Spatial::Protobuf::SpatialType_INT_MIN_SENTINEL_DO_NOT_USE_: return Experimental::SpatialType::INVALID;
        case NES::Spatial::Protobuf::SpatialType_INT_MAX_SENTINEL_DO_NOT_USE_: return Experimental::SpatialType::INVALID;
    }
    return Experimental::SpatialType::INVALID;
}

std::string NodeTypeUtilities::toString(const Experimental::SpatialType nodeType) {
    switch (nodeType) {
        case Experimental::SpatialType::NO_LOCATION: return "NO_LOCATION";
        case Experimental::SpatialType::FIXED_LOCATION: return "FIXED_LOCATION";
        case Experimental::SpatialType::MOBILE_NODE: return "MOBILE_NODE";
        case Experimental::SpatialType::INVALID: return "INVALID";
    }
}

NES::Spatial::Protobuf::SpatialType NodeTypeUtilities::toProtobufEnum(Experimental::SpatialType nodeType) {
    switch (nodeType) {
        case Experimental::SpatialType::NO_LOCATION: return NES::Spatial::Protobuf::SpatialType::NO_LOCATION;
        case Experimental::SpatialType::FIXED_LOCATION: return NES::Spatial::Protobuf::SpatialType::FIXED_LOCATION;
        case Experimental::SpatialType::MOBILE_NODE: return NES::Spatial::Protobuf::SpatialType::MOBILE_NODE;
        case Experimental::SpatialType::INVALID:
            NES_FATAL_ERROR("cannot construct protobuf enum from invalid spatial type, exiting")
            exit(EXIT_FAILURE);
    }
}
}// namespace NES::Spatial::Util