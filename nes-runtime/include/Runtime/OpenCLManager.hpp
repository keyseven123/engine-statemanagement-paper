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

#ifndef NES_OPENCLMANAGER_H
#define NES_OPENCLMANAGER_H

#include <array>
#include <memory>
#include <string>
#include <vector>

#ifdef ENABLE_OPENCL
#ifdef __APPLE__
#include <OpenCL/cl.h>
#else
#include <CL/cl.h>
#endif
#else
// Define some OpenCL types, so that this header file compiles, and we don't need #ifdef's everywhere.
using cl_platform_id = unsigned;
using cl_device_id = unsigned;
#endif

namespace NES::Runtime {

/**
 * Data structure to hold the information needed by the ELEGANT Acceleration Service to compile a kernel for an OpenCL device.
 */
struct OpenCLDeviceInfo {
  public:
    OpenCLDeviceInfo(const cl_platform_id platformId,
                     const cl_device_id deviceId,
                     const std::string& platformVendor,
                     const std::string& platformName,
                     const std::string& deviceName,
                     bool doubleFPSupport,
                     std::array<size_t, 3> maxWorkItems,
                     unsigned deviceAddressBits,
                     const std::string& deviceType,
                     const std::string& deviceExtensions,
                     unsigned availableProcessors)
        : platformId(platformId), deviceId(deviceId), platformVendor(platformVendor), platformName(platformName),
          deviceName(deviceName), doubleFPSupport(doubleFPSupport), maxWorkItems(maxWorkItems),
          deviceAddressBits(deviceAddressBits), deviceType(deviceType), deviceExtensions(deviceExtensions),
          availableProcessors(availableProcessors) {}

  public:
    constexpr static unsigned GRID_DIMENSIONS = 3;

  public:
    cl_platform_id platformId;
    cl_device_id deviceId;
    std::string platformVendor;
    std::string platformName;
    std::string deviceName;
    bool doubleFPSupport;
    std::array<size_t, GRID_DIMENSIONS> maxWorkItems;
    unsigned deviceAddressBits;
    std::string deviceType;
    std::string deviceExtensions;
    unsigned availableProcessors;
};

/**
 * Retrieve and store information about installed OpenCL devices.
 */
class OpenCLManager {
  public:
    OpenCLManager();

  private:
    std::vector<OpenCLDeviceInfo> devices;
};

}// namespace NES::Runtime

#endif//NES_OPENCLMANAGER_H
