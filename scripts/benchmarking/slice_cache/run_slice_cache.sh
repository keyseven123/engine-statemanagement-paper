#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Check if the script is being run from the repository root
expected_dirs=("nes-sources" "nes-sql-parser" "nes-systests")
current_dir=$(pwd)
valid_root=true

for dir in "${expected_dirs[@]}"; do
    if [ ! -d "$current_dir/$dir" ]; then
        valid_root=false
        break
    fi
done

if [ "$valid_root" = false ]; then
    echo "Error: The script is not being run from the repository root."
    exit 1
fi

# Define build directory
build_dir="build_dir"

# Create build directory and remove it if it already exists
#rm -rf "$build_dir"
#mkdir "$build_dir"

# Determine the vcpkg directory based on the hostname
hostname=$(hostname)
if [ "$hostname" = "nils-ThinkStation-P3-Tower" ]; then
    vcpkg_dir="/home/nils/remote_server/vcpkg/scripts/buildsystems/vcpkg.cmake"
elif [ "$hostname" = "hare" ]; then
    vcpkg_dir="/data/vcpkg/scripts/buildsystems/vcpkg.cmake"
elif [ "$hostname" = "mif-ws" ]; then
    vcpkg_dir="/home/nschubert/remote_server/vcpkg/scripts/buildsystems/vcpkg.cmake"
else
    echo "Unknown hostname: $hostname. Cannot determine vcpkg directory."
    exit 1
fi

# CMake flags
cmake_flags=(
    "-G Ninja"
    "-DCMAKE_BUILD_TYPE=Release"
    "-DCMAKE_TOOLCHAIN_FILE=$vcpkg_dir"
    "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF"
    "-DNES_LOG_LEVEL:STRING=LEVEL_NONE"
    "-DNES_BUILD_NATIVE:BOOL=ON"
)

# CMake commands
cmake_command="cmake ${cmake_flags[*]} -S . -B $build_dir"
build_command="cmake --build $build_dir --target slice-micro-benchmark"

echo "Running cmake..."
eval "$cmake_command"

echo "Building the project..."
eval "$build_command"

# Run the final command
./build_dir/nes-physical-operators/tests/slice-micro-benchmark
