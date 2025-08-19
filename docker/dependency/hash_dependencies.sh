#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# calculates a hash of the dependencies (vcpkg + stuff installed in dockerfiles like clang, mold, etc.)
# to enable pre-built images.

# exit if any command (even inside a pipe) fails or an undefined variable is used.
set -euo pipefail

# Function to change directory to where the specified folders are found
change_to_directory_with_folders() {
    local required_folders=("$@")
    local current_dir=$(pwd)

    while true; do
        cd $current_dir
        local all_folders_found=true
        for folder in "${required_folders[@]}"; do
            if [ ! -d "$current_dir/$folder" ]; then
                all_folders_found=false
                break
            fi
        done

        if [ "$all_folders_found" = true ]; then
            return 0
        fi

        local parent_dir=$(dirname "$current_dir")
        if [ "$parent_dir" = "$current_dir" ]; then
            echo "Error: Reached the root directory and could not find the required folders."
            return 1
        fi

        current_dir=$parent_dir
    done
}

# Try to find the top-level directory of the repository
change_to_directory_with_folders "vcpkg" "grpc"
if [ $? -eq 1 ]; then
    echo "Failed to find the Nebulastream repository root."
    exit 1
fi

# paths of dirs or files that affect the dependency images
#
# Do not use trailing slashes on dirs since this leads to diverging hashes on macos.
HASH_PATHS=(
  vcpkg
  docker/dependency
)

# Find all files, convert CRLF to LF on-the-fly, then hash
find "${HASH_PATHS[@]}" -type f -exec sh -c ' printf "%s  %s\n" "$(tr -d "\r" < "$1" | sha256sum | cut -d " " -f1)" "$1"' sh {} \; \
  | LC_ALL=C sort -k 2 \
  | sha256sum \
  | cut -d ' ' -f1
