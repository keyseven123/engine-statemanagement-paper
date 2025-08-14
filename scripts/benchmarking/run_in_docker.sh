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

set -e

# Initialize variables to track the options
rootless=false
script_to_run=""

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --rootless) rootless=true ;;
        --script)
            if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
                script_to_run="$2"
                shift
            else
                echo "Error: Argument for --script is missing"
                exit 1
            fi
            ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

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

# Call the script to install the local Docker environment
echo "rootless: $rootless"
if [[ "$rootless" == false ]]; then
  bash ./scripts/install-local-docker-environment.sh --libstdcxx -l
else
  bash ./scripts/install-local-docker-environment.sh --libstdcxx -l -r
fi

# Check if the script file exists
if [ ! -f "$script_to_run" ]; then
    echo "Script file $script_to_run does not exist."
    exit 1
fi

# Run the script inside the Docker container
docker run --hostname docker-hostname --privileged --ulimit nproc=65535:65535 --ulimit stack=-1:-1 --rm -v "$(pwd):/nebulastream" nebulastream/nes-development:local bash -c "cd /nebulastream && bash \"$script_to_run\""
