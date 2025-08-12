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
bash ./scripts/install-local-docker-environment.sh --libstdcxx -l

# Check if the --script argument is provided
if [ "$#" -ne 2 ] || [ "$1" != "--script" ]; then
    echo "Usage: $0 --script <path-to-script>"
    exit 1
fi

# Path to the script to be run inside the Docker container
SCRIPT_PATH=$2

# Check if the script file exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Script file $SCRIPT_PATH does not exist."
    exit 1
fi

# Run the script inside the Docker container
docker run --hostname docker-hostname --rm -v "$(pwd):/nebulastream" nebulastream/nes-development:local bash -c "cd /nebulastream && bash \"$SCRIPT_PATH\""
