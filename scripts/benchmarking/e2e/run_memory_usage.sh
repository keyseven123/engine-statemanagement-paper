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

set -euo pipefail

# These are commands to track memory usage of NebulaStream
while true; do echo "$(date +%s.%N) $(grep VmRSS /proc/$(pidof nes-single-node-worker)/status)" ; sleep 0.1; done > memory_log.txt
/home/ls/dima/engine-statemanagement-paper/cmake-build-release/nes-single-node-worker/nes-single-node-worker --worker.bufferSizeInBytes=256000 --worker.defaultQueryExecution-operatorBufferSize=256000 --worker.queryEngine.numberOfWorkerThreads=24 --worker.numberOfBuffersInGlobalBufferManager=500000
cmake-build-release/nes-systests/systest/systest -t Nexmark_multiple_GB_of_Bids.test:5 -s localhost:8080