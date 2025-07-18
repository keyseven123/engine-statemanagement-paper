#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Python script that runs the below systest files for different worker configurations
"""

import subprocess
import json
import os
import csv
import shutil
import itertools

#### Benchmark Configurations
build_dir = os.path.join(".", "build_dir")
working_dir = os.path.join(build_dir, "working_dir")
csv_file_path = "results_nebulastream.csv"
benchmark_json_file = os.path.abspath(os.path.join(working_dir, "BenchmarkResults.json"))
systest_executable = os.path.join(build_dir, "nes-systests/systest/systest")
test_data_dir = os.path.abspath(os.path.join(build_dir, "nes-systests/testdata"))
cmake_flags = ("-G Ninja "
               "-DCMAKE_BUILD_TYPE=Release "
               "-DCMAKE_TOOLCHAIN_FILE=/home/nils/remote_server/vcpkg/scripts/buildsystems/vcpkg.cmake "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=1 "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")
NUM_RUNS_PER_EXPERIMENT = 1

#### Worker Configurations
allExecutionModes = ["COMPILER"]  # ["COMPILER", "INTERPRETER"]
allNumberOfWorkerThreads = [1, 2, 4, 8, 16] #[1, 4]
allNumberOfBuffersInGlobalBufferManagers = [4000000] #[500000] if buffer size is 102400
allJoinStrategies = ["HASH_JOIN"]
allNumberOfEntriesSliceCaches = [5]
allSliceCacheTypes = ["LRU"]
allBufferSizes = [8196] #[100 * 1024]
allPageSizes = [4096]

#### Queries
allQueries = {
    "CM1": "nes-systests/benchmark/memory-source/ClusterMonitoring.test:01",
    "CM2": "nes-systests/benchmark/memory-source/ClusterMonitoring.test:03",
    "LRB1": "nes-systests/benchmark/memory-source/LinearRoadBenchmark.test:01",
    "LRB2": "nes-systests/benchmark/memory-source/LinearRoadBenchmark.test:02",
    "MA": "nes-systests/benchmark/memory-source/Manufacturing.test:01",
    "SG1": "nes-systests/benchmark/memory-source/SmartGrid.test:01",
    "SG2": "nes-systests/benchmark/memory-source/SmartGrid.test:02",
    "SG3": "nes-systests/benchmark/memory-source/SmartGrid.test:03",
    "NM1": "nes-systests/benchmark/memory-source/Nexmark_multiple_GB_of_Bids.test:02",
    "NM2": "nes-systests/benchmark/memory-source/Nexmark_multiple_GB_of_Bids.test:03",
    "NM5": "nes-systests/benchmark/memory-source/Nexmark_multiple_GB_of_Bids.test:04",
    "NM8": "nes-systests/benchmark/memory-source/Nexmark_multiple_GB_of_Bids.test:05",
    "NM8_Variant": "nes-systests/benchmark/memory-source/Nexmark_multiple_GB_of_Bids.test:06",
    "YSB1k": "nes-systests/benchmark/memory-source/YahooStreamingBenchmark_more_data.test:01",
    "YSB10k": "nes-systests/benchmark/memory-source/YahooStreamingBenchmark_more_data.test:02",
}


def check_repository_root():
    """Check if the script is being run from the repository root."""
    expected_dirs = ["nes-sources", "nes-sql-parser", "nes-systests"]
    current_dir = os.getcwd()
    contents = os.listdir(current_dir)

    if not all(expected_dir in contents for expected_dir in expected_dirs):
        raise RuntimeError("The script is not being run from the repository root.")


def create_folder_and_remove_if_exists(folder_path):
    """
    Create a folder and remove it if it already exists.
    :param folder_path: Path of the folder to create.
    """
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Remove the folder and all its contents
        shutil.rmtree(folder_path)
        print(f"Removed existing folder: {folder_path}")

    # Create the folder
    os.makedirs(folder_path)
    print(f"Created folder: {folder_path}")


def run_command(command, cwd=None):
    result = subprocess.run(command, cwd=cwd, shell=True, check=True, text=True, capture_output=True)
    return result.stdout


def compile_nebulastream():
    cmake_command = f"cmake {cmake_flags} -S . -B {build_dir}"
    build_command = f"cmake --build {build_dir}"

    print("Running cmake...")
    run_command(cmake_command)
    print("Building the project...")
    run_command(build_command)


def initialize_csv_file():
    """Initialize the CSV file with headers."""
    print("Initializing CSV file...")
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = [
            'bytesPerSecond', 'query_name', 'time', 'tuplesPerSecond',
            'executionMode', 'numberOfWorkerThreads', 'buffersInGlobalBufferManager',
            'joinStrategy', 'numberOfEntriesSliceCaches', 'sliceCacheType',
            'bufferSizeInBytes', 'pageSize'
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        print("CSV file initialized with headers.")


def run_benchmark(config, query, queryIdx, workerConfigIdx, no_combinations, no_queries):
    # Create the working directory
    create_folder_and_remove_if_exists(working_dir)

    # Running the query with a particular worker configuration
    worker_config = (f"--worker.queryEngine.numberOfWorkerThreads={numberOfWorkerThreads} "
                     f"--worker.queryOptimizer.executionMode={executionMode} "
                     f"--worker.numberOfBuffersInGlobalBufferManager={buffersInGlobalBufferManager} "
                     f"--worker.bufferSizeInBytes={bufferSizeInBytes} "
                     f"--worker.queryOptimizer.joinStrategy={joinStrategy} "
                     f"--worker.queryOptimizer.pageSize={pageSize} "
                     f"--worker.queryOptimizer.operatorBufferSize={bufferSizeInBytes} "
                     f"--worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache={numberOfEntriesSliceCaches} "
                     f"--worker.queryOptimizer.sliceCache.sliceCacheType={sliceCacheType}")

    benchmark_command = f"{systest_executable} -b -t {allQueries[query]} --data {test_data_dir} --workingDir={working_dir} -- {worker_config}"

    print(
        f"Running {query} [{queryIdx}/{no_queries}] for worker configuration [{workerConfigIdx}/{no_combinations}]...")
    run_command(benchmark_command)

    # Parse and save benchmark results
    try:
        with open(benchmark_json_file, 'r') as file:
            content = file.read()
            benchmark_results = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Failed to parse benchmark output as JSON from {benchmark_json_file}")
        print(f"Error details: {e}")
        benchmark_results = []
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        benchmark_results = []
        exit(1)

    with open(csv_file_path, mode='a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=[
            'bytesPerSecond', 'query_name', 'time', 'tuplesPerSecond',
            'executionMode', 'numberOfWorkerThreads', 'buffersInGlobalBufferManager',
            'joinStrategy', 'numberOfEntriesSliceCaches', 'sliceCacheType',
            'bufferSizeInBytes', 'pageSize'
        ])
        for result in benchmark_results:
            result['query_name'] = query
            writer.writerow({**result, **config})
        print(f"Results for config {config} written to CSV.")


if __name__ == "__main__":
    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Create folder
    create_folder_and_remove_if_exists(build_dir)

    # Build NebulaStream
    compile_nebulastream()

    # Init csv files
    initialize_csv_file()

    # Iterate over all cross-product combinations for each query
    no_combinations = (
            len(allExecutionModes) *
            len(allNumberOfWorkerThreads) *
            len(allNumberOfBuffersInGlobalBufferManagers) *
            len(allJoinStrategies) *
            len(allNumberOfEntriesSliceCaches) *
            len(allSliceCacheTypes) *
            len(allBufferSizes) *
            len(allPageSizes)
    )
    no_queries = len(allQueries)
    for queryIdx, query in enumerate(allQueries):
        workerConfigIdx = 0

        combinations = itertools.product(allExecutionModes, allNumberOfWorkerThreads,
                                         allNumberOfBuffersInGlobalBufferManagers, allJoinStrategies,
                                         allNumberOfEntriesSliceCaches, allSliceCacheTypes, allBufferSizes,
                                         allPageSizes)
        for [executionMode, numberOfWorkerThreads, buffersInGlobalBufferManager, joinStrategy,
             numberOfEntriesSliceCaches,
             sliceCacheType, bufferSizeInBytes, pageSize] in combinations:
            workerConfigIdx += 1

            config = {
                'executionMode': executionMode,
                'numberOfWorkerThreads': numberOfWorkerThreads,
                'buffersInGlobalBufferManager': buffersInGlobalBufferManager,
                'joinStrategy': joinStrategy,
                'numberOfEntriesSliceCaches': numberOfEntriesSliceCaches,
                'sliceCacheType': sliceCacheType,
                'bufferSizeInBytes': bufferSizeInBytes,
                'pageSize': pageSize
            }

            for i in range(NUM_RUNS_PER_EXPERIMENT):
                run_benchmark(config, query, queryIdx + 1, workerConfigIdx, no_combinations, no_queries)
