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

import subprocess
import json
import re
import os
import csv
import shutil
import itertools
import time
import socket
import yaml
import pandas as pd
from scripts.benchmarking.utils import *

#### Benchmark Configurations
build_dir = os.path.join(".", "build_dir")
working_dir = os.path.join(build_dir, "working_dir")
csv_file_path = "results_nebulastream.csv"
config_file = "config.yaml"
single_node_executable = os.path.join(build_dir, "nes-single-node-worker/nes-single-node-worker")
nebuli_executable = os.path.join(build_dir, "nes-nebuli/nes-nebuli --debug")
tcp_server_executable = os.path.join(build_dir, "scripts/benchmarking/work-dealing/tcp-server/tcpserver")
cmake_flags = ("-G Ninja "
               "-DCMAKE_BUILD_TYPE=Release "
               f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=1 "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")
NUM_RUNS_PER_EXPERIMENT = 1
EMIT_RATE_TUPLES_PER_SECOND = str(1 * 1000 * 1000)
WAIT_BETWEEN_COMMANDS_SHORT = 2
WAIT_BETWEEN_COMMANDS_LONG = 5
WAIT_BETWEEN_ADDING_NEW_QUERY = 10
WAIT_BEFORE_SIGKILL = 10

#### Worker Configurations
allExecutionModes = ["COMPILER"]  # ["COMPILER", "INTERPRETER"]
allNumberOfWorkerThreads = [8]
allNumberOfBuffersInGlobalBufferManagers = [4000000]  # [500000] if buffer size is 102400
allJoinStrategies = ["HASH_JOIN"]
allNumberOfEntriesSliceCaches = [5]
allSliceCacheTypes = ["LRU"]
allBufferSizes = [8196]  # [100 * 1024]
allPageSizes = [4096]
allResourceAssignments = ["WORK_STEALING", "WORK_DEALING_NEW_QUEUE_AND_THREAD"]

#### Queries
allQueries = {
    "agg": "scripts/benchmarking/work-dealing/configs/agg_query.yaml",
    "filter": "scripts/benchmarking/work-dealing/configs/agg_query.yaml"}
no_concurrent_queries = 128

def create_output_folder(appendix):
    timestamp = int(time.time())
    folder_name = f"ResourceAssignment_{timestamp}_{appendix}"
    create_folder_and_remove_if_exists(folder_name)
    print(f"Created folder {folder_name}...")
    return folder_name

def terminate_process_if_exists(process):
    try:
        process.terminate()
        process.wait(timeout=5)
        print(f"Process with PID {process.pid} terminated.")
    except subprocess.TimeoutExpired:
        print(f"Process with PID {process.pid} did not terminate within timeout. Sending SIGKILL.")
        process.kill()
        process.wait()
        print(f"Process with PID {process.pid} forcefully killed.")


def start_tcp_servers(starting_ports):
    processes = []
    ports = []
    max_retries = 10
    for port in starting_ports:
        for _ in range(max_retries):
            cmd = f"{tcp_server_executable} -p {port} -r {EMIT_RATE_TUPLES_PER_SECOND}"
            print(f"Trying to start tcp server with {cmd}")
            process = subprocess.Popen(cmd.split(" "), stdout=subprocess.DEVNULL)
            time.sleep(WAIT_BETWEEN_COMMANDS_SHORT)  # Allow server to start
            if process.poll() is not None and process.poll() != 0:
                # print(f"Failed to start tcp server with PID: {process.pid} and port: {port}")
                port = str(int(port) + random.randint(1, 10))
                terminate_process_if_exists(process)
                time.sleep(1)
            else:
                # print(f"Started tcp server with PID: {process.pid} and port: {port}")
                processes.append(process)
                ports.append(port)
                port = str(int(port) + 1)  # Increment the port for the next server
                break
        else:
            raise Exception(f"Failed to start the TCP server after {max_retries} attempts.")

    print(
        f"Started all TCP servers with the following <port, pid>: {list(zip(ports, [proc.pid for proc in processes]))}")
    return [processes, ports]


def start_single_node_worker(file_path_stdout):
    # Running the query with a particular worker configuration
    worker_config = (f"--worker.queryEngine.numberOfWorkerThreads={numberOfWorkerThreads} "
                     f"--worker.queryEngine.resourceAssignment={resourceAssignment} "
                     f"--worker.queryOptimizer.executionMode={executionMode} "
                     f"--worker.numberOfBuffersInGlobalBufferManager={buffersInGlobalBufferManager} "
                     f"--worker.bufferSizeInBytes={bufferSizeInBytes} "
                     f"--worker.queryOptimizer.joinStrategy={joinStrategy} "
                     f"--worker.queryOptimizer.pageSize={pageSize} "
                     f"--worker.queryOptimizer.operatorBufferSize={bufferSizeInBytes} "
                     f"--worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache={numberOfEntriesSliceCaches} "
                     f"--worker.queryOptimizer.sliceCache.sliceCacheType={sliceCacheType}")

    cmd = f"{single_node_executable} {worker_config}"
    print(f"Starting the single node worker with {cmd}")
    process = subprocess.Popen(cmd.split(" "), stdout=file_path_stdout)
    pid = process.pid
    print(f"Started single node worker with pid {pid}")
    return process

def submitting_query(query_file):
    cmd = f"cat {query_file} | {nebuli_executable} register -x -s localhost:8080"
    print(f"Submitting the query via {cmd}...")
    # shell=True is needed to pipe the output of cat to the register command
    try:
        result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE,  # Capture standard output
                                stderr=subprocess.PIPE,  # Capture standard error
                                text=True  # Decode output to a string
                                )
        # print(f"Submitted the query with the following output: {result.stdout.strip()} and error: {result.stderr.strip()}")
        query_id = result.stdout.strip()
        print(f"Submitted the query with id {query_id}")
        return query_id
    except subprocess.CalledProcessError as e:
        print("Command failed with exit status:", e.returncode)
        print("Error output:", e.stderr)
        exit(1)

def stop_query(query_id):
    cmd = f"{nebuli_executable} stop {query_id} -s localhost:8080"
    # print(f"Stopping the query via {cmd}...")
    process = subprocess.Popen(cmd.split(" "), stdout=subprocess.DEVNULL)
    return process

def copy_and_modify_query_config(old_config, new_config, tcp_source_name, new_port):
    # Loading the yaml file
    with open(old_config, 'r') as input_yaml_file:
        yaml_query_config = yaml.safe_load(input_yaml_file)

    # Update the logical name in the logical section
    yaml_query_config['logical'][0]['name'] = tcp_source_name

    # Update the query to use the new logical name
    old_logical_name = "tcp_source"  # Assuming the old name is tcp_source
    yaml_query_config['query'] = yaml_query_config['query'].replace(old_logical_name, tcp_source_name)

    # Update the physical logical reference to use the new logical name
    yaml_query_config['physical'][0]['logical'] = tcp_source_name

    # Update the socket port
    yaml_query_config['physical'][0]['sourceConfig']['socketPort'] = new_port

    # Save the updated content back to the YAML file
    with open(new_config, 'w') as file:
        yaml.dump(yaml_query_config, file, sort_keys=False)

def parse_log_to_csv(log_file_path, csv_file_path):
    # Regular expression to parse each log line
    log_pattern = re.compile(
        r'Throughput for queryId (\d+) in window (\d+)-(\d+) is \d+\.\d+ MB/s / (\d+\.\d+) (\w)Tup/s'
    )

    # List to store the extracted data
    data = []

    # Open the log file for reading
    with open(log_file_path, mode='r') as log_file:
        for line in log_file:
            # Use regex to find matches in the log line
            match = log_pattern.match(line)
            if match:
                query_id = match.group(1)
                start_timestamp = int(match.group(2))
                throughput_value = float(match.group(4))
                unit_prefix = match.group(5)

                # Convert throughput based on the unit prefix
                if unit_prefix == 'M':
                    throughput = throughput_value * 1e6  # Convert to actual value (million)
                elif unit_prefix == 'B':
                    throughput = throughput_value * 1e9  # Convert to actual value (billion)
                elif unit_prefix == 'k':
                    throughput = throughput_value * 1e3  # Convert to actual value (thousand)
                else:
                    throughput = throughput_value  # No conversion needed

                # Append the extracted data to the list
                data.append((start_timestamp, query_id, throughput))

    # Find the minimum timestamp to normalize
    min_timestamp = min(data, key=lambda x: x[0])[0]

    # Open the CSV file for writing
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        # Write the header
        writer.writerow(['normalized_timestamp', 'query_id', 'throughput'])
        # Write the normalized data to the CSV file
        for start_timestamp, query_id, throughput in data:
            normalized_timestamp = start_timestamp - min_timestamp
            writer.writerow([normalized_timestamp, query_id, throughput])

def concatenate_csv_files(folders, output_file, config_file):
    # Initialize an empty list to store DataFrames
    dfs = []

    # Regex for extracting query and

    # Traverse through all subdirectories and files
    for folder in folders:
        # Read the CSV file into a DataFrame
        file_path = os.path.join(folder, csv_file_path)
        df = pd.read_csv(file_path)

        # Read the YAML configuration file
        config_filepath = os.path.join(folder, config_file)
        with open(config_filepath, 'r') as file:
            current_config = yaml.safe_load(file)

        # Assign the entire current_config dictionary to the DataFrame
        df = df.assign(**current_config)

        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    if dfs:
        concatenated_df = pd.concat(dfs, ignore_index=True)

        # Write the concatenated DataFrame to a new CSV file
        concatenated_df.to_csv(output_file, index=False)
        print(f"Concatenated CSV file saved to {output_file}")
    else:
        print("No CSV files found.")

if __name__ == "__main__":
    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Create folder
    create_folder_and_remove_if_exists(build_dir)

    # Build NebulaStream
    compile_nebulastream(cmake_flags, build_dir)

    tcp_server_processes = []
    single_node_process = []
    stop_process = []

    # Iterate over all cross-product combinations
    no_combinations = (
            len(allExecutionModes) *
            len(allNumberOfWorkerThreads) *
            len(allNumberOfBuffersInGlobalBufferManagers) *
            len(allJoinStrategies) *
            len(allNumberOfEntriesSliceCaches) *
            len(allSliceCacheTypes) *
            len(allBufferSizes) *
            len(allPageSizes) *
            len(allResourceAssignments) *
            len(allQueries)
    )
    combinations = itertools.product(allExecutionModes, allNumberOfWorkerThreads,
                                     allNumberOfBuffersInGlobalBufferManagers, allJoinStrategies,
                                     allNumberOfEntriesSliceCaches, allSliceCacheTypes, allBufferSizes,
                                     allPageSizes, allResourceAssignments, allQueries)

    counter = 0
    new_folders = []
    for [executionMode, numberOfWorkerThreads, buffersInGlobalBufferManager, joinStrategy,
         numberOfEntriesSliceCaches,
         sliceCacheType, bufferSizeInBytes, pageSize, resourceAssignment, query] in combinations:
        try:
            counter += 1
            print(f"Running combination [{counter}/{no_combinations}]")

            # Creating new output folder for this benchmark run and writing the current combination to a file
            folder_name = create_output_folder(resourceAssignment + "_" + query)
            new_folders.append(folder_name)
            with (open(os.path.join(folder_name, config_file), 'w') as file):
                # Write the combination to the file
                config = {
                    "executionMode": executionMode,
                    "numberOfWorkerThreads": numberOfWorkerThreads,
                    "buffersInGlobalBufferManager": buffersInGlobalBufferManager,
                    "joinStrategy": joinStrategy,
                    "numberOfEntriesSliceCaches": numberOfEntriesSliceCaches,
                    "sliceCacheType": sliceCacheType,
                    "bufferSizeInBytes": bufferSizeInBytes,
                    "pageSize": pageSize,
                    "resourceAssignment": resourceAssignment,
                    "query": query
                }
                yaml.dump(config, file, default_flow_style=False)


            # Starting the single node worker
            file_path_stdout = os.path.join(folder_name, "SingleNodeStdout.log")
            with open(file_path_stdout, 'w') as stdout_file:
                single_node_process = start_single_node_worker(stdout_file)

            start_port = [5123]
            query_ids = []
            for concurrent_query_number in range(no_concurrent_queries):
                # Starting the tcp server(s)
                [tcp_server_processes, tcp_server_ports] = start_tcp_servers(start_port)

                # Changing the query yaml file to the new ports etc.
                new_query_config_name = os.path.join(folder_name, f"{query}_{concurrent_query_number}.yaml")
                copy_and_modify_query_config(allQueries[query], new_query_config_name, f"{query}_{concurrent_query_number}_source", tcp_server_ports[0])

                # Setting start ports for next query
                start_port[0] = tcp_server_ports[0] + 1

                # Waiting to give the tcp server time to start
                time.sleep(WAIT_BETWEEN_COMMANDS_LONG)

                # Submitting the query
                query_id = submitting_query(new_query_config_name)
                query_ids.append(query_id)

                # Waiting to give the engine time to start the query and for measuring the current throughput
                time.sleep(WAIT_BETWEEN_ADDING_NEW_QUERY)


            # Stopping all queries
            for query_id in query_ids:
                stop_process = stop_query(query_id)

            full_csv_path = os.path.join(folder_name, csv_file_path)
            parse_log_to_csv(file_path_stdout, full_csv_path)

        finally:
            time.sleep(WAIT_BEFORE_SIGKILL)  # Wait additional time before cleanup
            all_processes = tcp_server_processes + [single_node_process] + [stop_process]
            for proc in all_processes:
                print(f"Trying to terminate {proc}")
                if not proc:
                    continue
                terminate_process_if_exists(proc)

    # After all experiments have been run, we merge all csv files into one main csv file
    concat_file_name = "results_nebulastream_concat.csv"
    concatenate_csv_files(new_folders, concat_file_name, config_file)