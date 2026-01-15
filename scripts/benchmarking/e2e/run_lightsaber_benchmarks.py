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

import argparse
import os
import shutil
import subprocess
import time
import argparse
import re
import csv
import requests
import socket

from scripts.benchmarking.utils import *

from urllib.request import urlretrieve

queries = {
    # "SG1": "/root/LightSaber/build/test/benchmarks/applications/smartgrid --query 1 --slots 128 --latency true",
    "SG1": "/root/LightSaber/build/test/benchmarks/applications/smartgrid --query 1 --hashtable-size 1024 --unbounded-size 1048576  --slots 128   --parallel-merge true --latency true",
    "SG2": "/root/LightSaber/build/test/benchmarks/applications/smartgrid --query 2 --slots 128 --latency true",
    # "SG2": "/root/LightSaber/build/test/benchmarks/applications/smartgrid --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 --parallel-merge true --latency true",
    "SG3": "/root/LightSaber/build/test/benchmarks/applications/smartgrid --query 3 --hashtable-size 2048 --unbounded-size 1048576  --slots 128  --unbounded-size 4194304 --parallel-merge true --latency true",
    "LRB1": "/root/LightSaber/build/test/benchmarks/applications/linear_road_benchmark --query 1 --unbounded-size 8388608 --circular-size 16777216 --batch-size 524288 --bundle-size 524288 --hashtable-size 256 --parallel-merge true --latency true",
    "LRB2": "/root/LightSaber/build/test/benchmarks/applications/linear_road_benchmark --query 2 --unbounded-size 16777216 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --parallel-merge true --latency true",
}

# parallelisms = ["1", "24"]
parallelisms = ["1", "4", "8", "16", "24"]
MAX_RUNTIME_PER_JOB = 10  # in seconds


def get_tmp_data_dir():
    # Get the hostname
    hostname = socket.gethostname()

    # Determine the tmp directory based on the hostname
    if hostname == "nschubert-thinkstation":
        data_dir = "/tmp/data"
    elif hostname == "hare":
        data_dir = "/data/tmp_data"
    elif hostname == "mif-ws":
        data_dir = "/tmp/data"
    elif hostname == "docker-hostname":
        data_dir = "/tmp/data"
    else:
        raise ValueError(f"Unknown hostname: {hostname}. Cannot determine vcpkg directory.")

    return data_dir


local_data_folder = get_tmp_data_dir()
csv_folder = os.path.join(os.getcwd(), "lightsaber_results")
csv_path = os.path.join(csv_folder, "all_queries.csv")

csv_fieldnames = [
    "query_name",
    "parallelism",
    "tuplesPerSecond"
]


def write_to_csv(query_name, avg_throughput, parallelism):
    # Write results to csv
    with open(csv_path, "a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        writer.writerow({
            "query_name": query_name,  # same as row[0]
            "parallelism": parallelism,
            "tuplesPerSecond": avg_throughput
        })


def run_command_in_docker(command, timeout=None):
    docker_run_cmd = (
        f"docker run -d --rm -v $(pwd):/root/LightSaber lightsaber /bin/bash -c \"{command}\""
    )
    print(f"Running {docker_run_cmd} at cwd {os.getcwd()}")
    result = subprocess.run(docker_run_cmd, shell=True, check=True, text=True, capture_output=True)
    container_id = result.stdout.strip()

    # Wait for the timeout and then stop the container
    if timeout:
        time.sleep(timeout)
        # Check if the container exists before stopping
        inspect_cmd = f"docker inspect -f '{{{{.State.Running}}}}' {container_id} 2>/dev/null"
        inspect_result = subprocess.run(inspect_cmd, shell=True, capture_output=True, text=True)
        if inspect_result.returncode == 0 and inspect_result.stdout.strip() == "true":
            subprocess.run(f"docker stop -t 1 {container_id}", shell=True, check=True)


def analyze_logs(log_path):
    # Use regex to find all "Average: <decimal>" values
    pattern = r'Average: (\d+\.?\d*) tuples/sec'
    print(f"log path abs : {os.path.abspath(log_path)}")
    with open(log_path) as f:
        log_content = f.read()
    averages = re.findall(pattern, log_content)

    # Convert to float
    averages = [float(avg) for avg in averages]
    print(f"Averages: {averages}")

    if averages and len(averages) > 0:
        return sum(averages) / len(averages)
    else:
        return 0


def main():
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Run Flink queries.")
    parser.add_argument("--all", action="store_true", help="Run all queries.")
    parser.add_argument("-q", "--queries", nargs="+", help="List of queries to run.")
    parser.add_argument("-p", "--parallelism", nargs="+", help="Parallelism to run the query with.")
    args = parser.parse_args()

    # Determine which queries to run
    queries_to_run = queries

    if not args.all and args.queries:
        # Filter queries based on the provided list
        queries_to_run = {k: v for k, v in queries.items() if k in args.queries}

    # Determine the parallelisms to run the queries with
    parallelisms_to_run = parallelisms
    if args.parallelism:
        # Filter queries based on the provided list
        parallelisms_to_run = args.parallelism

    print(",".join(queries_to_run.keys()))
    print(",".join(parallelisms_to_run))

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Store the current working directory
    original_dir = os.getcwd()

    try:
        os.chdir('LightSaber')

        if os.path.exists(csv_folder):
            shutil.rmtree(csv_folder)
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)

        # Prepare csv file
        with open(csv_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
            writer.writeheader()

        # Built Docker Image and Lightsaber
        run_command("docker build --tag=\"lightsaber\" .")
        run_command_in_docker("bash /root/LightSaber/scripts/build.sh")

        for query_name, query_command in queries_to_run.items():
            for parallelism in parallelisms_to_run:
                print(f"Running {query_name} with {parallelism} threads...")
                run_command_in_docker(
                    f"{query_command} --threads {parallelism} | tee /root/LightSaber/{query_name}.log",
                    MAX_RUNTIME_PER_JOB)
                avg_throughput = analyze_logs(f"{query_name}.log")
                write_to_csv(query_name, avg_throughput, parallelism)

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
    except KeyboardInterrupt:
        print("Process interrupted by user")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Change back to the original directory
        os.chdir(original_dir)

    abs_csv_path = os.path.abspath(csv_path)
    print(f"CSV Measurement file can be found in {abs_csv_path}")


if __name__ == "__main__":
    main()
