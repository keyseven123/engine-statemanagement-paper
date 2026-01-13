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
import argparse
import csv
import requests
import socket
from scripts.benchmarking.utils import *

from urllib.request import urlretrieve

csv_folder = "results"
csv_path = os.path.join(csv_folder, "all_queries_spark.csv")
parallelisms = ["1", "4", "8", "16", "24"] #["1", "24"]


def parse_throughput(stdout):
    # Split the log into lines
    lines = stdout.strip().split('\n')

    # Find the start of the CSV section
    csv_started = False
    csv_rows = []

    for line in lines:
        # Check if the line indicates the start of the CSV section
        if line.strip() == "[info] Query,InputRows,OutputRows,TimeMs,ThroughputRowsPerSec":
            csv_started = True
            continue  # Skip the header line
        # If CSV section has started and the line is not empty, add it to the result
        if csv_started and line.strip():
            # Remove the [info] prefix and any leading/trailing whitespace
            csv_line = line.replace("[info] ", "").strip()
            csv_rows.append(csv_line)

    return csv_rows

csv_fieldnames = [
    "query_name",
    "input_rows",
    "output_rows",
    "execution_time_ms",
    "tuplesPerSecond",
    "parallelism"
]

def main():
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Run Flink queries.")
    parser.add_argument("-p", "--parallelism", nargs="+", help="Parallelism to run the query with.")
    args = parser.parse_args()


    # Determine the parallelisms to run the queries with
    parallelisms_to_run = parallelisms
    if args.parallelism:
        # Filter queries based on the provided list
        parallelisms_to_run = args.parallelism

    print(",".join(parallelisms_to_run))

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Store the current working directory
    original_dir = os.getcwd()

    try:
        os.chdir('spark')
        create_folder_and_remove_if_exists(csv_folder)

        # Prepare csv file
        with open(csv_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
            writer.writeheader()

        # Downloading the data sets and building the docker image
        run_command("./scripts/download_data.sh")
        run_command("docker build -t spark-streaming-benchmark .")

        # Running all queries with different no. parallelism
        for parallelism in parallelisms_to_run:
            stdout = run_command(f"docker run -v $(pwd)/data:/data -e SPARK_DRIVER_MEMORY=64g -e SPARK_CORES={parallelism} spark-streaming-benchmark:latest")
            csv_rows = parse_throughput(stdout)
            with open(csv_path, 'a') as file:
                for row in csv_rows[:-1]:
                    row += f",{parallelism}\n"
                    file.write(row)

        print(f"CSV File is located at {os.path.abspath(csv_path)}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
    except KeyboardInterrupt:
        print("Process interrupted by user")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Change back to the original directory
        os.chdir(original_dir)


if __name__ == "__main__":
    main()
