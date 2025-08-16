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

flink = "flink-2.0.0"
jar_path = os.path.join("target", "bench-flink_2.0-0.1-SNAPSHOT.jar")

queries = {
    "CM1": "clustermonitoring.CM1",
    "CM2": "clustermonitoring.CM2",
    "LRB1": "linearroad.LR1",
    "LRB2": "linearroad.LR2",
    "MA": "manufacturingequipment.MA",
    "SG1": "smartgrid.SG1",
    "SG2": "smartgrid.SG2",
    "SG3": "smartgrid.SG3",
    "YSB1k": "ysb.YSB1k",
    "YSB10k": "ysb.YSB10k",
    "NM1": "nexmark.NM1",
    "NM2": "nexmark.NM2",
    "NM5": "nexmark.NM5",
    "NM8": "nexmark.NM8",
    "NM8_Variant": "nexmark.NM8_Variant"
}

all_data_sets = {
    "CM1": ["/data/cluster_monitoring/google-cluster-data-original_1G.csv"],
    "CM2": ["/data/cluster_monitoring/google-cluster-data-original_1G.csv"],
    "LRB1": ["/data/lrb/linear_road_benchmark_5GB.csv"],
    "LRB2": ["/data/lrb/linear_road_benchmark_5GB.csv"],
    "MA": ["/data/manufacturing/manufacturing_1G.csv"],
    "SG1": ["/data/smartgrid/smartgrid-data_2GB.csv"],
    "SG2": ["/data/smartgrid/smartgrid-data_2GB.csv"],
    "SG3": ["/data/smartgrid/smartgrid-data_2GB.csv"],
    "YSB1k": ["/data/ysb/ysb1k_more_data_3GB.csv"],
    "YSB10k": ["/data/ysb/ysb10k_more_data_3GB.csv"],
    "NM1": ["/data/nexmark/bid_more_data_6GB.csv"],
    "NM2": ["/data/nexmark/bid_more_data_6GB.csv"],
    "NM5": ["/data/nexmark/bid_more_data_6GB.csv"],
    "NM8": ["/data/nexmark/auction_more_data_707MB.csv",
            "/data/nexmark/person_more_data_840MB.csv"]
}

num_of_records = [20 * 1000 * 1000]  # [10000, 1000000, 10000000]
parallelisms = ["1", "4", "8", "16", "24"] #["1", "4"]
MAX_RUNTIME_PER_JOB = 60  # in seconds


def get_tmp_data_dir():
    # Get the hostname
    hostname = socket.gethostname()

    # Determine the vcpkg directory based on the hostname
    if hostname == "nils-ThinkStation-P3-Tower":
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
csv_folder = "results"
csv_path = os.path.join(csv_folder, "all_queries.csv")

csv_fieldnames = [
    "query_name",
    "parallelism",
    "numOfRecords",
    "tuplesPerSecond"
]


def download_file_with_progress(url, local_path):
    """Download a file from a URL and save it to the local path with progress tracking."""
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024  # 1 KB
            progress_bar_length = 50
            downloaded = 0
            with open(local_path, 'wb') as f:
                for data in response.iter_content(block_size):
                    f.write(data)
                    downloaded += len(data)
                    progress = downloaded / total_size
                    bar = '█' * int(progress * progress_bar_length)
                    spaces = ' ' * (progress_bar_length - len(bar))
                    print(f"\rDownloading {os.path.basename(local_path)}: |{bar}{spaces}| {progress * 100:.2f}%",
                          end='')
            print(f"\nDownloaded {url} to {local_path}")
        else:
            print(f"Failed to download {url}")
    except Exception as e:
        print(f"An error occurred while downloading {url}: {e}")
        exit(1)


def download_data_sets(queries_to_run):
    """Download the data sets if they are not already present in the local folder."""
    if not os.path.exists(local_data_folder):
        os.makedirs(local_data_folder)

    # Download the mapping file
    datasets_base_url = "https://tubcloud.tu-berlin.de/s/28Tr2wTd73Ggeed/download?files"
    mapping_url = f"{datasets_base_url}=mapping.txt"
    mapping_response = requests.get(mapping_url)
    if mapping_response.status_code != 200:
        raise Exception(f"Failed to download mapping file: {mapping_response.status_code}")

    # Parse the mapping file
    mapping = {}
    for line in mapping_response.text.splitlines():
        # Split each line into original file name and MD5 name
        parts = line.strip().split(" --> ")
        if len(parts) == 2:
            original_name = parts[0].strip()
            md5_name = parts[1].strip()
            # Remove the .md5 extension if present
            if original_name.endswith('.md5'):
                original_name = original_name[:-4]
            mapping[original_name] = md5_name

    # Check and download files
    needed_data_sets = {k: v for k, v in all_data_sets.items() if k in queries_to_run.keys()}
    needed_files = set([item for value in needed_data_sets.values() for item in value])
    for file_name in needed_files:
        base_name = os.path.basename(file_name)
        md5_name = mapping.get(file_name, None)
        if md5_name:
            local_file_path = os.path.join(local_data_folder, base_name)
            if not os.path.exists(local_file_path):
                # Construct the download URL for the file
                download_url = f"{datasets_base_url}={md5_name}"
                print(f"File {file_name} not found. Scheduling download...")
                download_file_with_progress(download_url, local_file_path)
            else:
                print(f"File {file_name} already exists at {local_file_path}")
        else:
            print(f"No MD5 mapping found for {file_name}")
            exit(1)


def download_flink():
    flink_url = f"https://dlcdn.apache.org/flink/{flink}/{flink}-bin-scala_2.12.tgz"
    subprocess.run(["wget", flink_url, "-O", "flink.tgz"], check=True)
    subprocess.run(["tar", "-xvf", "flink.tgz"], check=True)
    os.remove("flink.tgz")


def prepare():
    # Set config file
    shutil.copy("flink_config.yaml", os.path.join(flink, "conf", "config.yaml"))
    # CLeanup log files
    subprocess.run(f"rm -rf {flink}/log/*", shell=True, check=True)
    # Stop Flink cluster
    subprocess.run([os.path.join(flink, "bin", "stop-cluster.sh")], check=True)


def run_flink_job(query, parallelism, num_records, max_runtime_per_job):
    # Start Flink cluster
    subprocess.run([os.path.join(flink, "bin", "start-cluster.sh")], check=True)
    # Start query
    print(f"Now running query {query} with {parallelism} threads.")
    subprocess.run(
        [os.path.join(flink, "bin", "flink"), "run", "--class", f"de.tub.nebulastream.benchmarks.flink.{query}",
         jar_path, "--parallelism", parallelism, "--numOfRecords", f"{num_records}", "-Xmx41456m", "--maxRuntime ",
         str(max_runtime_per_job), "--basePathForDataFiles", f"{local_data_folder}"])  # continue even if it fails
    # Stop Flink cluster
    subprocess.run([os.path.join(flink, "bin", "stop-cluster.sh")], check=True)


def analyze_logs(query_name, parallelism):
    # Find log file
    all_log_files = os.listdir(os.path.join(flink, "log"))
    matching_files = [f for f in all_log_files if
                      f.startswith("flink-") and "taskexecutor-" in f and f.endswith(".log")]
    if not len(matching_files) == 1:
        print(f"Log file not found.")
        exit(1)
    log_file = os.path.join(flink, "log", matching_files[0])

    # Calculate throughput
    subprocess.run(
        ["java", "-cp", jar_path, "de.tub.nebulastream.benchmarks.flink.utils.AnalyzeTool", log_file, query_name,
         parallelism], check=True)


def write_to_csv(query_name, num_records):
    input_path = os.path.join(f"{query_name}.csv")

    # Write results to csv
    with open(csv_path, "a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        with open(input_path) as input_file:
            csv_reader = csv.reader(input_file)
            for row in csv_reader:
                writer.writerow({
                    "query_name": query_name,  # same as row[0]
                    "parallelism": row[1],
                    "numOfRecords": num_records,
                    "tuplesPerSecond": row[2]
                })

    # Remove the input csv file
    os.remove(input_path)

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
        os.chdir('apache-flink-baseline')
        if not os.path.exists(flink):
            download_flink()

        download_data_sets(queries_to_run)

        if os.path.exists(csv_folder):
            shutil.rmtree(csv_folder)
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)

        # Prepare csv file
        with open(csv_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
            writer.writeheader()

        # Run Maven once
        env = os.environ.copy()
        # env["MAVEN_OPTS"] = "-Xss256k"
        subprocess.run(["mvn", "clean"], env=env, check=True)
        subprocess.run(["mvn", "package"], env=env, check=True)

        for query_name, query_class in queries_to_run.items():
            for parallelism in parallelisms_to_run:
                for num_records in num_of_records:
                    if "YSB" in query_name:
                        # If we go above 5M for YSB, we require more RAM than we have on the machine
                        num_records = 5 * 1000 * 1000

                    prepare()
                    run_flink_job(query_class, parallelism, num_records, MAX_RUNTIME_PER_JOB)
                    analyze_logs(query_name, parallelism)
                    write_to_csv(query_name, num_records)

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
