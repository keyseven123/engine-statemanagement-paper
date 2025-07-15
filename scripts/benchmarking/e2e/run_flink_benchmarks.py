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

import os
import shutil
import subprocess
import argparse
import csv
import requests
import concurrent.futures

from urllib.request import urlretrieve


parser = argparse.ArgumentParser(description="Script for running Flink benchmarks")
parser.add_argument("--flink_version", default="2.0.0", help="Flink version to download (default: 2.0.0)")
args = parser.parse_args()

flink = "flink-2.0.0"
jar_path = os.path.join("target", "bench-flink_2.0-0.1-SNAPSHOT.jar")

queries = {
    "CM1": "clustermonitoring.CM1",
    "CM2": "clustermonitoring.CM2",
    "LRB1": "linearroad.LR1",
    "LRB2": "linearroad.LR2",
    "MA": "manufacturingequipment.ME1",
    "SG1": "smartgrid.SG1",
    "SG2": "smartgrid.SG2",
    "SG3": "smartgrid.SG3",
    "YSB1k": "ysb.YSB1k",
    "YSB10k": "ysb.YSB10k",
    # "multiquery_ysb": "multiquery.ysb.YSB",
    "NM1": "nexmark.NE1",
    "NM2": "nexmark.NE2",
    "NM5": "nexmark.NE5",
    "NM8": "nexmark.NE8",
    "NM8_Variant": "nexmark.NE8_Variant"
}

needed_data_sets = {
    "CM1": ["https://bench.nebula.stream/data/smartgrid/smartgrid-data_974K.csv"],
    "CM2": ["https://bench.nebula.stream/data/smartgrid/smartgrid-data_974K.csv"],
    "LRB1": ["https://bench.nebula.stream/data/lrb/linear_road_benchmark_560K.csv"],
    "LRB2": ["https://bench.nebula.stream/data/lrb/linear_road_benchmark_560K.csv"],
    "MA": ["https://bench.nebula.stream/data/manufacturing/manufacturing_1G.csv"],
    "SG1": ["https://bench.nebula.stream/data/smartgrid/smartgrid-data_974K.csv"],
    "SG2": ["https://bench.nebula.stream/data/smartgrid/smartgrid-data_974K.csv"],
    "SG3": ["https://bench.nebula.stream/data/smartgrid/smartgrid-data_974K.csv"],
    "YSB1k": ["https://bench.nebula.stream/data/ysb/ysb1k_more_data_3GB.csv"],
    "YSB10k": ["https://bench.nebula.stream/data/ysb/ysb10k_more_data_3GB.csv"],
    "NM1-5": ["https://bench.nebula.stream/data/nexmark/bid_more_data_6GB.csv"],
    "NM8": ["https://bench.nebula.stream/data/nexmark/auction_more_data_707MB.csv",
            "https://bench.nebula.stream/data/nexmark/person_more_data_840MB.csv"]
}


num_of_records = [50 * 1000 * 1000]  # [10000, 1000000, 10000000]
parallelisms = ["1", "2", "4", "8", "16"] #["1", "4"]

local_data_folder = "/tmp/data"
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
                    print(f"\rDownloading {os.path.basename(local_path)}: |{bar}{spaces}| {progress * 100:.2f}%", end='')
            print(f"\nDownloaded {url} to {local_path}")
        else:
            print(f"Failed to download {url}")
    except Exception as e:
        print(f"An error occurred while downloading {url}: {e}")
        exit(1)

def download_data_sets():
    """Download the data sets if they are not already present in the local folder."""
    if not os.path.exists(local_data_folder):
        os.makedirs(local_data_folder)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for dataset_name, urls in needed_data_sets.items():
            for url in urls:
                file_name = os.path.basename(url)
                local_file_path = os.path.join(local_data_folder, file_name)
                if not os.path.isfile(local_file_path):
                    print(f"File {file_name} not found. Scheduling download...")
                    future = executor.submit(download_file_with_progress, url, local_file_path)
                    futures.append(future)
                else:
                    print(f"File {file_name} already exists at {local_file_path}")

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

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


def run_flink_job(query, parallelism, num_records):
    # Start Flink cluster
    subprocess.run([os.path.join(flink, "bin", "start-cluster.sh")], check=True)
    # Start query
    print(f"Now running query {query} with {parallelism} threads.")
    subprocess.run(
        [os.path.join(flink, "bin", "flink"), "run", "--class", f"de.tub.nebulastream.benchmarks.flink.{query}",
         jar_path, "--parallelism", parallelism, "--numOfRecords", f"{num_records}", "-Xmx32768m"])  # continue even if it fails
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


def check_repository_root():
    """Check if the script is being run from the repository root."""
    expected_dirs = ["nes-sources", "nes-sql-parser", "nes-systests"]
    current_dir = os.getcwd()
    contents = os.listdir(current_dir)

    if not all(expected_dir in contents for expected_dir in expected_dirs):
        raise RuntimeError("The script is not being run from the repository root.")

def main():
    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Store the current working directory
    original_dir = os.getcwd()


    try:
        os.chdir('apache-flink-baseline')
        global flink
        flink = f"flink-{args.flink_version}"

        if not os.path.exists(flink):
            download_flink()

        download_data_sets()

        if os.path.exists(csv_folder):
            shutil.rmtree(csv_folder)
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)

        # Prepare csv file
        with open(csv_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
            writer.writeheader()

        # Run Maven once
        subprocess.run(["mvn", "clean"], check=True)
        subprocess.run(["mvn", "package"], check=True)

        for query_name, query_class in queries.items():
            for parallelism in parallelisms:
                for num_records in num_of_records:
                    if "YSB" in query_name:
                        # If we go above 10M for YSB, we require more RAM than we have
                        num_records = 10 * 1000 * 1000

                    prepare()
                    run_flink_job(query_class, parallelism, num_records)
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



if __name__ == "__main__":
    main()
