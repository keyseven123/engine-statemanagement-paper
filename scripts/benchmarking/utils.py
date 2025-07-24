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
import socket



#### General Util Methods
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


def check_repository_root():
    """Check if the script is being run from the repository root."""
    expected_dirs = ["nes-sources", "nes-sql-parser", "nes-systests"]
    current_dir = os.getcwd()
    contents = os.listdir(current_dir)

    if not all(expected_dir in contents for expected_dir in expected_dirs):
        raise RuntimeError("The script is not being run from the repository root.")



#### NebulaStream specific
def get_vcpkg_dir():
    # Get the hostname
    hostname = socket.gethostname()

    # Determine the vcpkg directory based on the hostname
    if hostname == "nils-ThinkStation-P3-Tower":
        vcpkg_dir = "/home/nils/remote_server/vcpkg/scripts/buildsystems/vcpkg.cmake"
    elif hostname == "hare":
        vcpkg_dir = "/data/vcpkg/scripts/buildsystems/vcpkg.cmake"
    elif hostname == "mif-ws":
        vcpkg_dir = "/home/nschubert/remote_server/vcpkg/scripts/buildsystems/vcpkg.cmake"
    else:
        raise ValueError(f"Unknown hostname: {hostname}. Cannot determine vcpkg directory.")

    return vcpkg_dir

def run_command(command, cwd=None):
    result = subprocess.run(command, cwd=cwd, shell=True, check=True, text=True, capture_output=True)
    return result.stdout

def convert_unit_prefix(value, unit_prefix):
    # Convert throughput based on the unit prefix
    if unit_prefix == 'M':
        return value * 1e6  # Convert to actual value (million)
    elif unit_prefix == 'B':
        return value * 1e9  # Convert to actual value (billion)
    elif unit_prefix == 'k':
        return value * 1e3  # Convert to actual value (thousand)
    elif unit_prefix == '':
        return value  # No conversion needed
    else:
        raise ValueError(f"Could not convert {value} for {unit_prefix}")

def compile_nebulastream(cmake_flags, build_dir):
    cmake_command = f"cmake {cmake_flags} -S . -B {build_dir}"
    build_command = f"cmake --build {build_dir}"

    print("Running cmake...")
    run_command(cmake_command)
    print("Building the project...")
    run_command(build_command)