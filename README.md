# General
This repo corresponds to the paper "NebulaStream: An Adaptive and Efficient Multi-query Stream Processing Engine" by Schubert et al.


# Building and running Experiments from the Paper
The scripts for running the experiments are under `scripts/benchmarking`.
The general structure is that there is a bash script that calls a Python script. 
The Python script creates a CSV file, which can then be plotted with the Jupyter notebooks under `plots`.

# Dependencies
It might be required to install some dependencies found in `docker/dependency/Base.dockerfile`