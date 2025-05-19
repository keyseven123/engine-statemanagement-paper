#!/usr/bin/env bash

# name of the FIFOs
FIFO_PREFIX="perf_fd"

PID=$1
OUTPUT_FILE=$2

# TODO: Add check if args are passed

# remove dangling files if any
rm -rf ${FIFO_PREFIX}.*

# create two fifos
mkfifo ${FIFO_PREFIX}.ctl
mkfifo ${FIFO_PREFIX}.ack

# associate file descriptors
exec {perf_ctl_fd}<>${FIFO_PREFIX}.ctl
exec {perf_ack_fd}<>${FIFO_PREFIX}.ack

# set env vars for application
export PERF_CTL_FD=${perf_ctl_fd}
export PERF_ACK_FD=${perf_ack_fd}

# start perf with the associated file descriptors
perf record -p "${PID}" \
  --delay=-1 \
  --control fd:${perf_ctl_fd},${perf_ack_fd} \
  -o "${OUTPUT_FILE}"

# TODO: Maybe add a return code so we know if everything works
