#!/usr/bin/env bash

SYSTEST_LIST=$(find ../nes-systests/benchmark -type f -name "*.test" | sort)

echo "$SYSTEST_LIST"

for SYSTEST in $SYSTEST_LIST; do
  echo "Profiling $SYSTEST"
  echo "Saving data to $SYSTEST.perf.data"
  perf record -o "$SYSTEST".perf.data /home/klaas/CLionProjects/nebulastream-public/cmake-build-debug-nes/nes-systests/systest/systest -t "$SYSTEST"
  perf script -i "$SYSTEST.perf.data" > "$SYSTEST.perf"
done