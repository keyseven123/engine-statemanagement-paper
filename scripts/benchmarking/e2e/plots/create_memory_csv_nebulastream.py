#!/usr/bin/env python3
import re
import numpy as np
import csv

# Constants
PREALLOCATED_BUFFERS = 50_000
BUFFER_SIZE_KB = 256  # 256KB per buffer

# Parse memory_log.txt
memory_times = []
memory_values_kb = []

with open('memory_log.txt', 'r') as f:
    for line in f:
        match = re.search(r'(\d+\.\d+)\s+VmRSS:\s+(\d+)\s+kB', line)
        if match:
            timestamp = float(match.group(1))
            value_kb = int(match.group(2))
            memory_times.append(timestamp)
            memory_values_kb.append(value_kb)

# Parse buffer_usage.csv
buffer_times = []
buffer_values = []

with open('buffer_usage.csv', 'r') as f:
    next(f)  # Skip header
    for line in f:
        parts = line.strip().split(',')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            timestamp_ms = int(parts[0])
            used_buffers = int(parts[1])
            buffer_times.append(timestamp_ms / 1000.0)  # Convert to seconds
            buffer_values.append(used_buffers)

# Normalize each dataset independently to start from 0
memory_times_normalized = [t - memory_times[0] for t in memory_times]
buffer_times_normalized = [t - buffer_times[0] for t in buffer_times]

# Calculate baseline RSS (first few samples before activity)
baseline_rss_kb = np.mean(memory_values_kb[:10])
preallocated_kb = PREALLOCATED_BUFFERS * BUFFER_SIZE_KB

# Interpolate buffer usage to memory timestamps for adjustment
buffer_usage_interp = np.interp(
    memory_times_normalized,
    buffer_times_normalized,
    buffer_values,
    left=0,  # Use 0 for times before buffer data starts
    right=buffer_values[-1]  # Use last buffer value for times after buffer data ends
)

# Adjusted RSS = (Raw RSS - baseline) + (used_buffers * BUFFER_SIZE_KB)
# This shows: other memory growth + actual buffer usage
adjusted_rss_mb = []
for rss_kb, used_bufs in zip(memory_values_kb, buffer_usage_interp):
    adjusted = (rss_kb - baseline_rss_kb) + (used_bufs * BUFFER_SIZE_KB)
    adjusted_rss_mb.append(adjusted / 1024)  # Convert to MB

# Also compute used buffer memory in MB for comparison
used_buffer_mb = [b * BUFFER_SIZE_KB / 1024 for b in buffer_usage_interp]

# Raw RSS in MB
raw_rss_mb = [kb / 1024 for kb in memory_values_kb]

# Write data to CSV
with open('nebulastream_e2e_memory_usage.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['time_s', 'raw_rss_mb', 'adjusted_rss_mb', 'used_buffer_mb'])

    for time, raw, adjusted, used in zip(memory_times_normalized, raw_rss_mb, adjusted_rss_mb, used_buffer_mb):
        writer.writerow([time, raw, adjusted, used])

print(f"Data saved to memory_data.csv")
print(f"Baseline RSS: {baseline_rss_kb/1024:.1f} MB")
print(f"Preallocated buffer pool: {preallocated_kb/1024:.1f} MB ({PREALLOCATED_BUFFERS} × {BUFFER_SIZE_KB}KB)")
print(f"Adjusted RSS range: {min(adjusted_rss_mb):.1f} MB - {max(adjusted_rss_mb):.1f} MB")
print(f"Max used buffer memory: {max(used_buffer_mb):.1f} MB")
print(f"Total rows written: {len(memory_times_normalized)}")
print(f"Memory time range: 0.0s - {max(memory_times_normalized):.1f}s")
print(f"Buffer time range: 0.0s - {max(buffer_times_normalized):.1f}s")