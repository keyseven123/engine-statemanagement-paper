#!/usr/bin/env python3
import re
import numpy as np
import matplotlib.pyplot as plt

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
            buffer_times.append(timestamp_ms / 1000.0)
            buffer_values.append(used_buffers)

# Convert to relative time, starting from when both datasets are available
start_time = max(memory_times[0], buffer_times[0])
end_time = min(memory_times[-1], buffer_times[-1])
memory_times_rel = [t - start_time for t in memory_times]
buffer_times_rel = [t - start_time for t in buffer_times]

# Calculate baseline RSS (first few samples before activity)
baseline_rss_kb = np.mean(memory_values_kb[:10])
preallocated_kb = PREALLOCATED_BUFFERS * BUFFER_SIZE_KB

# Interpolate buffer usage to memory timestamps for adjustment
buffer_usage_interp = np.interp(memory_times_rel, buffer_times_rel, buffer_values)

# Adjusted RSS = (Raw RSS - baseline) + (used_buffers * 4KB)
# This shows: other memory growth + actual buffer usage
adjusted_rss_mb = []
for rss_kb, used_bufs in zip(memory_values_kb, buffer_usage_interp):
    adjusted = (rss_kb - baseline_rss_kb) + (used_bufs * BUFFER_SIZE_KB)
    adjusted_rss_mb.append(adjusted / 1024)  # Convert to MB

# Also compute used buffer memory in MB for comparison
used_buffer_mb = [b * BUFFER_SIZE_KB / 1024 for b in buffer_values]

# Raw RSS in MB
raw_rss_mb = [kb / 1024 for kb in memory_values_kb]

# Create the plot
fig, ax1 = plt.subplots(figsize=(12, 6))

color1 = '#2563eb'
ax1.set_xlabel('Time (seconds)')
ax1.set_ylabel('Memory (MB)', color=color1)
ax1.plot(memory_times_rel, raw_rss_mb, color='#94a3b8', linewidth=2, label='Raw RSS')
ax1.plot(memory_times_rel, adjusted_rss_mb, color=color1, linewidth=2, label='Adjusted RSS')
ax1.plot(buffer_times_rel, used_buffer_mb, color='#10b981', linewidth=1.5, linestyle='--', label='Used Buffer Memory')
ax1.tick_params(axis='y', labelcolor=color1)
ax1.set_ylim(bottom=0)
ax1.set_xlim(0, end_time - start_time)
ax1.grid(True, alpha=0.3)
ax1.legend(loc='upper left')

plt.title(f'Memory Usage (baseline: {baseline_rss_kb/1024:.0f}MB, buffer pool: {preallocated_kb/1024:.0f}MB)')
plt.tight_layout()
plt.savefig('memory_chart.png', dpi=150)

print(f"Chart saved to memory_chart.png")
print(f"Baseline RSS: {baseline_rss_kb/1024:.1f} MB")
print(f"Preallocated buffer pool: {preallocated_kb/1024:.1f} MB ({PREALLOCATED_BUFFERS} × {BUFFER_SIZE_KB}KB)")
print(f"Adjusted RSS range: {min(adjusted_rss_mb):.1f} MB - {max(adjusted_rss_mb):.1f} MB")
print(f"Max used buffer memory: {max(used_buffer_mb):.1f} MB")
