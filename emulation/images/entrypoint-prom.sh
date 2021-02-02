#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

mkdir -p /logs/$1

if [ $3 = "prometheus" ]; then
  echo "Starting prometheus daemon"
  /bin/prometheus-node-exporter --no-collector.arp --no-collector.bcache --no-collector.bonding --no-collector.conntrack \
  --no-collector.edac --no-collector.entropy --no-collector.filefd --no-collector.hwmon --no-collector.infiniband \
  --no-collector.ipvs --no-collector.mdadm --no-collector.nfs --no-collector.nfsd --no-collector.pressure \
  --no-collector.sockstat --no-collector.textfile --no-collector.time --no-collector.timex --no-collector.uname \
  --no-collector.vmstat --no-collector.xfs --no-collector.zfs --no-collector.systemd --no-collector.cpu \
  --no-collector.diskstats --no-collector.loadavg --no-collector.netclass --no-collector.netstat --no-collector.stat \
  > /logs/$1/$2_prometheus.log 2>&1 &
fi

if [ $4 = "crd" ]; then
  echo "Executing coordinator script:"
  sleep 5 && exec /entrypoint.sh ${@:5} > /logs/$1/$2_nes-runtime.log 2>&1 &
else
  echo "Executing worker script:"
  sleep 10 && exec /entrypoint.sh ${@:5} > /logs/$1/$2_nes-runtime.log 2>&1 &
fi
