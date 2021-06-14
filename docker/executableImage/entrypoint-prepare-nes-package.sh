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

[[ ! -f /nebulastream/CMakeLists.txt ]] && echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]" && exit 1

if [ $# -eq 0 ]
then
    mkdir -p /nebulastream/build
    cd /nebulastream/build
    cmake -DCMAKE_BUILD_TYPE=Release -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_USE_ADAPTIVE=0 ..
    make version
    make -j4
    cpack
    cd ..
    if test -f docker/executableImage/resources/*.deb; then
        rm docker/executableImage/resources/*.deb
    fi
    cp build/*.deb docker/executableImage/resources
    result=$?
    rm -rf /nebulastream/build
    exit $result
else
    exec $@
fi
