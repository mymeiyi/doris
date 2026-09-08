#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

# Start with an independent diagnostic load. Never include its timing in A/B results.
set -euo pipefail
profile_dir=$1
profile_start_epoch=$2
mkdir -p "$profile_dir"
cd "$profile_dir"
be_pid=$(pgrep -x doris_be)
while (( $(date +%s) < profile_start_epoch )); do sleep 1; done
date -Ins > start.txt
ps -L -p "$be_pid" -o tid,psr,comm > threads_before.txt
top -b -H -1 -p "$be_pid" -d 1 -n 31 -w 220 > top_threads.txt &
top_pid=$!
pidstat -t -u -p "$be_pid" 1 30 > pidstat_threads.txt &
pidstat_pid=$!
# System-wide recording avoids races while attaching to transient BE threads.
# Only this BE process is exported below.
perf record -e cpu-clock -a -F 99 -m 64 --call-graph fp -o perf.data -- sleep 30 > perf_record.log 2>&1
date -Ins > end.txt
wait "$top_pid" "$pidstat_pid"
perf script -i perf.data --pid "$be_pid" > perf.script 2> perf_script.log
/root/FlameGraph/stackcollapse-perf.pl perf.script > stacks.folded
/root/FlameGraph/flamegraph.pl --countname nanoseconds --title "${profile_dir##*/} - 48 concurrency" stacks.folded > flamegraph.svg
