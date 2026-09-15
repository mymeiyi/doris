#!/usr/bin/env python3
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

"""Run with python3 test_metrics.py; no cluster is needed."""

from bench import cpu_at, percentile

samples = [dict(time=10, cpu_seconds=100), dict(time=12, cpu_seconds=106),
           dict(time=15, cpu_seconds=118)]
assert cpu_at(samples, 10) == 100
assert cpu_at(samples, 12) == 106
assert cpu_at(samples, 15) == 118
assert cpu_at(samples, 11) == 103
assert cpu_at(samples, 14) - cpu_at(samples, 11) == 11
for timestamp in [9, 16]:
    try:
        cpu_at(samples, timestamp)
    except ValueError:
        pass
    else:
        raise AssertionError('Out-of-range CPU interval accepted')
assert percentile([40, 10, 30, 20], .5) == 20
assert percentile([40, 10, 30, 20], .95) == 40
assert percentile([7], .99) == 7
print('Metric checks passed')
