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

"""Run the requested packed-file control pair, restoring runtime BE configuration."""

import argparse
import json
import pathlib
import subprocess
import time
import urllib.parse
import urllib.request

from bench import HOSTS


def get_config(host):
    with urllib.request.urlopen(f'http://{host}:8040/api/show_config', timeout=10) as response:
        rows = json.load(response)
    return next(row[2] for row in rows if row[0] == 'enable_packed_file')


def set_config(host, value):
    query = urllib.parse.urlencode({'enable_packed_file': value, 'persist': 'false'})
    request = urllib.request.Request(f'http://{host}:8040/api/update_config?{query}', data=b'',
                                     headers={'Authorization': 'Basic cm9vdDo='})
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    assert len(result) == 1 and result[0]['config_name'] == 'enable_packed_file', result
    assert result[0]['status'] == 'OK', result
    assert get_config(host) == value


def wait_for_compaction():
    quiet = 0
    while quiet < 3:
        active = {}
        for host in HOSTS:
            with urllib.request.urlopen(f'http://{host}:8040/api/compaction/run_status', timeout=10) as r:
                status = json.load(r)
            active[host] = len(status['CumulativeCompaction']) + len(status['BaseCompaction'])
        quiet = quiet + 1 if sum(active.values()) == 0 else 0
        print('Waiting for compaction', active, 'quiet samples', quiet, flush=True)
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--concurrency', type=int, default=48)
    parser.add_argument('--on-only', action='store_true')
    parser.add_argument('--compare-packed', action='store_true',
                        help='Keep forwarding enabled and compare packed file OFF/ON')
    parser.add_argument('--data', default='data.jsonl')
    args = parser.parse_args()
    assert args.concurrency > 0 and args.concurrency % 3 == 0
    assert not (args.compare_packed and args.on_only)
    suffix = f'_{args.concurrency}' if args.concurrency != 48 else ''
    config_prefix = 'packed_switch' if args.compare_packed else 'packed'
    original = {host: get_config(host) for host in HOSTS}
    snapshot = pathlib.Path(f'{config_prefix}_config_before{suffix}.json')
    assert not snapshot.exists(), 'Keep previous snapshot intact; use a new experiment directory'
    snapshot.write_text(json.dumps(original, indent=2))
    try:
        wait_for_compaction()
        for host in HOSTS:
            set_config(host, 'false')
        print('All BEs: enable_packed_file=false', flush=True)
        rounds = [('packed_off_mem_off', 'false'), ('packed_off_mem_on', 'true')]
        if args.compare_packed:
            rounds = [('packed_switch_off', 'true'), ('packed_switch_on', 'true')]
        if args.on_only:
            rounds = rounds[1:]
        for name, enabled in rounds:
            name += suffix
            wait_for_compaction()
            if args.compare_packed:
                packed = 'true' if name.startswith('packed_switch_on') else 'false'
                for host in HOSTS:
                    set_config(host, packed)
                print(name, 'enable_packed_file=' + packed, flush=True)
            with open(name + '.log', 'w') as output:
                subprocess.run(['python3', 'bench.py', '--name', name, '--enabled', enabled,
                                '--count', '7500', '--concurrency', str(args.concurrency),
                                '--data', args.data], stdout=output,
                               stderr=subprocess.STDOUT, check=True)
            print('Finished', name, flush=True)
    finally:
        # Try every host even when one restoration fails; report all failures.
        errors = []
        for host, value in original.items():
            try:
                set_config(host, value)
            except Exception as error:
                errors.append(f'{host}: {error}')
        assert not errors, errors
        restored = {host: get_config(host) for host in HOSTS}
        pathlib.Path(f'{config_prefix}_config_restored{suffix}.json').write_text(json.dumps(restored, indent=2))
        assert restored == original
        print('Restored packed file configuration', restored, flush=True)


if __name__ == '__main__':
    main()
