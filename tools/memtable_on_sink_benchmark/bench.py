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

"""Run balanced Stream Load against three BEs; retain every response and CPU sample.

Run on the benchmark host, with passwordless root SSH to each BE and mysql in PATH.
Tables and data are retained. HTTP requests are never retried or redirected.
"""

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import math
import pathlib
import re
import shlex
import statistics
import subprocess
import threading
import time

HOSTS = ['172.20.49.5', '172.20.49.6', '172.20.49.7']
DB = 'memtable_ab_20260908'


def sql(statement):
    return subprocess.check_output(
        ['mysql', '-h172.20.49.1', '-P9030', '-uroot', '--batch', '--raw',
         '--skip-column-names', '-e', statement], text=True)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def cpu_at(samples, timestamp):
    for left, right in zip(samples, samples[1:]):
        if left['time'] <= timestamp <= right['time']:
            fraction = (timestamp - left['time']) / (right['time'] - left['time'])
            return left['cpu_seconds'] + fraction * (right['cpu_seconds'] - left['cpu_seconds'])
    raise ValueError('CPU samples do not cover the measured interval')


def run_loads(table, payload, count, concurrency, enabled, profile, prefix, output):
    assert count % 3 == concurrency % 3 == 0
    barrier = threading.Barrier(concurrency + 1)
    stop = threading.Event()
    per_host = count // 3
    workers_per_host = concurrency // 3
    expected_rows = payload.count(b'\n')

    def worker(host, ordinal):
        connection = http.client.HTTPConnection(host, 8040, timeout=300)
        records = []
        barrier.wait()
        try:
            for index in range(ordinal, per_host, workers_per_host):
                if stop.is_set():
                    break
                label = f'{prefix}_{host.split(".")[-1]}_{index}'
                start = time.time()
                timer = time.monotonic()
                record = dict(host=host, label=label, start=start)
                try:
                    connection.request('PUT', f'/api/{DB}/{table}/_stream_load', payload, {
                        'Authorization': 'Basic cm9vdDo=', 'format': 'json',
                        'read_json_by_line': 'true', 'group_commit': 'off_mode',
                        'load_to_single_tablet': 'true',
                        'memtable_on_sink_node': str(enabled).lower(),
                        'enable_profile': str(profile).lower(), 'label': label,
                        'max_filter_ratio': '0', 'strict_mode': 'true', 'timeout': '300',
                    })
                    response = connection.getresponse()
                    body = response.read().decode()
                    record.update(http_status=response.status, body=body)
                    result = json.loads(body)
                    record['response'] = result
                    assert response.status == 200, record
                    assert result['Status'] == 'Success', record
                    assert result['NumberLoadedRows'] == expected_rows, record
                    assert result['NumberFilteredRows'] == 0, record
                except Exception as error:
                    record['error'] = str(error)
                    stop.set()
                record.update(end=time.time(), elapsed_ms=(time.monotonic() - timer) * 1000)
                records.append(record)
        finally:
            connection.close()
        return records

    records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, host, ordinal)
                   for host in HOSTS for ordinal in range(workers_per_host)]
        barrier.wait()
        with output.open('w') as stream:
            for future in concurrent.futures.as_completed(futures):
                batch = future.result()
                records.extend(batch)
                for record in batch:
                    stream.write(json.dumps(record) + '\n')
                stream.flush()
                print(f'{prefix}: {len(records)}/{count} responses', flush=True)
    assert len(records) == count and not stop.is_set(), f'Failed round: see {output}'
    return records


# One independent process per host. CPU is expressed as occupied cores, not percent.
MONITOR = r'''
import json, os, pathlib, subprocess, sys, time, urllib.request
pid = int(subprocess.check_output(['pgrep', '-x', 'doris_be']))
clock = os.sysconf('SC_CLK_TCK')
previous = None
while True:
    started = time.monotonic()
    fields = pathlib.Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    stat = pathlib.Path('/proc/stat').read_text().splitlines()[0].split()[1:]
    network = pathlib.Path('/proc/net/dev').read_text()
    sample = dict(time=time.time(), pid=pid, cpu_seconds=(int(fields[11])+int(fields[12]))/clock,
                  rss_bytes=int(fields[21])*os.sysconf('SC_PAGE_SIZE'),
                  host_cpu_ticks=list(map(int, stat)), network=network)
    if previous is not None:
        sample['cpu_cores'] = (sample['cpu_seconds']-previous['cpu_seconds'])/(sample['time']-previous['time'])
    with urllib.request.urlopen('http://127.0.0.1:8040/metrics', timeout=3) as response:
        sample['metrics'] = '\n'.join(line for line in response.read().decode().splitlines()
            if not line.startswith('#') and any(word in line for word in
               ('compaction', 's3_', 'memtable', 'flush', 'load_channel', 'load_stream',
                'thread_pool_', 'queue_size', 'streaming_load_current_processing')))
    sample['bvars'] = {}
    for pattern in ['load_stream*', '*s3*', '*packed*']:
        with urllib.request.urlopen(f'http://127.0.0.1:8060/vars/{pattern}?console=1', timeout=3) as response:
            sample['bvars'][pattern] = response.read().decode()
    print(json.dumps(sample), flush=True)
    previous = sample
    time.sleep(max(0, 1-(time.monotonic()-started)))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', required=True)
    parser.add_argument('--enabled', choices=['true', 'false'], required=True)
    parser.add_argument('--count', type=int, default=7500)
    parser.add_argument('--concurrency', type=int, default=48)
    parser.add_argument('--profile', action='store_true')
    parser.add_argument('--receiver', default='172.20.49.7')
    parser.add_argument('--data', type=pathlib.Path, default=pathlib.Path('data.jsonl'))
    args = parser.parse_args()
    assert re.fullmatch('[a-z][a-z0-9_]*', args.name)
    assert args.count > 0 and args.concurrency > 0
    assert args.count % 3 == args.concurrency % 3 == 0
    folder = pathlib.Path(args.name)
    folder.mkdir()
    payload = args.data.read_bytes()
    rows = [json.loads(line) for line in payload.splitlines()]
    assert rows and payload.endswith(b'\n')
    assert all(row['_ctime_'].startswith('2026-09-08 10:') for row in rows)
    enabled = args.enabled == 'true'
    backends = [line.split('\t') for line in sql('SHOW BACKENDS').splitlines()]
    sql(f'CREATE DATABASE IF NOT EXISTS {DB}')
    for attempt in range(20):
        table = f'ali_virginia_prod_06_{args.name}_{attempt}'
        ddl = pathlib.Path('table.sql').read_text().replace('TABLE_NAME', f'{DB}.{table}')
        sql(ddl)
        tablets = sql(f'SHOW TABLETS FROM {DB}.{table}')
        assert len(tablets.strip().splitlines()) == 1, tablets
        backend_id = tablets.split('\t')[2]
        receiver = next(row[1] for row in backends if row[0] == backend_id)
        if receiver == args.receiver:
            break
    assert receiver == args.receiver, 'Could not allocate tablet on requested receiver'
    (folder / 'table.tsv').write_text(sql(f'SHOW CREATE TABLE {DB}.{table}'))
    prefix = args.name + '_' + str(time.time_ns())
    warmup = 3 if args.profile else 30
    run_loads(table, payload, warmup, 3, enabled, args.profile, prefix + '_warm',
              folder / 'warmup.jsonl')
    tablets = sql(f'SHOW TABLETS FROM {DB}.{table}')
    assert len(tablets.strip().splitlines()) == 1, tablets
    (folder / 'tablets_before.tsv').write_text(tablets)
    monitors = []
    try:
        for host in HOSTS:
            stream = (folder / f'monitor_{host}.jsonl').open('w')
            process = subprocess.Popen(['ssh', '-oBatchMode=yes', '-oConnectTimeout=5',
                                        f'root@{host}', 'python3 -u -c ' + shlex.quote(MONITOR)],
                                       stdout=stream, stderr=(folder / f'monitor_{host}.err').open('w'))
            monitors.append((host, process, stream))
        time.sleep(5)
        assert all(process.poll() is None for _, process, _ in monitors)
        cpu_before = time.process_time()
        records = run_loads(table, payload, args.count, args.concurrency, enabled,
                            args.profile, prefix, folder / 'requests.jsonl')
        client_cpu = time.process_time() - cpu_before
        time.sleep(2)
        assert all(process.poll() is None for _, process, _ in monitors)
    finally:
        for _, process, stream in monitors:
            process.terminate()
            process.wait(timeout=10)
            stream.close()
    start, end = min(r['start'] for r in records), max(r['end'] for r in records)
    elapsed = [r['elapsed_ms'] for r in records]
    summary = dict(name=args.name, table=f'{DB}.{table}', enabled=enabled,
                   concurrency=args.concurrency, receiver=receiver,
                   count=args.count, warmup=warmup, rows_per_load=len(rows), bytes_per_load=len(payload),
                   sha256=hashlib.sha256(payload).hexdigest(), start=start, end=end,
                   wall_seconds=end-start, requests_per_second=args.count/(end-start),
                   mib_per_second=args.count*len(payload)/(end-start)/2**20,
                   mean_ms=statistics.mean(elapsed), p50_ms=percentile(elapsed, .5),
                   p95_ms=percentile(elapsed, .95), p99_ms=percentile(elapsed, .99),
                   max_ms=max(elapsed), client_cpu_seconds=client_cpu, be={})
    for host in HOSTS:
        samples = [json.loads(line) for line in (folder / f'monitor_{host}.jsonl').read_text().splitlines()]
        cpu_seconds = cpu_at(samples, end) - cpu_at(samples, start)
        samples = [s for s in samples if start <= s['time'] <= end and 'cpu_cores' in s]
        assert samples, f'No CPU samples: {host}'
        values = [s['cpu_cores'] for s in samples]
        summary['be'][host] = dict(mean_cores=cpu_seconds/(end-start), cpu_seconds=cpu_seconds,
                                   p95_cores=percentile(values, .95),
                                   peak_rss_bytes=max(s['rss_bytes'] for s in samples),
                                   requests=sum(r['host'] == host for r in records))
    summary['server_mean_ms'] = {key: statistics.mean(r['response'][key] for r in records)
                                 for key in records[0]['response'] if key.endswith('TimeMs')}
    totals = sql(f'SET enable_sql_cache=false; SELECT COUNT(*), SUM(LENGTH(msg)), '
                 f'MIN(_ctime_), MAX(_ctime_) FROM {DB}.{table}')
    (folder / 'validation.tsv').write_text(totals)
    assert int(totals.split('\t')[0]) == len(rows)*(args.count+warmup), totals
    expected_msg_bytes = sum(len(row['msg'].encode()) for row in rows if row['msg'] is not None)
    assert int(totals.split('\t')[1]) == expected_msg_bytes*(args.count+warmup), totals
    (folder / 'tablets_after.tsv').write_text(sql(f'SHOW TABLETS FROM {DB}.{table}'))
    (folder / 'partitions_after.tsv').write_text(sql(f'SHOW PARTITIONS FROM {DB}.{table}'))
    (folder / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
