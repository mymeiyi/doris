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

"""Generate a report from completed benchmark artifacts (requires matplotlib)."""

import csv
import json
import pathlib
import statistics
import sys

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

from bench import HOSTS, cpu_at


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def main():
    root = pathlib.Path(sys.argv[1])
    packed_control = '--packed' in sys.argv[2:]
    suffix = '_packed' if packed_control else ''
    names = (['packed_off_mem_off', 'packed_off_mem_on'] if packed_control
             else ['off_1', 'on_1', 'on_2', 'off_2'])
    summaries = [json.loads((root / name / 'summary.json').read_text()) for name in names]
    assert all(s['count'] == 7500 and s['concurrency'] == 48 for s in summaries)
    assert len({s['sha256'] for s in summaries}) == 1
    assert all(s['receiver'] == '172.20.49.7' for s in summaries)
    cpu_rows = []
    fig, axes = plt.subplots(len(names)//2, 2, figsize=(13, 3.5*(len(names)//2)), sharey=True)
    for summary, axis in zip(summaries, axes.flat):
        name = summary['name']
        for host in HOSTS:
            samples = read_lines(root / name / f'monitor_{host}.jsonl')
            seconds = cpu_at(samples, summary['end']) - cpu_at(samples, summary['start'])
            mean = seconds / summary['wall_seconds']
            summary['be'][host]['mean_cores'] = mean
            summary['be'][host]['cpu_seconds'] = seconds
            samples = [s for s in samples if summary['start'] <= s['time'] <= summary['end']
                       and 'cpu_cores' in s]
            axis.plot([s['time'] - summary['start'] for s in samples],
                      [s['cpu_cores'] for s in samples], label=host, linewidth=.8, alpha=.85)
            cpu_rows.append(dict(round=name, host=host, mean_cores=mean, cpu_seconds=seconds,
                                 share=seconds / sum(v['cpu_seconds'] for v in summary['be'].values())))
        axis.set(title=f'{name}: {summary["wall_seconds"]:.1f}s', xlabel='Seconds from first request',
                 ylabel='BE CPU cores (16 available)', ylim=(0, 17))
        axis.grid(alpha=.2)
        axis.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(root / f'cpu{suffix}.png', dpi=160)
    fig.savefig(root / f'cpu{suffix}.svg')
    plt.close(fig)

    columns = ['name', 'enabled', 'concurrency', 'count', 'wall_seconds', 'requests_per_second',
               'mib_per_second', 'mean_ms', 'p50_ms', 'p95_ms', 'p99_ms', 'max_ms', 'client_cpu_seconds']
    with (root / f'comparison{suffix}.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(summaries)
    with (root / f'cpu{suffix}.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(cpu_rows[0]))
        writer.writeheader()
        writer.writerows(cpu_rows)
    off = [s for s in summaries if not s['enabled']]
    on = [s for s in summaries if s['enabled']]
    off_time = statistics.mean(s['wall_seconds'] for s in off)
    on_time = statistics.mean(s['wall_seconds'] for s in on)
    lines = ['# Cloud memtable 前移 Stream Load 对照测试', '',
             ('本组两边均关闭 packed file；结束后恢复三个 BE 原配置。' if packed_control
              else '本组保留集群原有 packed file 配置。'), '',
             f'关闭组平均总耗时 **{off_time:.2f} 秒**，开启组 **{on_time:.2f} 秒**。'
             f'开启后的耗时变化为 **{(on_time/off_time-1)*100:+.1f}%**。', '',
             ('本补充组每种状态仅一轮，需结合原始四轮结果看待波动。' if packed_control
              else '关闭组耗时为 ' + '/'.join(f'{s["wall_seconds"]:.2f}' for s in off)
              + ' 秒；开启组为 ' + '/'.join(f'{s["wall_seconds"]:.2f}' for s in on)
              + ' 秒。CPU 分布与吞吐需分别判断，不能仅依赖单轮结果。'), '',
             '## 测试条件', '',
             '- 2026-09-08；BE 172.20.49.5/.6/.7，16 核、约 61 GiB/台。',
             '- BE 版本 4306e8f7bf4；FE 报告版本 b6c0a841062。',
             '- 发压机 .6；各轮的单 tablet 接收端均为 .7。',
             '- 总并发 48，每 BE 16；每轮 7500 次，每 BE 2500 次；顺序 ' + '/'.join(names) + '。',
             f'- 每次 10,000 行、{summaries[0]["bytes_per_load"]:,} 字节 JSONL；每轮 7500 万行。',
             '- 样本来自现有日志 backup 表，统一时间到同一小时；不是新采集的生产流量。',
             f'- 样本 SHA-256：`{summaries[0]["sha256"]}`。',
             '- 保留 DUPLICATE KEY、RANDOM BUCKETS 1、全部 V2 倒排索引、自动 compaction、file cache。',
             '- 当前 Cloud 版本拒绝 enable_single_replica_compaction，该属性从两组 DDL 中同时移除。',
             '- 两组均设置 group_commit:off_mode、load_to_single_tablet:true；仅 memtable_on_sink_node 不同。',
             '- 每轮独立空表，先预建小时分区并预热 30 次；预热和最终校验不计入导入耗时。',
             '- 不清空 file cache；后台 compaction 保持原配置并随负载运行，后续轮次开始前等待已有合并完成。',
             '- HTTP 请求无重试、无重定向；正式轮次关闭 profile。', '',
             '## 导入耗时', '',
             '| 轮次 | 总耗时(s) | 次/s | MiB/s | 平均(ms) | P50 | P95 | P99 | 最大(ms) |',
             '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for s in summaries:
        lines.append(f'| {s["name"]} | {s["wall_seconds"]:.2f} | {s["requests_per_second"]:.2f} | '
                     f'{s["mib_per_second"]:.2f} | {s["mean_ms"]:.1f} | {s["p50_ms"]:.1f} | '
                     f'{s["p95_ms"]:.1f} | {s["p99_ms"]:.1f} | {s["max_ms"]:.1f} |')
    lines += ['', '总耗时为最早请求开始到最后请求完成的墙钟时间。延迟为客户端单次完整 HTTP 请求耗时；'
              '这是固定并发、闭环测试，不包括未派发请求的排队时间。', '',
              '## CPU 分布', '', '| 轮次 | .5 平均核数 | .6 平均核数 | .7 平均核数 | '
              'CPU 总核秒 | 最大/平均 | 客户端平均核数 |', '|---|---:|---:|---:|---:|---:|---:|']
    for s in summaries:
        cores = [s['be'][h]['mean_cores'] for h in HOSTS]
        total = sum(s['be'][h]['cpu_seconds'] for h in HOSTS)
        lines.append(f'| {s["name"]} | {cores[0]:.2f} | {cores[1]:.2f} | {cores[2]:.2f} | '
                     f'{total:.1f} | {max(cores)/statistics.mean(cores):.2f} | '
                     f'{s["client_cpu_seconds"]/s["wall_seconds"]:.3f} |')
    lines += ['', 'CPU 核数来自 BE 进程 user+system 时间增量；16 核为整台机器满负载。'
              '最大/平均越接近 1，三台 BE 的平均 CPU 越均匀。客户端 CPU 单独统计。', '',
              f'![BE CPU 时间曲线](cpu{suffix}.png)', '', '## 服务端耗时分解', '',
              '| 轮次 | BeginTxn | StreamLoadPut | ReadData | WriteData | ReceiveData | CommitAndPublish |',
              '|---|---:|---:|---:|---:|---:|---:|']
    keys = ['BeginTxnTimeMs', 'StreamLoadPutTimeMs', 'ReadDataTimeMs', 'WriteDataTimeMs',
            'ReceiveDataTimeMs', 'CommitAndPublishTimeMs']
    for s in summaries:
        rows = read_lines(root / s['name'] / 'requests.jsonl')
        assert len(rows) == 7500 and all(r['response']['Status'] == 'Success' for r in rows)
        assert all(sum(r['host'] == h for r in rows) == 2500 for h in HOSTS)
        lines.append('| ' + s['name'] + ' | ' + ' | '.join(
            f'{statistics.mean(r["response"][key] for r in rows):.1f}' for key in keys) + ' |')
    lines += ['', '单位 ms，为响应字段平均值；阶段可能重叠，不能相加当作总时间。', '',
              '## 正确性与适用范围', '',
              f'- 正式 {len(names)*7500:,} 次请求均为 Success，每次 10,000 行，无过滤；各 BE 请求数相同。',
              '- 每张表最终 75,300,000 行（含 30 次预热），消息字节总和与样本重复次数一致。',
              '- 原 DDL 的默认自适应随机分桶路径在关闭前移时复现 unknown partition channel；'
              '失败预检保留在 pre_off_v2，未混入成功性能统计。',
              '- 对照结果适用于 load_to_single_tablet:true、单 tablet、当前样本和当前集群；'
              '不能代表默认自适应分桶路径的性能。',
              '- 开启路径通过 BE 的 vtablet_writer_v2.cpp 日志和 WriteMemTableTime profile 验证；'
              'FE query-profile 列表未收录本次 Stream Load profile。',
              '- 本组每种状态仅有 ' + ('一轮' if packed_control else '两轮')
              + '，结论属于该环境实测；CPU 更均匀与端到端吞吐提升分别判断。', '',
              '- 集群自动统计保持开启，诊断时确认旧表的内部统计查询占用额外 CPU；'
              '本报告是当前环境端到端结果，不能把全部 CPU 变化归于导入。',
              '- 原配置 packed file 全局开启，前移路径内部禁用；真正的小文件门槛为 1 MiB，'
              '5 MiB 是合并文件目标大小。此次数据/索引文件约 2.05/4.19 MiB，'
              '都超出小文件门槛，不能据此把吞吐下降归因于禁用 packed file。', '',
              '## 线程与火焰图', '',
              '[开启前移后 .6/.7 的同步线程分析及火焰图](profile_analysis.md)，'
              '[关闭前移的线程分析及火焰图](profile_off/analysis.md)。'
              '使用独立的 48 并发诊断负载，不计入正式对照耗时。', '',
              '## 产物', '',
              f'- comparison{suffix}.csv：逐轮延迟和吞吐；cpu{suffix}.csv：逐 BE 的 CPU 和占比。',
              f'- cpu{suffix}.png/cpu{suffix}.svg：CPU 曲线。',
              '- 每轮目录：原始请求、监控、建表语句、tablet/partition 元数据及校验。',
              '- 远端目录：root@172.20.49.6:/root/load_test/memtable_ab_20260908。']
    (root / f'report{suffix}.md').write_text('\n'.join(lines) + '\n')
    print(root / f'report{suffix}.md')


if __name__ == '__main__':
    main()
