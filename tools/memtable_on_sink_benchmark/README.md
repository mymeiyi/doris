# Cloud memtable Stream Load benchmark

This harness compares ordinary Stream Load with memtable on sink using the same
one-tablet duplicate-key table with V2 inverted indexes. It targets the user's
three-BE test cluster at `172.20.49.5`, `.6`, and `.7`; SQL runs through
`172.20.49.1:9030`. Run the harness on `.6` with passwordless root SSH to each BE.
Only Python's standard library and the installed mysql client are required.

Place `bench.py`, `table.sql`, and the JSON-lines payload `data.jsonl` together.
The payload must contain the table's columns, end with a newline, and use
timestamps within `2026-09-08 10:00:00` to `11:00:00`. The experiment uses 10,000
rows extracted from the existing `ali_virginia_prod_06_backup` table, with only
the timestamp normalized. The payload stays on the test machine.

```sh
python3 bench.py --name off_1 --enabled false --count 7500 --concurrency 48
python3 bench.py --name on_1 --enabled true --count 7500 --concurrency 48
```

The formal concurrency was selected after paired 24/48/96-concurrency pilots.
Run formal comparisons sequentially in OFF/ON/ON/OFF order, allowing background
work to settle between rounds. Every round creates a new table, chooses a
tablet assigned to `.7`, warms up with 30 requests, and then measures exactly
7500 requests (2500 per BE). Empty candidate tables and loaded tables are retained.
The final row count and total message byte length include the warmup requests.

Both groups explicitly set `group_commit:off_mode` and
`load_to_single_tablet:true`. The latter avoids a reproduced Cloud adaptive
random-bucket routing failure (`unknown partition channel`) in the old writer.
With one tablet, this does not alter the data destination. The supplied
`enable_single_replica_compaction` table property is omitted because this Cloud
version rejects it. Automatic compaction, the other table settings, and file
cache remain enabled. The ordinary A/B runs do not change global FE/BE settings.

Use `--profile --count 30 --concurrency 3` for preflight. Preflight profiles are
written into BE logs in this deployed version, and do not appear in the FE
query-profile list. Correlate the unique request labels to query IDs and inspect
the writer log source (`vtablet_writer_v2.cpp` versus `vtablet_writer.cpp`) and
the sink's `WriteMemTableTime` profile counter to verify actual execution.

Each round retains every HTTP response, monotonic request duration, wall-clock
start/end timestamps, per-second BE process CPU/RSS and host network samples,
selected BE metrics, table metadata, validation results, and a JSON summary.
Each second, monitoring also records thread-pool queues, active threads, cumulative
task wait/execution times, and the load-stream/S3 bvars from BE port 8060.
Thread-pool execution time is elapsed time, including blocked time, not CPU time.
CPU is measured in occupied cores (16 cores per BE). The client runs on `.6`, so
its CPU time is reported separately. CPU totals are interpolated at the request
interval boundaries using the surrounding samples.

Requests are neither redirected nor retried. A failed load, publish timeout,
filtered row, wrong row count, or dead monitor fails the round and retains its
diagnostics; do not include such rounds in a successful throughput comparison.
The 7500-request duration excludes warmup and the subsequent validation query.
This is a closed-loop concurrency test, so its latency percentiles include only
active HTTP requests, not the waiting time of undispatched requests.

For the separately requested packed-file control pair, run `python3 packed_ab.py`.
It snapshots `enable_packed_file` on all BEs, waits for compaction to settle,
temporarily disables the option, runs one OFF/ON pair, and restores the original
values in `finally`. Updates are not persisted to configuration files. If the
process is forcibly killed, use `packed_config_before.json` to restore manually.
For the requested 96-concurrency forward-only follow-up, run
`python3 packed_ab.py --concurrency 96 --on-only`. Round names and configuration
snapshots gain a `_96` suffix, preserving the original 48-concurrency results.
The original cluster uses a 1 MiB small-file eligibility limit; 5 MiB is the target
packed-file size. The experiment's approximately 2.05 MiB segment and 4.19 MiB V2
index files exceed the eligibility limit, so a disabled option alone does not
establish a change in object count or throughput.

`profile.sh OUTPUT_DIRECTORY START_EPOCH` runs synchronized 30-second `perf`,
`pidstat`, and `top -H -1` captures on a BE during a separate diagnostic load.
It requires the existing FlameGraph scripts in `/root/FlameGraph`. Run it on each
host with the same start epoch. CPU-clock weights are nanoseconds. Raw perf data
records the host; `perf script --pid` exports only the BE process.

Generate the report on a separate analysis machine with matplotlib installed:

```sh
python3 report.py /path/to/copied/results
python3 report.py /path/to/copied/results --packed
```

Background automatic statistics collection was observed on existing tables and
is retained in this experiment. Results describe the deployed environment;
CPU sampling separates that work from ingestion where call stacks allow it.
