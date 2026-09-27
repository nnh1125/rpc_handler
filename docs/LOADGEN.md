# Open-loop load generator

Run the server with verbose request logging disabled, then run the load generator in another terminal:

```sh
go run ./cmd/server -workers 4

go run ./cmd/loadgen -rate 100 -duration 30s -warmup 2s \
  -connections 4 -max-in-flight 128 \
  -mix hash_compute:1,sort_array:1,matrix_multiply:1,compress_data:1 \
  -bytes 1024 -sort-size 1024 -matrix-n 16 -algorithm zlib \
  -seed 1 -out results/mixed-100
```

The output directory must be new so previous measurements cannot be overwritten. Prepare dependencies with `go mod download` before running. Baseline retries are always disabled. Server-side error logs can still affect overload measurements; record that limitation when interpreting results.

## Controls

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `127.0.0.1:8080` | Server endpoint |
| `-rate` | `100` | Offered arrivals per second; positive fractional rates are supported |
| `-duration` | `30s` | Measurement window |
| `-warmup` | `2s` | Separate warm-up phase; `0` disables it |
| `-drain` | `5s` | Maximum time after each window to await outstanding responses |
| `-timeout` | `5s` | Timeout for each dispatched call |
| `-connections` | `1` | Persistent connections, used round-robin |
| `-max-in-flight` | `128` | Global outstanding-call limit |
| `-mix` | `hash_compute:1` | Comma-separated method:weight pairs; positive weights need not sum to one |
| `-bytes` | `1024` | Decoded hash/compression input bytes |
| `-sort-size` | `1024` | Sort array length |
| `-matrix-n` | `16` | Square matrix dimension |
| `-algorithm` | `zlib` | Compression algorithm: `zlib` or `lz4` |
| `-compressible` | `false` | Use repeating compression bytes instead of pseudorandom bytes |
| `-samples` | `8` | Reusable pre-generated samples per operation |
| `-seed` | `1` | Reproduces input samples and scheduled workload choices |
| `-late-threshold` | `1ms` | Scheduling delay above which a dispatched call is counted late |
| `-out` | `results/loadgen` | New report directory |

To vary sizes, algorithms, or rates, run separate commands with distinct output directories. Inputs are limited to the server's default ceilings: 4 MiB of decoded byte data, 100,000 sort elements, or matrix dimension 256. Samples range from 1 to 64, and the serialized input pool is capped at 128 MiB. A phase supports at most 1,000,000 scheduled arrivals; the global in-flight limit is also capped at 1,000,000. These bounds prevent accidental unbounded memory growth.

## Scheduling and measurement

The generator pre-generates/serializes input samples and chooses each scheduled operation before starting its monotonic clock. Arrival `i` is scheduled at `start + i/rate`, for all arrivals strictly inside the configured window. It never waits for a previous response to schedule the next arrival.

At each arrival, the scheduler either dispatches or records a miss. When capacity is full, it records `missed_capacity`; it does not block waiting for capacity. If local scheduling or serialization falls behind, the original schedule stays fixed and actual dispatch delay is recorded. It catches up while the window is open, then marks remaining arrivals `missed_late` instead of sending them after the window. Consequently, inspect delay and miss counts to detect generator saturation or catch-up bursts.

Warm-up uses the same scheduling and drain rules, followed by a fresh measurement phase. Warm-up measurements are excluded from measurement CSV files; its aggregate summary is preserved in `run.json`. A warm-up timeout or incomplete request can leave computation running remotely because RPC cancellation does not cancel server computation. Check the warm-up summary before interpreting a benchmark.

Primary latency starts immediately before `CallAsync` and ends when the generator observes its terminal response. It includes client serialization, writer queueing, transport, server work, and client/generator response dispatch overhead. It is not a measurement of network transit alone. All elapsed times use Go's monotonic clock.

At the window deadline, arrivals stop. Remaining calls may finish during drain, subject to their own timeouts. At the drain deadline, unfinished calls are cancelled and marked `incomplete`. The generator waits only for bounded observer goroutines, not for the remote computation to stop.

## Output files

- `requests.csv`: one row for every scheduled arrival, including misses.
- `summary.csv`: aggregate and per-operation statistics.
- `run.json`: configuration, seed, phase start in UTC, platform/Go metadata, interruption flag, warm-up summary, and detailed outcome/error counts.

Raw time columns are nanoseconds relative to the measurement start:

| Column | Meaning |
| --- | --- |
| `scheduled_ns` | Intended arrival time |
| `decision_ns` | Time scheduler considered the arrival |
| `send_ns` | Start of `CallAsync`, or `-1` if never dispatched |
| `completed_ns` | Terminal response/timeout observation, or `-1` if missed/incomplete |
| `latency_ns` | `completed_ns - send_ns`, or `-1` |
| `scheduling_delay_ns` | `send_ns - scheduled_ns`, or `-1` |
| `scheduled_to_completion_ns` | `completed_ns - scheduled_ns`, or `-1` |

Other columns identify the index, method, outcome, error code, and serialized parameter size. `params_bytes` excludes the request envelope and four-byte framing header. Raw data payloads are not written to the reports.

Outcomes are `success`, `error`, `timeout`, `incomplete`, `missed_capacity`, `missed_late`, or `missed_cancelled`. RPC error codes such as `OVERLOADED` are preserved. Local client capacity failures are explicitly recorded as errors with `CLIENT_CAPACITY`; normally the generator's global cap prevents them.

Summary rules:

- Mean/p50/p95/p99 latency use successful calls sent inside the measurement window, including successes received during drain.
- Percentiles use nearest rank: `ceil(p * sample_count)`. Zero successful samples produce zero latency fields; always inspect `successes`.
- Throughput counts only successful completions before the measurement deadline, divided by configured duration. Drain completions do not inflate throughput.
- Offered rate is scheduled arrivals divided by duration; configured target rate is also preserved in metadata. Finite windows can make these slightly different.
- Dispatched rate excludes missed arrivals. Mean/max scheduling delay use dispatched arrivals; raw decision times also show late misses.
- Counts are separate: `scheduled = dispatched + missed`, and `dispatched = successes + errors + timeouts + incomplete`.
- On SIGINT/SIGTERM, remaining arrivals become misses, pending work becomes incomplete, and reports are written with `Interrupted: true`. Rates still use the configured window; exclude interrupted runs from performance conclusions.

`run.json` stores Go duration fields as integer nanoseconds, except the human-readable warm-up string.

## Validation

Tests cover deterministic input generation, nearest-rank percentiles, drain versus measurement-window throughput, timeout accounting, round-robin connections, and fixed scheduling under local delay. A real TCP test blocks the server handler and confirms that arrivals continue to be accounted for while the in-flight limit produces misses.

[The smoke-run artifacts](../results/step8-smoke/run.json) verify separate server/load-generator processes, two connections, all four operations, and CSV/JSON output. The 300 ms run is a functional check only, not a throughput or latency study. Formal experiments remain step 9.
