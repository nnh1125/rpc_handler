# RPC Handler Implementation Plan

**Status:** Steps 1–8 completed; later milestones and benchmark results are pending.  
**Design reference:** [DESIGN.md](DESIGN.md)  
**Assignment:** [Tufts CS151 Simple RPC Handler](https://www.cs.tufts.edu/comp/151FDS/pages/assignments.html)

Build in Go, starting with one working remote hash call and then adding the remaining operations, concurrency, failure handling, and measurement. Follow the design document's protocol: requests contain `id`, `method`, and `params`; responses contain `id` and exactly one of `result` or `error`. There is no protocol version field.

## 1. Set up the project

- [x] Initialize a Go module and record the required Go version.
- [x] Create the package structure below as the relevant components are implemented.
- [x] Add a README with build, test, server, and client usage instructions.
- [x] Keep generated binaries out of version control; retain reproducible benchmark inputs, configurations, and selected results.

```text
cmd/server/           Server CLI
cmd/loadgen/          Benchmark CLI
client/              Public RPC client
internal/protocol/   Wire messages, framing, errors
internal/operations/ Computation and parameter validation
internal/server/     Registry, connections, dispatch, workers
results/             CSV measurements and graphs
docs/                Performance report and examples
```

**Completion check:** The module builds and package responsibilities are clear.

## 2. Implement protocol framing

- [x] Define request, success response, and error response structures. Use `json.RawMessage` for operation-specific parameters and results.
- [x] Implement a four-byte unsigned big-endian length prefix followed by JSON.
- [x] Read headers and payloads fully; handle partial writes and reject zero-progress writes.
- [x] Enforce the configurable 16 MiB default frame limit before allocating inbound payloads and before sending outbound frames.
- [x] Validate required fields and response shape. Preserve string request IDs without numeric conversion.
- [x] Define the design's error codes and distinguish structured RPC errors from local transport errors.
- [x] Test partial headers/payloads, consecutive frames, truncated streams, malformed JSON, oversized lengths, and invalid envelopes.

**Completion check:** Messages round-trip through fragmented I/O, and invalid frames fail without unbounded allocation.

## 3. Deliver the first end-to-end call

- [x] Implement `hash_compute` as a pure function with typed parameters and results.
- [x] Create the method registry and a minimal server connection loop.
- [x] Add a minimal client that sends a framed request and receives its matching response.
- [x] Add a runnable example that remotely hashes known bytes and prints the digest.
- [x] Verify known hash vectors and an empty byte input, both locally and through TCP.

**Completion check:** A separately running client receives the expected digest from the server. Keep this integration test as later milestones change the networking code.

## 4. Complete the operation handlers

| Handler | Implementation work | Essential checks |
| --- | --- | --- |
| `sort_array` | Decode int32 values and sort ascending | Empty input, duplicates, negatives, integer bounds, rejection of fractions |
| `matrix_multiply` | Validate `n` and flat row-major arrays; implement straightforward multiplication | Identity and known products, mismatched lengths, invalid dimensions, overflow-safe sizing, non-finite results |
| `compress_data` | Support zlib streams and LZ4 frames; pin the LZ4 dependency | Both algorithms decompress to the original bytes; empty and binary inputs; unsupported algorithm |

- [x] Apply configurable input limits before expensive computation or output allocation.
- [x] Validate base64 decoding and required parameter fields.
- [x] Keep operation code independent of sockets and request dispatch.
- [x] Register all handlers and test each through the client.

**Completion check:** Every operation passes unit and end-to-end tests; invalid parameters produce structured errors.

## 5. Add bounded server concurrency

- [x] Give each connection a request reader and serialize all writes to that connection.
- [x] Dispatch valid requests through a bounded shared queue to a fixed worker pool.
- [x] Make queue admission nonblocking and return `OVERLOADED` when full.
- [x] Preserve request IDs when responses complete out of order.
- [x] Configure worker count, queue size, connection limit, input limits, and read/write deadlines through the server CLI.
- [x] Ensure write deadlines cover response delivery and failed delivery releases the worker.
- [x] Close connections for unparseable frames or unusable IDs; return structured errors for identifiable invalid calls.
- [x] Implement shutdown: stop admission, drain accepted work within a deadline, then close connections.
- [x] Add configurable logging with request IDs, operation names, durations, and error codes.

**Completion check:** Concurrent clients work correctly; a full queue returns overload errors; a slow or disconnected peer cannot hold resources indefinitely.

## 6. Complete the client API and failure handling

- [x] Implement one background response reader per connection and serialized request writes.
- [x] Generate connection-unique IDs and register pending calls before sending requests.
- [x] Implement `CallAsync` with a buffered result channel that receives one terminal response and then closes.
- [x] Implement `Call` by waiting on the same machinery and decoding the result.
- [x] Make response arrival, cancellation, and connection failure resolve each call exactly once.
- [x] Remove cancelled requests and ignore late responses; document that server computation may continue.
- [x] Fail all pending requests when a connection breaks and reconnect for eligible retries or later calls.
- [x] Add optional transient-transport retries: at most two retries, capped exponential backoff with jitter, a new ID per attempt, and the original caller deadline.
- [x] Return validation and overload errors directly. Document that retries can repeat computation.
- [x] Ensure `CallAsync` returns promptly even when writes are delayed. Bound outbound work rather than allowing an unlimited goroutine backlog.

**Completion check:** Out-of-order responses reach the correct callers; cancellation and disconnects leave no pending-call leaks; retries remain within their configured limits.

## 7. Validate concurrency and failure behavior

- [x] Add deterministic integration tests using controlled handlers and connections for delayed responses, concurrent requests, dropped connections, malformed messages, overload, and shutdown.
- [x] Verify client cancellation while waiting to send and while waiting for a response.
- [x] Verify that a lost response followed by a retry does not misroute the later response.
- [x] Run the following checks and save the final output for submission:

```sh
go test ./...
go test -race ./...
go vet ./...
```

**Completion check:** Correctness tests and the race detector pass before performance measurements begin.

Validation evidence: [testing guide](docs/TESTING.md) and [saved check output](results/step7-checks.txt).

## 8. Implement the open-loop load generator

- [x] Add CLI settings for target rate, duration, warm-up, operation mix, payload sizes, connections, in-flight limit, timeout, and deterministic seed.
- [x] Pre-generate inputs and distribute calls over persistent client connections.
- [x] Schedule arrivals against a monotonic start time using `start + i/rate`, independently of response completion.
- [x] Record missed dispatches when the in-flight limit is reached; never wait for a response to create the next scheduled arrival.
- [x] Record scheduling delays when the generator falls behind instead of hiding them by resetting the schedule.
- [x] Disable retries for baseline runs and keep verbose logging off.
- [x] Record scheduled arrival, send start, completion, operation, payload size, outcome, and error code in raw CSV. Measure durations using monotonic time.
- [x] Stop arrivals at the measurement deadline and allow a bounded drain period; mark remaining requests incomplete.
- [x] Report mean, p50, p95, and p99 latency, offered/dispatched rates, successful throughput, errors, misses, and scheduling delay.
- [x] Compute throughput from completions within the measurement window. Use requests sent during that window for latency statistics, including their completions during drain; identify incomplete requests separately.
- [x] Test percentile calculations against known samples and verify open-loop scheduling with a deliberately slow handler.

**Completion check:** Slow responses increase outstanding work or recorded misses without reducing the configured arrival schedule. All attempts have a recorded outcome.

Usage and measurement definitions: [load generator guide](docs/LOADGEN.md).

## 9. Run experiments and produce graphs

- [ ] Start with an inexpensive operation to check that the generator can sustain the intended rates.
- [ ] Benchmark each operation separately at fixed input sizes, then run a documented mixed workload.
- [ ] Sweep rates from below capacity through saturation; use 10, 50, 100, 500, and 1,000 requests/second as starting points and extend or refine the sweep based on observations.
- [ ] Start with 30-second measurement windows after warm-up and at least three repetitions per configuration.
- [ ] Measure both compression algorithms and include representative compressible and incompressible inputs.
- [ ] Record hardware, OS, Go version, worker count, queue size, workload seed, and whether client and server share a host.
- [ ] Save raw CSV and add a reproducible plotting command, such as a gnuplot script.
- [ ] Plot mean/p95 latency versus offered rate, successful throughput versus offered rate, and error/miss rates. Compare operations with input sizes labeled.
- [ ] State the latency/error threshold used to define sustainable throughput and explain generator limitations or CPU contention that affect interpretation.

**Completion check:** Graphs can be regenerated from saved measurements, and the report explains observed saturation without treating generator misses or errors as successful throughput.

## 10. Prepare the submission

- [ ] Complete README setup instructions, API examples, protocol details, and exact benchmark/plotting commands.
- [ ] Update [DESIGN.md](DESIGN.md) to reflect the final implementation and format it to the assignment's two-page design-document requirement.
- [ ] Write `docs/PERFORMANCE.md` with methodology, graphs, throughput findings, operation comparisons, and limitations.
- [ ] Include unit-test output, raw benchmark data, and configuration details needed to reproduce reported results.
- [ ] Verify the documented build, test, and example commands from a clean checkout.

**Completion check:** Source code, README, test results, performance report, and final design document are ready to submit.
