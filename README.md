# Simple RPC Handler

A Go implementation of the [Tufts CS151 RPC assignment](https://www.cs.tufts.edu/comp/151FDS/pages/assignments.html).

## Current status

Steps 1–8 are complete: module setup, framed JSON, all four operations, a bounded concurrent TCP server, graceful shutdown, and a multiplexed client with optional retries. The open-loop load generator is implemented; formal performance experiments remain pending. See [DESIGN.md](DESIGN.md) and [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

## Setup and checks

Requires Go 1.25 or newer (developed with Go 1.25.5). Compression uses the pinned `github.com/pierrec/lz4/v4` v4.1.22 dependency; the other components use the standard library. Run `go mod download` to fetch dependencies.

```sh
go build ./...
go test ./...
go test -race ./...
go vet ./...
```

Place future executables in `bin/`, which is ignored by Git. Retain benchmark configurations and selected measurements under `results/` when the benchmark is implemented.

## Structure

`internal/protocol/` contains wire messages, validation, RPC error codes, and framing. Components are organized as follows (planned components are labeled):

| Directory | Responsibility |
| --- | --- |
| `cmd/server/` | Server command and configuration |
| `cmd/hash/` | Runnable remote hash example |
| `cmd/loadgen/` | Open-loop benchmark command |
| `client/` | Synchronous/asynchronous client, cancellation, reconnects, retries |
| `internal/operations/` | Computation handlers |
| `internal/server/` | Registry, dispatch, connections, bounded worker pool |
| `docs/` | Testing/load-generator guides; performance report planned |
| `results/` | Saved test output and functional smoke measurements |

## Run the server and hash example

Start the server in one terminal:

```sh
go run ./cmd/server -addr 127.0.0.1:8080
```

In another terminal:

```sh
go run ./cmd/hash -addr 127.0.0.1:8080 -data abc
# ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad

go run ./cmd/hash -data ''
# e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

The example has a configurable `-timeout` (default `5s`). Ctrl-C or SIGTERM stops admission and drains accepted requests within the shutdown timeout.

The client API is `client.Dial(ctx, address)`, followed by `c.Call(ctx, method, params, &result)` and `c.Close()`. For hashing, pass a struct with `Data []byte` tagged `json:"data"` and decode into a struct with `Hex string` tagged `json:"hex"`. Go encodes the bytes as base64 automatically. The runnable example is in `cmd/hash/main.go`.

Calls share a persistent connection and can complete out of order. `Call` wraps `CallAsync`, waits for its single terminal response, and decodes the result. RPC errors are returned as `*client.RPCError` and keep the connection usable. See the asynchronous API and retry settings below.

## Protocol usage

Each frame is a four-byte unsigned big-endian payload length followed by JSON. The default maximum is 16 MiB. Configure `protocol.Codec{MaxFrameSize: limit}` on both peers to override it; zero selects the default. Empty frames are invalid.

Requests require a nonempty string `id`, a nonempty string `method`, and an object `params`. IDs remain strings throughout. Responses require the ID and exactly one of `result` or `error`; an explicit `result: null` is valid. Errors require nonempty string `code` and `message`. Duplicate envelope fields are rejected; unknown fields are ignored. No version field is used.

```json
{"id":"42","method":"sort_array","params":{"values":[3,1,2]}}
{"id":"42","result":{"values":[1,2,3]}}
{"id":"43","error":{"code":"INVALID_ARGUMENT","message":"invalid values"}}
```

Code inside this module can send and read protocol messages using an `io.Writer` and `io.Reader` (such as a TCP connection):

```go
codec := protocol.Codec{}
err := codec.WriteRequest(conn, protocol.Request{
    ID: "42",
    Method: "sort_array",
    Params: json.RawMessage(`{"values":[3,1,2]}`),
})
if err != nil {
    return err
}
response, err := codec.ReadResponse(conn)
```

This snippet uses `encoding/json` and `rpc_handler/internal/protocol`; it is a wire-protocol example, not the public client API. Callers must serialize writes to a shared connection and set network deadlines.

Use `errors.Is` to inspect local `ErrInvalidMessage`, `ErrEmptyFrame`, `ErrFrameTooLarge`, and wrapped I/O errors. A received `Response.Error` is an application-level `*protocol.RPCError`, distinguishable with `errors.As`. `ReadRequest` preserves a usable ID on parameter or method validation failures so a future server can send a structured error. Framing/I/O failures require closing the stream; malformed JSON or unusable IDs also require closing it under the design's server policy. All operation parameters are validated by the server; server-side request-ID uniqueness tracking belongs to a later milestone.

## Operations and limits

| Method | Parameters | Result |
| --- | --- | --- |
| `hash_compute` | `data`: base64 string | `hex`: lowercase SHA-256 digest |
| `sort_array` | `values`: int32 array | `values`: ascending array |
| `matrix_multiply` | `n`: positive integer, `a` and `b`: row-major float64 arrays with `n*n` elements | `n`, `values`: product in row-major order |
| `compress_data` | `algorithm`: `zlib` or `lz4`, `data`: base64 string | `algorithm`, `data`: compressed bytes as base64 |

Empty byte strings and empty sort arrays are accepted. Missing/null required fields, fractional or out-of-range int32 values, and invalid matrix dimensions are rejected. Non-finite matrix results return `NUMERIC_ERROR`. Compression produces standard zlib streams or LZ4 frames.

Default operation limits are 4 MiB of decoded bytes for hashing/compression, 100,000 sort elements, and matrix dimension 256. Configure them before starting the server:

```sh
go run ./cmd/server -max-data-bytes 1048576 -max-sort-values 10000 -max-matrix-n 128
```

Zero selects a default; negative settings are rejected. Exceeding an operation limit returns `RESOURCE_EXHAUSTED`. Limits are checked before computation/result allocation, and numeric arrays and base64 bytes are decoded with bounds. The frame limit still applies independently; keep custom operation limits compatible with the maximum encoded request/response size. The server executes requests concurrently and may return responses out of order. Concurrent client calls are matched to responses by ID.

## Server concurrency and shutdown

The server uses a single bounded FIFO queue shared across connections. A fixed number of workers execute handlers and deliver responses. Writes are serialized per connection; request IDs associate out-of-order responses with their callers. Operation-specific decoding runs in the worker pool, after envelope validation and method lookup.

```sh
go run ./cmd/server -workers 4 -queue-size 128 -max-connections 128 \
  -read-timeout 30s -write-timeout 5s -shutdown-timeout 10s
```

Defaults: worker count equals `GOMAXPROCS`, queue capacity 128, connection limit 128, read timeout 30 seconds, write timeout 5 seconds, shutdown timeout 10 seconds. Zero selects defaults for pool/connection/deadline settings; negatives are rejected. The CLI shutdown timeout must be positive. Configure input limits with the flags above.

A full queue returns `OVERLOADED` immediately without waiting for queue space. Response delivery is still subject to the connection's write lock and timeout. Connections beyond the limit are closed. Read deadlines cover an entire frame, including idle time; clients should reconnect after an idle timeout. A failed or timed-out write closes that connection and releases the worker.

SIGINT/SIGTERM closes listeners, stops request admission, interrupts connection readers, and lets accepted work drain. At the shutdown deadline, connections are closed. Computation already executing cannot be forcibly stopped; the CLI exits after the timeout, while an embedded server's workers exit once queued/executing handlers finish. Embedded users should call `Shutdown(ctx)` when done, including after `ServeConn`.

Request errors and transport failures are logged without payloads. `-verbose` adds successful request logs with IDs, methods, durations, and result codes; leave it off during benchmarks. Handlers must be registered before serving begins.

## Asynchronous client and retries

Use `c.CallAsync(ctx, method, params)` to get a buffered `<-chan client.Response`. Each channel receives exactly one value with `Result` (`json.RawMessage`) or `Error`, then closes. The caller does not need to read immediately for completion to release resources.

```go
first := c.CallAsync(ctx, "sort_array", map[string]any{"values": []int32{3, 1, 2}})
second := c.CallAsync(ctx, "hash_compute", map[string]any{"data": []byte("abc")})
// Both calls are in flight; either may complete first.
for _, pending := range []<-chan client.Response{first, second} {
    response := <-pending
    if response.Error != nil {
        return response.Error
    }
    fmt.Println(string(response.Result))
}
```

`CallAsync` serializes parameters before returning but never waits for network I/O or capacity. `MaxInFlight` (default 128) bounds outstanding calls, including calls waiting to write, reconnect, or retry. At capacity, the returned channel immediately contains `client.ErrCapacity`; no extra call goroutine is started.

Cancellation removes the call's pending entry. Late replies are ignored. Cancelling while waiting for a reply or write permission preserves the shared connection. Cancelling during a write closes that connection because its frame may be incomplete; other pending calls receive a transport failure and may retry if enabled. Server computation is not cancelled. `Close` interrupts pending calls with `client.ErrClosed` and prevents future calls.

Default `client.Dial` disables retries. Opt in explicitly:

```go
c, err := client.DialWithOptions(ctx, "127.0.0.1:8080", client.Options{
    MaxInFlight: 128,
    MaxRetries: 2,
    BaseBackoff: 25 * time.Millisecond,
    MaxBackoff: 250 * time.Millisecond,
    DialTimeout: 5 * time.Second,
    WriteTimeout: 5 * time.Second,
})
```

Retries apply only to transient transport failures, with exponential backoff and jitter, a fresh ID per attempt, and the original caller deadline. There are at most two retries (three total attempts). RPC errors, overload, invalid parameters, and caller cancellation are not retried. A lost response can cause computation to execute again, so retries assume side-effect-free operations. Keep retries disabled for baseline benchmarks. Later calls reconnect after a broken connection even when automatic retries are disabled. Unknown response IDs are ignored; callers should supply deadlines so an unanswered call cannot wait indefinitely.

## Validation results

Step 7 correctness, concurrency, and failure checks passed, including the Go race detector and `go vet`. See [the testing guide](docs/TESTING.md) for scenario coverage and [saved check output](results/step7-checks.txt) for submission evidence.

## Load generation

With the server running:

```sh
go run ./cmd/loadgen -rate 100 -duration 30s -warmup 2s \
  -mix hash_compute:1,sort_array:1,matrix_multiply:1,compress_data:1 \
  -connections 4 -out results/mixed-100
```

The output directory must not already exist. Each run writes raw request CSV, aggregate/per-operation summary CSV, and JSON configuration metadata. Retries are disabled. See [the load-generator guide](docs/LOADGEN.md) for all flags, time-window rules, and outcome definitions. Formal rate sweeps and graphs are step 9.
