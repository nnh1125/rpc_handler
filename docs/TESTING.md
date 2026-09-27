# Correctness and concurrency validation

Step 7 completed on 2026-09-26 with Go 1.25.5 on darwin/arm64. Run from the repository root:

```sh
go test ./...
go test -race ./...
go vet ./...
```

All three checks passed. The captured output is in [results/step7-checks.txt](../results/step7-checks.txt). Go reused cached results for some unchanged packages, as recorded in that output. Tests use localhost TCP sockets and in-memory connections; environments that disallow local listening sockets must grant that access.

## Scenario coverage

| Scenario | Tests |
| --- | --- |
| Multiple clients and concurrent calls through shared workers | `TestConcurrentClientsThroughSharedWorkerPool` |
| Delayed/out-of-order responses and intact concurrent frames | `TestAsyncRoutesOutOfOrder`, `TestOutOfOrderAndSerializedResponses`, `TestConcurrentResponseFramesRemainIntact` |
| Broken connections fail all pending calls and allow later reconnects | `TestDisconnectFailsAllPending`, `TestReconnectForLaterCallWithRetriesDisabled` |
| Malformed requests/responses | `TestMalformedJSONClosesConnection`, `TestMalformedResponseFailsPendingCalls`, protocol package tests |
| Full server queue and client capacity | `TestQueueOverloadAndDrain`, `TestBoundedCallsAndCancelWhileQueued` |
| Cancellation waiting to send, mid-write, and awaiting reply | `TestBoundedCallsAndCancelWhileQueued`, `TestCancellationDuringPartialWrite`, `TestCancelWaitingResponseIgnoresLateResult` |
| Response arrival racing cancellation completes once and releases state | `TestResponseCancellationRaceCompletesOnce` |
| Lost response, fresh retry IDs, stale results, and another concurrent call | `TestRetryLostResponseUsesNewID`, `TestLostResponseRetryCannotConsumeOldOrOtherCallResult` |
| Retry cap, overload without retries, and original deadline | `TestRetryLimitAndApplicationErrors`, `TestRetryBackoffRespectsOriginalDeadline` |
| Slow readers, connection cap, and read deadlines | `TestSlowReaderReleasesWorker`, `TestConnectionLimitAndReadDeadline` |
| Shutdown drains accepted work, closes listeners, and enforces its deadline | `TestQueueOverloadAndDrain`, `TestShutdownClosesListener`, `TestShutdownDeadlineAndAdmission` |

Tests coordinate key events with channels, controlled handlers, and writer synchronization. Timeout values act as failure bounds or exercise configured deadlines. Cancellation/reply races accept either valid terminal outcome while requiring one response, a closed channel, and no remaining pending-call entry or capacity slot.

Reading a complete frame at the peer does not guarantee that the writer function has returned. Tests specifically exercising cancellation while awaiting a response synchronize with the writer first; partial-write cancellation is tested separately.

These checks validate correctness and concurrency behavior, not sustained throughput or benchmark accuracy. Those measurements begin with steps 8–9.
