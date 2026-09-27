package loadgen

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"reflect"
	"testing"
	"time"

	"rpc_handler/client"
	"rpc_handler/internal/protocol"
	"rpc_handler/internal/server"
)

type callerFunc func(context.Context, string, any) <-chan client.Response

func (f callerFunc) CallAsync(ctx context.Context, method string, params any) <-chan client.Response {
	return f(ctx, method, params)
}
func testPool() []Work { return []Work{{Method: "slow", Params: json.RawMessage(`{}`), Weight: 1}} }

func TestPercentileAndMeasurementWindow(t *testing.T) {
	values := []float64{5, 1, 4, 2, 3}
	for _, tc := range []struct{ p, want float64 }{{.5, 3}, {.95, 5}, {.99, 5}, {0, 1}, {1, 5}} {
		if got := Percentile(values, tc.p); got != tc.want {
			t.Fatalf("p=%v got %v", tc.p, got)
		}
	}
	if Percentile(nil, .95) != 0 || values[0] != 5 {
		t.Fatal("empty or input mutation")
	}
	run := Run{Config: Config{Duration: time.Second, LateThreshold: time.Millisecond}, Records: []Record{
		{Method: "a", ScheduledNS: 0, SendNS: 2000000, CompletedNS: 12000000, Outcome: "success"},
		{Method: "a", ScheduledNS: 990000000, SendNS: 990000000, CompletedNS: 1010000000, Outcome: "success"},
		{Method: "b", SendNS: 100, CompletedNS: 200, Outcome: "error", ErrorCode: "OVERLOADED"},
		{Method: "b", SendNS: -1, CompletedNS: -1, Outcome: "missed_capacity"},
		{Method: "a", SendNS: 200, CompletedNS: -1, Outcome: "incomplete"},
	}}
	s := Summarize(run, "")
	if s.Scheduled != 5 || s.Dispatched != 4 || s.Successes != 2 || s.Throughput != 1 || s.MeanMS != 15 || s.P95MS != 20 || s.Errors != 1 || s.Missed != 1 || s.Incomplete != 1 || s.LateDispatches != 1 {
		t.Fatalf("summary=%+v", s)
	}
	if got := Summarize(run, "a"); got.Scheduled != 3 || got.Errors != 0 {
		t.Fatal(got)
	}
	var csvData bytes.Buffer
	if err := WriteRecords(&csvData, run); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(&csvData).ReadAll()
	if err != nil || len(rows) != 6 {
		t.Fatalf("CSV rows=%d err=%v", len(rows), err)
	}
	if rows[4][7] != "-1" {
		t.Fatal("missed latency is not blank sentinel")
	}
}

func TestOpenLoopDoesNotWaitForCompletion(t *testing.T) {
	calls := 0
	blocked := callerFunc(func(context.Context, string, any) <-chan client.Response { calls++; return make(chan client.Response) })
	config := Config{Rate: 100, Duration: 100 * time.Millisecond, Drain: 20 * time.Millisecond, Timeout: time.Second, MaxInFlight: 2, Seed: 1}
	run, err := RunPhase(context.Background(), config, []Caller{blocked}, testPool())
	if err != nil {
		t.Fatal(err)
	}
	s := Summarize(run, "")
	if calls != 2 || s.Scheduled != 10 || s.Missed != 8 || s.Incomplete != 2 {
		t.Fatalf("open-loop summary=%+v calls=%d", s, calls)
	}
	for i, r := range run.Records {
		if r.ScheduledNS != int64(i)*int64(10*time.Millisecond) || r.Outcome == "" {
			t.Fatalf("arrival %d = %+v", i, r)
		}
	}
}

func TestGeneratorDelayRecordedWithoutScheduleReset(t *testing.T) {
	caller := callerFunc(func(context.Context, string, any) <-chan client.Response {
		// Model a slow local enqueue/serialization path, not response latency.
		time.Sleep(35 * time.Millisecond)
		result := make(chan client.Response, 1)
		result <- client.Response{Result: json.RawMessage(`true`)}
		close(result)
		return result
	})
	c := Config{Rate: 100, Duration: 80 * time.Millisecond, Drain: time.Second, Timeout: time.Second, MaxInFlight: 8, LateThreshold: time.Millisecond}
	run, err := RunPhase(context.Background(), c, []Caller{caller}, testPool())
	if err != nil {
		t.Fatal(err)
	}
	s := Summarize(run, "")
	if s.Scheduled != 8 || s.LateDispatches == 0 || s.Outcomes["missed_late"] == 0 {
		t.Fatalf("delay hidden: %+v", s)
	}
	for i, r := range run.Records {
		if r.ScheduledNS != int64(i)*int64(10*time.Millisecond) {
			t.Fatal("schedule reset")
		}
	}
}

func TestTimeoutAndRoundRobin(t *testing.T) {
	counts := [2]int{}
	callers := make([]Caller, 2)
	for i := range callers {
		callers[i] = callerFunc(func(context.Context, string, any) <-chan client.Response {
			counts[i]++
			return make(chan client.Response)
		})
	}
	c := Config{Rate: 20, Duration: 200 * time.Millisecond, Drain: time.Second, Timeout: 5 * time.Millisecond, MaxInFlight: 4}
	run, err := RunPhase(context.Background(), c, callers, testPool())
	if err != nil {
		t.Fatal(err)
	}
	if counts != [2]int{2, 2} || Summarize(run, "").Timeouts != 4 {
		t.Fatalf("counts=%v summary=%+v", counts, Summarize(run, ""))
	}
}

func TestPreparedWorkloadsAreDeterministic(t *testing.T) {
	c := DefaultWorkload()
	c.Mix = "hash_compute:2,sort_array:1,matrix_multiply:1,compress_data:1"
	c.Bytes = 4
	c.SortSize = 4
	c.MatrixN = 2
	c.Samples = 2
	first, err := Prepare(c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Prepare(c)
	if err != nil || !reflect.DeepEqual(first, second) || len(first) != 8 {
		t.Fatal("workload is not reproducible")
	}
	c.Seed++
	third, _ := Prepare(c)
	if reflect.DeepEqual(first, third) {
		t.Fatal("seed has no effect")
	}
	c.Mix = "unknown:1"
	if _, err := Prepare(c); err == nil {
		t.Fatal("unknown operation accepted")
	}
	c.Mix = "hash_compute:NaN"
	if _, err := Prepare(c); err == nil {
		t.Fatal("invalid weight accepted")
	}
}

func TestSlowServerOpenLoopOverTCP(t *testing.T) {
	srv, err := server.NewWithConfig(server.Config{Workers: 2, QueueSize: 8, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	if err := srv.Register("slow", func(json.RawMessage) (any, *protocol.RPCError) { <-release; return true, nil }); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	defer func() {
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rpc, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	config := Config{Rate: 100, Duration: 100 * time.Millisecond, Drain: 20 * time.Millisecond, Timeout: time.Second, MaxInFlight: 2}
	run, err := RunPhase(ctx, config, []Caller{rpc}, testPool())
	if err != nil {
		t.Fatal(err)
	}
	s := Summarize(run, "")
	if s.Scheduled != 10 || s.Dispatched != 2 || s.Missed != 8 || s.Incomplete != 2 || s.Successes != 0 {
		t.Fatalf("slow-server summary=%+v", s)
	}
}

func TestCancellationAndInvalidConfiguration(t *testing.T) {
	never := callerFunc(func(context.Context, string, any) <-chan client.Response { t.Error("unexpected dispatch"); return nil })
	c := DefaultConfig()
	c.Duration = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run, err := RunPhase(ctx, c, []Caller{never}, testPool())
	if err != nil {
		t.Fatal(err)
	}
	if got := Summarize(run, ""); got.Missed != got.Scheduled {
		t.Fatal(got)
	}
	c.Rate = 0
	if _, err := RunPhase(context.Background(), c, []Caller{never}, testPool()); err == nil {
		t.Fatal("invalid rate accepted")
	}
	if outcome, code := classify(&client.RPCError{Code: protocol.Overloaded, Message: "full"}); outcome != "error" || code != "OVERLOADED" {
		t.Fatal(outcome, code)
	}
	if outcome, _ := classify(errors.New("disconnect")); outcome != "error" {
		t.Fatal(outcome)
	}
}
