package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"rpc_handler/client"
)

type Caller interface {
	CallAsync(context.Context, string, any) <-chan client.Response
}

type Config struct {
	Rate          float64
	Duration      time.Duration
	Drain         time.Duration
	Timeout       time.Duration
	MaxInFlight   int
	Seed          int64
	LateThreshold time.Duration
}

func DefaultConfig() Config {
	return Config{Rate: 100, Duration: 30 * time.Second, Drain: 5 * time.Second, Timeout: 5 * time.Second, MaxInFlight: 128, Seed: 1, LateThreshold: time.Millisecond}
}

// Times are monotonic offsets from the phase start; -1 means not applicable.
// PayloadBytes is the encoded params size (excludes envelope and frame header).
type Record struct {
	Index        int
	Method       string
	PayloadBytes int
	ScheduledNS  int64
	DecisionNS   int64
	SendNS       int64
	CompletedNS  int64
	Outcome      string
	ErrorCode    string
}
type Run struct {
	Start   time.Time
	Config  Config
	Records []Record
}

func RunPhase(ctx context.Context, c Config, callers []Caller, pool []Work) (Run, error) {
	if c.Rate <= 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate > 1e9 || c.Duration <= 0 || c.Drain < 0 || c.Timeout <= 0 || c.MaxInFlight <= 0 || c.MaxInFlight > 1000000 || c.LateThreshold < 0 || len(callers) == 0 || len(pool) == 0 {
		return Run{}, fmt.Errorf("invalid run configuration")
	}
	countFloat := math.Ceil(c.Rate * c.Duration.Seconds())
	if countFloat > 1000000 {
		return Run{}, fmt.Errorf("phase exceeds 1,000,000 scheduled arrivals; reduce rate or duration")
	}
	if c.Duration > time.Duration(math.MaxInt64)-c.Drain {
		return Run{}, fmt.Errorf("duration plus drain overflows")
	}
	total := 0.0
	for _, w := range pool {
		if w.Weight <= 0 || math.IsNaN(w.Weight) || math.IsInf(w.Weight, 0) {
			return Run{}, fmt.Errorf("invalid workload weight")
		}
		total += w.Weight
	}
	if math.IsInf(total, 0) {
		return Run{}, fmt.Errorf("workload weight sum overflows")
	}
	count := int(countFloat)
	records := make([]Record, count)
	choices := make([]int, count)
	rng := rand.New(rand.NewSource(c.Seed))
	for i := range records {
		pick := rng.Float64() * total
		chosen := len(pool) - 1
		for j, w := range pool {
			pick -= w.Weight
			if pick < 0 {
				chosen = j
				break
			}
		}
		choices[i] = chosen
		records[i] = Record{Index: i, Method: pool[chosen].Method, PayloadBytes: len(pool[chosen].Params), ScheduledNS: int64(float64(i) / c.Rate * float64(time.Second)), DecisionNS: -1, SendNS: -1, CompletedNS: -1}
	}
	start := time.Now()
	end := start.Add(c.Duration)
	cutoff := end.Add(c.Drain)
	runCtx, cancel := context.WithDeadline(ctx, cutoff)
	defer cancel()
	slots := make(chan struct{}, c.MaxInFlight)
	var wg sync.WaitGroup
	dispatched := 0
	for i := range records {
		record := &records[i]
		target := start.Add(time.Duration(record.ScheduledNS))
		if delay := time.Until(target); delay > 0 && ctx.Err() == nil {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
		now := time.Now()
		record.DecisionNS = now.Sub(start).Nanoseconds()
		if ctx.Err() != nil {
			record.Outcome = "missed_cancelled"
			record.ErrorCode = "CANCELLED"
			continue
		}
		if !now.Before(end) {
			record.Outcome = "missed_late"
			record.ErrorCode = "ARRIVAL_WINDOW_ENDED"
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			record.Outcome = "missed_capacity"
			record.ErrorCode = "IN_FLIGHT_LIMIT"
			continue
		}
		callCtx, callCancel := context.WithTimeout(runCtx, c.Timeout)
		work := pool[choices[i]]
		record.SendNS = time.Since(start).Nanoseconds()
		if record.SendNS >= int64(c.Duration) {
			record.SendNS = -1
			record.Outcome = "missed_late"
			record.ErrorCode = "ARRIVAL_WINDOW_ENDED"
			callCancel()
			<-slots
			continue
		}
		// CallAsync marshals the prepared JSON and enqueues without waiting on I/O.
		responses := callers[dispatched%len(callers)].CallAsync(callCtx, work.Method, work.Params)
		dispatched++
		wg.Add(1)
		go func(record *Record) {
			defer wg.Done()
			defer func() { <-slots }()
			defer callCancel()
			select {
			case response, ok := <-responses:
				completed := time.Now()
				if runCtx.Err() != nil || !completed.Before(cutoff) {
					record.Outcome = "incomplete"
					record.ErrorCode = "DRAIN_OR_RUN_CANCELLED"
					return
				}
				record.CompletedNS = completed.Sub(start).Nanoseconds()
				if !ok {
					record.Outcome = "error"
					record.ErrorCode = "EMPTY_RESPONSE"
					return
				}
				record.Outcome, record.ErrorCode = classify(response.Error)
			case <-callCtx.Done():
				if runCtx.Err() != nil {
					record.Outcome = "incomplete"
					record.ErrorCode = "DRAIN_OR_RUN_CANCELLED"
				} else {
					record.CompletedNS = time.Since(start).Nanoseconds()
					record.Outcome = "timeout"
					record.ErrorCode = "DEADLINE_EXCEEDED"
				}
			}
		}(record)
	}
	// Preserve the full configured phase even when the final call finishes early.
	if delay := time.Until(end); delay > 0 && ctx.Err() == nil {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
	wg.Wait()
	return Run{Start: start, Config: c, Records: records}, nil
}

func classify(err error) (string, string) {
	if err == nil {
		return "success", ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", "DEADLINE_EXCEEDED"
	}
	if errors.Is(err, client.ErrCapacity) {
		return "error", "CLIENT_CAPACITY"
	}
	var remote *client.RPCError
	if errors.As(err, &remote) {
		return "error", string(remote.Code)
	}
	if errors.Is(err, context.Canceled) {
		return "error", "CANCELLED"
	}
	return "error", "TRANSPORT_OR_CLIENT_ERROR"
}
