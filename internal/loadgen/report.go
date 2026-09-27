package loadgen

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"time"
)

type Summary struct {
	Scope             string         `json:"scope"`
	Scheduled         int            `json:"scheduled"`
	Dispatched        int            `json:"dispatched"`
	Successes         int            `json:"successes"`
	Errors            int            `json:"errors"`
	Timeouts          int            `json:"timeouts"`
	Missed            int            `json:"missed"`
	Incomplete        int            `json:"incomplete"`
	CompletedInWindow int            `json:"successful_completions_in_window"`
	LateDispatches    int            `json:"late_dispatches"`
	OfferedRate       float64        `json:"offered_rate"`
	DispatchedRate    float64        `json:"dispatched_rate"`
	Throughput        float64        `json:"throughput"`
	MeanMS            float64        `json:"mean_ms"`
	P50MS             float64        `json:"p50_ms"`
	P95MS             float64        `json:"p95_ms"`
	P99MS             float64        `json:"p99_ms"`
	MeanSchedulingMS  float64        `json:"mean_scheduling_delay_ms"`
	MaxSchedulingMS   float64        `json:"max_scheduling_delay_ms"`
	Outcomes          map[string]int `json:"outcomes"`
	ErrorCodes        map[string]int `json:"error_codes"`
}

// Percentile uses nearest rank (ceil(p*n)); values are sorted on a copy.
// Empty input returns zero; summaries include Successes to disambiguate it.
func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
func Summarize(run Run, method string) Summary {
	s := Summary{Scope: method, Outcomes: map[string]int{}, ErrorCodes: map[string]int{}}
	if method == "" {
		s.Scope = "all"
	}
	var latencies []float64
	latencySum, delaySum := 0.0, 0.0
	for _, r := range run.Records {
		if method != "" && r.Method != method {
			continue
		}
		s.Scheduled++
		s.Outcomes[r.Outcome]++
		if r.ErrorCode != "" {
			s.ErrorCodes[r.ErrorCode]++
		}
		if r.SendNS >= 0 {
			s.Dispatched++
			delay := float64(r.SendNS-r.ScheduledNS) / float64(time.Millisecond)
			delaySum += delay
			if delay > s.MaxSchedulingMS {
				s.MaxSchedulingMS = delay
			}
			if r.SendNS-r.ScheduledNS > int64(run.Config.LateThreshold) {
				s.LateDispatches++
			}
		}
		switch r.Outcome {
		case "success":
			s.Successes++
			latency := float64(r.CompletedNS-r.SendNS) / float64(time.Millisecond)
			latencies = append(latencies, latency)
			latencySum += latency
			if r.CompletedNS >= 0 && r.CompletedNS < int64(run.Config.Duration) {
				s.CompletedInWindow++
			}
		case "error":
			s.Errors++
		case "timeout":
			s.Timeouts++
		case "incomplete":
			s.Incomplete++
		default:
			s.Missed++
		}
	}
	seconds := run.Config.Duration.Seconds()
	s.OfferedRate = float64(s.Scheduled) / seconds
	s.DispatchedRate = float64(s.Dispatched) / seconds
	s.Throughput = float64(s.CompletedInWindow) / seconds
	if s.Successes > 0 {
		s.MeanMS = latencySum / float64(s.Successes)
	}
	if s.Dispatched > 0 {
		s.MeanSchedulingMS = delaySum / float64(s.Dispatched)
	}
	s.P50MS = Percentile(latencies, .5)
	s.P95MS = Percentile(latencies, .95)
	s.P99MS = Percentile(latencies, .99)
	return s
}

func WriteRecords(w io.Writer, run Run) error {
	out := csv.NewWriter(w)
	if err := out.Write([]string{"index", "method", "params_bytes", "scheduled_ns", "decision_ns", "send_ns", "completed_ns", "latency_ns", "scheduling_delay_ns", "scheduled_to_completion_ns", "outcome", "error_code"}); err != nil {
		return err
	}
	for _, r := range run.Records {
		latency, delay, total := int64(-1), int64(-1), int64(-1)
		if r.SendNS >= 0 {
			delay = r.SendNS - r.ScheduledNS
		}
		if r.CompletedNS >= 0 {
			latency = r.CompletedNS - r.SendNS
			total = r.CompletedNS - r.ScheduledNS
		}
		row := []string{strconv.Itoa(r.Index), r.Method, strconv.Itoa(r.PayloadBytes)}
		for _, value := range []int64{r.ScheduledNS, r.DecisionNS, r.SendNS, r.CompletedNS, latency, delay, total} {
			row = append(row, strconv.FormatInt(value, 10))
		}
		row = append(row, r.Outcome, r.ErrorCode)
		if err := out.Write(row); err != nil {
			return err
		}
	}
	out.Flush()
	return out.Error()
}
func WriteSummaries(w io.Writer, summaries []Summary) error {
	out := csv.NewWriter(w)
	if err := out.Write([]string{"scope", "scheduled", "dispatched", "successes", "errors", "timeouts", "missed", "incomplete", "successful_completions_in_window", "late_dispatches", "offered_rate", "dispatched_rate", "throughput", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "mean_scheduling_delay_ms", "max_scheduling_delay_ms"}); err != nil {
		return err
	}
	for _, s := range summaries {
		row := []string{s.Scope}
		for _, v := range []int{s.Scheduled, s.Dispatched, s.Successes, s.Errors, s.Timeouts, s.Missed, s.Incomplete, s.CompletedInWindow, s.LateDispatches} {
			row = append(row, strconv.Itoa(v))
		}
		for _, v := range []float64{s.OfferedRate, s.DispatchedRate, s.Throughput, s.MeanMS, s.P50MS, s.P95MS, s.P99MS, s.MeanSchedulingMS, s.MaxSchedulingMS} {
			row = append(row, fmt.Sprintf("%.6f", v))
		}
		if err := out.Write(row); err != nil {
			return err
		}
	}
	out.Flush()
	return out.Error()
}
