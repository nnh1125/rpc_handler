package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"syscall"
	"time"

	"rpc_handler/client"
	"rpc_handler/internal/loadgen"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	config := loadgen.DefaultConfig()
	workload := loadgen.DefaultWorkload()
	address := flag.String("addr", "127.0.0.1:8080", "RPC server")
	connections := flag.Int("connections", 1, "persistent client connections")
	warmup := flag.Duration("warmup", 2*time.Second, "unmeasured warm-up duration (0 disables)")
	output := flag.String("out", "results/loadgen", "new output directory (must not exist)")
	flag.Float64Var(&config.Rate, "rate", config.Rate, "offered requests per second")
	flag.DurationVar(&config.Duration, "duration", config.Duration, "measurement duration")
	flag.DurationVar(&config.Drain, "drain", config.Drain, "maximum post-window drain time")
	flag.DurationVar(&config.Timeout, "timeout", config.Timeout, "per-call timeout")
	flag.IntVar(&config.MaxInFlight, "max-in-flight", config.MaxInFlight, "global outstanding-call limit")
	flag.DurationVar(&config.LateThreshold, "late-threshold", config.LateThreshold, "delay threshold for late-dispatch count")
	flag.StringVar(&workload.Mix, "mix", workload.Mix, "comma-separated method:weight pairs")
	flag.IntVar(&workload.Bytes, "bytes", workload.Bytes, "hash/compression input bytes")
	flag.IntVar(&workload.SortSize, "sort-size", workload.SortSize, "sort array length")
	flag.IntVar(&workload.MatrixN, "matrix-n", workload.MatrixN, "matrix dimension")
	flag.StringVar(&workload.Algorithm, "algorithm", workload.Algorithm, "zlib or lz4")
	flag.BoolVar(&workload.Compressible, "compressible", false, "use repeating compression input rather than pseudorandom bytes")
	flag.IntVar(&workload.Samples, "samples", workload.Samples, "pre-generated samples per operation")
	flag.Int64Var(&workload.Seed, "seed", workload.Seed, "deterministic workload seed")
	flag.Parse()
	config.Seed = workload.Seed
	if *connections < 1 || *connections > 1024 || *warmup < 0 {
		return fmt.Errorf("connections must be 1..1024 and warmup nonnegative")
	}
	pool, err := loadgen.Prepare(workload)
	if err != nil {
		return err
	}
	// Reserve a fresh directory; never overwrite a previous benchmark.
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(*output, 0755); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	callers := make([]loadgen.Caller, 0, *connections)
	for i := 0; i < *connections; i++ {
		c, err := client.DialWithOptions(ctx, *address, client.Options{MaxInFlight: config.MaxInFlight, MaxRetries: 0})
		if err != nil {
			return err
		}
		defer c.Close()
		callers = append(callers, c)
	}
	var warmupSummary *loadgen.Summary
	if *warmup > 0 {
		warm := config
		warm.Duration = *warmup
		warmRun, err := loadgen.RunPhase(ctx, warm, callers, pool)
		if err != nil {
			return err
		}
		summary := loadgen.Summarize(warmRun, "")
		warmupSummary = &summary
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	measured, err := loadgen.RunPhase(ctx, config, callers, pool)
	if err != nil {
		return err
	}
	summaries := []loadgen.Summary{loadgen.Summarize(measured, "")}
	names := map[string]bool{}
	for _, w := range pool {
		names[w.Method] = true
	}
	methods := make([]string, 0, len(names))
	for name := range names {
		methods = append(methods, name)
	}
	slices.Sort(methods)
	for _, name := range methods {
		summaries = append(summaries, loadgen.Summarize(measured, name))
	}
	records, err := os.Create(filepath.Join(*output, "requests.csv"))
	if err != nil {
		return err
	}
	err = loadgen.WriteRecords(records, measured)
	closeErr := records.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	summary, err := os.Create(filepath.Join(*output, "summary.csv"))
	if err != nil {
		return err
	}
	err = loadgen.WriteSummaries(summary, summaries)
	closeErr = summary.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	metadata := struct {
		StartedUTC    time.Time
		Address       string
		Connections   int
		Warmup        string
		Config        loadgen.Config
		Workload      loadgen.WorkloadConfig
		GoVersion     string
		OS            string
		Architecture  string
		CPUs          int
		Retries       int
		Interrupted   bool
		Summaries     []loadgen.Summary
		WarmupSummary *loadgen.Summary
	}{measured.Start.UTC(), *address, *connections, warmup.String(), config, workload, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), 0, ctx.Err() != nil, summaries, warmupSummary}
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*output, "run.json"), append(encoded, '\n'), 0644); err != nil {
		return err
	}
	s := summaries[0]
	fmt.Printf("dispatched=%d success=%d errors=%d timeouts=%d missed=%d incomplete=%d throughput=%.2f/s mean=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms\n", s.Dispatched, s.Successes, s.Errors, s.Timeouts, s.Missed, s.Incomplete, s.Throughput, s.MeanMS, s.P50MS, s.P95MS, s.P99MS)
	fmt.Printf("Reports: %s\n", *output)
	return ctx.Err()
}
