package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rpc_handler/internal/server"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "TCP listen address")
	config := server.DefaultConfig()
	flag.IntVar(&config.Limits.MaxDataBytes, "max-data-bytes", config.Limits.MaxDataBytes, "maximum decoded hash/compression input bytes")
	flag.IntVar(&config.Limits.MaxSortValues, "max-sort-values", config.Limits.MaxSortValues, "maximum sort array length")
	flag.IntVar(&config.Limits.MaxMatrixN, "max-matrix-n", config.Limits.MaxMatrixN, "maximum matrix dimension")
	flag.IntVar(&config.Workers, "workers", config.Workers, "number of shared workers")
	flag.IntVar(&config.QueueSize, "queue-size", config.QueueSize, "maximum queued requests")
	flag.IntVar(&config.MaxConnections, "max-connections", config.MaxConnections, "maximum active connections")
	flag.DurationVar(&config.ReadTimeout, "read-timeout", config.ReadTimeout, "time allowed to read a complete frame")
	flag.DurationVar(&config.WriteTimeout, "write-timeout", config.WriteTimeout, "time allowed to write a complete response")
	flag.BoolVar(&config.Verbose, "verbose", false, "log every request (disable for benchmarks)")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "maximum time to drain on SIGINT/SIGTERM")
	flag.Parse()
	if *shutdownTimeout <= 0 {
		log.Fatal("shutdown-timeout must be positive")
	}
	srv, err := server.NewWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	log.Printf("RPC server listening on %s", listener.Addr())
	select {
	case <-signals.Done():
	case err := <-done:
		if err != nil {
			log.Printf("serve: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
