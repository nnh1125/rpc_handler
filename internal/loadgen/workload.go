// Package loadgen schedules open-loop arrivals and reports benchmark outcomes.
package loadgen

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"rpc_handler/internal/operations"
)

type WorkloadConfig struct {
	Mix          string
	Bytes        int
	SortSize     int
	MatrixN      int
	Algorithm    string
	Compressible bool
	Samples      int
	Seed         int64
}
type Work struct {
	Method string
	Params json.RawMessage
	Weight float64
}

func DefaultWorkload() WorkloadConfig {
	return WorkloadConfig{Mix: "hash_compute:1", Bytes: 1024, SortSize: 1024, MatrixN: 16, Algorithm: "zlib", Samples: 8, Seed: 1}
}

// Prepare generates and serializes a reusable input pool outside measurement.
func Prepare(c WorkloadConfig) ([]Work, error) {
	if c.Bytes < 0 || c.Bytes > 4<<20 || c.SortSize < 0 || c.SortSize > 100000 || c.MatrixN < 1 || c.MatrixN > 256 || c.Samples < 1 || c.Samples > 64 {
		return nil, fmt.Errorf("invalid workload size (bytes <= 4 MiB, sort <= 100000, matrix 1..256, samples 1..64)")
	}
	if c.Algorithm != "zlib" && c.Algorithm != "lz4" {
		return nil, fmt.Errorf("algorithm must be zlib or lz4")
	}
	r := rand.New(rand.NewSource(c.Seed))
	seen := map[string]bool{}
	var pool []Work
	totalBytes := 0
	for _, part := range strings.Split(c.Mix, ",") {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 2 {
			return nil, fmt.Errorf("mix must contain method:weight pairs")
		}
		method := strings.TrimSpace(fields[0])
		weight, err := strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
		if err != nil || weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) || seen[method] {
			return nil, fmt.Errorf("invalid or duplicate mix entry %q", part)
		}
		seen[method] = true
		for sample := 0; sample < c.Samples; sample++ {
			var params any
			switch method {
			case "hash_compute", "compress_data":
				data := make([]byte, c.Bytes)
				if c.Compressible && method == "compress_data" {
					for i := range data {
						data[i] = byte('a' + i%4)
					}
				} else {
					_, _ = r.Read(data)
				}
				if method == "hash_compute" {
					params = operations.HashParams{Data: data}
				} else {
					params = operations.CompressParams{Algorithm: c.Algorithm, Data: data}
				}
			case "sort_array":
				values := make([]int32, c.SortSize)
				for i := range values {
					values[i] = int32(r.Uint32())
				}
				params = operations.SortParams{Values: values}
			case "matrix_multiply":
				a, b := make([]float64, c.MatrixN*c.MatrixN), make([]float64, c.MatrixN*c.MatrixN)
				for i := range a {
					a[i] = r.Float64()*2 - 1
					b[i] = r.Float64()*2 - 1
				}
				params = operations.MatrixParams{N: c.MatrixN, A: a, B: b}
			default:
				return nil, fmt.Errorf("unknown operation %q", method)
			}
			data, err := json.Marshal(params)
			if err != nil {
				return nil, err
			}
			totalBytes += len(data)
			if totalBytes > 128<<20 {
				return nil, fmt.Errorf("input pool exceeds 128 MiB; reduce sizes or samples")
			}
			pool = append(pool, Work{Method: method, Params: data, Weight: weight})
		}
	}
	return pool, nil
}
