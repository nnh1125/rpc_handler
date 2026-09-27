package operations

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/pierrec/lz4/v4"
)

type SortParams struct {
	Values []int32 `json:"values"`
}
type SortResult struct {
	Values []int32 `json:"values"`
}

func SortArray(p SortParams) SortResult {
	values := append([]int32{}, p.Values...)
	slices.Sort(values)
	return SortResult{Values: values}
}

type MatrixParams struct {
	N int       `json:"n"`
	A []float64 `json:"a"`
	B []float64 `json:"b"`
}
type MatrixResult struct {
	N      int       `json:"n"`
	Values []float64 `json:"values"`
}

// MatrixMultiply validates shapes before allocating the result.
func MatrixMultiply(p MatrixParams) (MatrixResult, error) {
	if p.N <= 0 || p.N > int(^uint(0)>>1)/p.N || len(p.A) != p.N*p.N || len(p.B) != p.N*p.N {
		return MatrixResult{}, fmt.Errorf("matrices must have n*n elements and positive n")
	}
	for _, values := range [][]float64{p.A, p.B} {
		for _, v := range values {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return MatrixResult{}, fmt.Errorf("matrix inputs must be finite")
			}
		}
	}
	result := MatrixResult{N: p.N, Values: make([]float64, len(p.A))}
	for i := 0; i < p.N; i++ {
		for j := 0; j < p.N; j++ {
			sum := 0.0
			for k := 0; k < p.N; k++ {
				sum += p.A[i*p.N+k] * p.B[k*p.N+j]
			}
			if math.IsNaN(sum) || math.IsInf(sum, 0) {
				return MatrixResult{}, ErrNumeric
			}
			result.Values[i*p.N+j] = sum
		}
	}
	return result, nil
}

var ErrNumeric = fmt.Errorf("matrix result is not finite")

type CompressParams struct {
	Algorithm string `json:"algorithm"`
	Data      []byte `json:"data"`
}
type CompressResult struct {
	Algorithm string `json:"algorithm"`
	Data      []byte `json:"data"`
}

func CompressData(p CompressParams) (CompressResult, error) {
	var output bytes.Buffer
	var writer io.WriteCloser
	switch p.Algorithm {
	case "zlib":
		writer = zlib.NewWriter(&output)
	case "lz4":
		writer = lz4.NewWriter(&output)
	default:
		return CompressResult{}, fmt.Errorf("algorithm must be zlib or lz4")
	}
	if _, err := writer.Write(p.Data); err != nil {
		_ = writer.Close()
		return CompressResult{}, err
	}
	if err := writer.Close(); err != nil {
		return CompressResult{}, err
	}
	return CompressResult{Algorithm: p.Algorithm, Data: output.Bytes()}, nil
}
