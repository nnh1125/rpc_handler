package operations

import (
	"bytes"
	"compress/zlib"
	"errors"
	"github.com/pierrec/lz4/v4"
	"io"
	"math"
	"slices"
	"testing"
)

func TestSortArray(t *testing.T) {
	for _, input := range [][]int32{{}, {3, -1, 3, 0, math.MaxInt32, math.MinInt32}} {
		before := slices.Clone(input)
		got := SortArray(SortParams{Values: input}).Values
		if !slices.IsSorted(got) || len(got) != len(input) || !slices.Equal(input, before) {
			t.Fatalf("sort = %v, input = %v", got, input)
		}
		want := slices.Clone(input)
		slices.Sort(want)
		if !slices.Equal(got, want) || got == nil {
			t.Fatal("incorrect result")
		}
	}
}
func TestMatrixMultiply(t *testing.T) {
	for _, tc := range []struct {
		p    MatrixParams
		want []float64
	}{
		{MatrixParams{2, []float64{1, 2, 3, 4}, []float64{5, 6, 7, 8}}, []float64{19, 22, 43, 50}},
		{MatrixParams{2, []float64{1.5, -2, 3, 4}, []float64{1, 0, 0, 1}}, []float64{1.5, -2, 3, 4}},
	} {
		got, err := MatrixMultiply(tc.p)
		if err != nil || !slices.Equal(got.Values, tc.want) {
			t.Fatalf("matrix = %+v, %v", got, err)
		}
	}
	for _, p := range []MatrixParams{{N: 0}, {N: -1}, {N: int(^uint(0) >> 1)}, {N: 2, A: []float64{1}, B: []float64{1}}, {N: 1, A: []float64{math.NaN()}, B: []float64{1}}, {N: 1, A: []float64{math.Inf(1)}, B: []float64{1}}} {
		if _, err := MatrixMultiply(p); err == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
	if _, err := MatrixMultiply(MatrixParams{1, []float64{math.MaxFloat64}, []float64{2}}); !errors.Is(err, ErrNumeric) {
		t.Fatalf("overflow: %v", err)
	}
}
func TestCompressData(t *testing.T) {
	for _, algorithm := range []string{"zlib", "lz4"} {
		for _, input := range [][]byte{{}, {0, 255, 1, 128}, bytes.Repeat([]byte("abc"), 1000)} {
			result, err := CompressData(CompressParams{algorithm, input})
			if err != nil {
				t.Fatal(err)
			}
			var r io.Reader
			if algorithm == "zlib" {
				z, err := zlib.NewReader(bytes.NewReader(result.Data))
				if err != nil {
					t.Fatal(err)
				}
				defer z.Close()
				r = z
			} else {
				r = lz4.NewReader(bytes.NewReader(result.Data))
			}
			got, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(got, input) || result.Algorithm != algorithm {
				t.Fatalf("roundtrip %s: %v", algorithm, err)
			}
		}
	}
	if _, err := CompressData(CompressParams{Algorithm: "gzip"}); err == nil {
		t.Fatal("accepted unsupported algorithm")
	}
}
