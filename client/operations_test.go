package client_test

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"github.com/pierrec/lz4/v4"
	"io"
	"net"
	"rpc_handler/client"
	"rpc_handler/internal/operations"
	"rpc_handler/internal/server"
	"slices"
	"testing"
	"time"
)

func TestOperationsOverTCP(t *testing.T) {
	c := connect(t, func(conn net.Conn) {
		srv := server.New()
		defer srv.Shutdown(context.Background())
		_ = srv.ServeConn(conn)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sorted operations.SortResult
	if err := c.Call(ctx, "sort_array", operations.SortParams{Values: []int32{3, -1, 3}}, &sorted); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sorted.Values, []int32{-1, 3, 3}) {
		t.Fatal(sorted)
	}
	var matrix operations.MatrixResult
	if err := c.Call(ctx, "matrix_multiply", operations.MatrixParams{N: 2, A: []float64{1, 2, 3, 4}, B: []float64{5, 6, 7, 8}}, &matrix); err != nil {
		t.Fatal(err)
	}
	if matrix.N != 2 || !slices.Equal(matrix.Values, []float64{19, 22, 43, 50}) {
		t.Fatal(matrix)
	}
	for _, algorithm := range []string{"zlib", "lz4"} {
		for _, input := range [][]byte{{}, {0, 255, 1, 128}} {
			var compressed operations.CompressResult
			if err := c.Call(ctx, "compress_data", operations.CompressParams{Algorithm: algorithm, Data: input}, &compressed); err != nil {
				t.Fatal(err)
			}
			var r io.Reader
			if algorithm == "zlib" {
				z, err := zlib.NewReader(bytes.NewReader(compressed.Data))
				if err != nil {
					t.Fatal(err)
				}
				defer z.Close()
				r = z
			} else {
				r = lz4.NewReader(bytes.NewReader(compressed.Data))
			}
			got, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(got, input) {
				t.Fatalf("compression %s: %v", algorithm, err)
			}
		}
	}
	for _, tc := range []struct{ method, params string }{
		{"sort_array", `{"values":[1.5]}`}, {"matrix_multiply", `{"n":2,"a":[],"b":[]}`}, {"compress_data", `{"algorithm":"bad","data":""}`},
	} {
		err := c.Call(ctx, tc.method, json.RawMessage(tc.params), nil)
		var remote *client.RPCError
		if !errors.As(err, &remote) || remote.Code != "INVALID_ARGUMENT" {
			t.Fatalf("remote error = %v", err)
		}
	}
}
