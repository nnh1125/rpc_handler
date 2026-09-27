package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"rpc_handler/internal/operations"
	"rpc_handler/internal/protocol"
)

// Limits is configured before serving. Zero fields select defaults.
// Negative limits are rejected by NewWithLimits.
type Limits struct {
	MaxDataBytes  int
	MaxSortValues int
	MaxMatrixN    int
}

func DefaultLimits() Limits {
	return Limits{MaxDataBytes: 4 << 20, MaxSortValues: 100000, MaxMatrixN: 256}
}

func argument(message string) *protocol.RPCError {
	return &protocol.RPCError{Code: protocol.InvalidArgument, Message: message}
}
func exhausted() *protocol.RPCError {
	return &protocol.RPCError{Code: protocol.ResourceExhausted, Message: "operation input exceeds configured limit"}
}

func fields(raw json.RawMessage) (map[string]json.RawMessage, *protocol.RPCError) {
	var values map[string]json.RawMessage
	if json.Unmarshal(raw, &values) != nil || values == nil {
		return nil, argument("params must be an object")
	}
	return values, nil
}

func dataBytes(raw json.RawMessage, limit int) ([]byte, *protocol.RPCError) {
	var encoded *string
	if json.Unmarshal(raw, &encoded) != nil || encoded == nil {
		return nil, argument("data must be a base64 string")
	}
	// Decode incrementally so even a configured small limit bounds allocation.
	decoded, err := io.ReadAll(io.LimitReader(base64.NewDecoder(base64.StdEncoding, strings.NewReader(*encoded)), int64(limit)+1))
	if err != nil {
		return nil, argument("data must be a base64 string")
	}
	if len(decoded) > limit {
		return nil, exhausted()
	}
	return decoded, nil
}

// Decode numeric arrays incrementally, rejecting nulls and checking the bound
// before appending. Typed JSON decoding enforces int32 and float64 ranges.
func numberArray[T int32 | float64](raw json.RawMessage, limit int) ([]T, *protocol.RPCError) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return nil, argument("expected numeric array")
	}
	values := make([]T, 0)
	for d.More() {
		if len(values) >= limit {
			return nil, exhausted()
		}
		var value *T
		if d.Decode(&value) != nil || value == nil {
			return nil, argument("invalid numeric array element")
		}
		values = append(values, *value)
	}
	if _, err := d.Token(); err != nil {
		return nil, argument("invalid numeric array")
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, argument("trailing array data")
	}
	return values, nil
}

func (s *Server) hashHandler(raw json.RawMessage) (any, *protocol.RPCError) {
	p, err := fields(raw)
	if err != nil {
		return nil, err
	}
	data, err := dataBytes(p["data"], s.limits.MaxDataBytes)
	if err != nil {
		return nil, err
	}
	return operations.HashCompute(operations.HashParams{Data: data}), nil
}
func (s *Server) sortHandler(raw json.RawMessage) (any, *protocol.RPCError) {
	p, err := fields(raw)
	if err != nil {
		return nil, err
	}
	values, err := numberArray[int32](p["values"], s.limits.MaxSortValues)
	if err != nil {
		return nil, err
	}
	return operations.SortArray(operations.SortParams{Values: values}), nil
}
func (s *Server) matrixHandler(raw json.RawMessage) (any, *protocol.RPCError) {
	p, err := fields(raw)
	if err != nil {
		return nil, err
	}
	var n int
	if json.Unmarshal(p["n"], &n) != nil || n <= 0 {
		return nil, argument("n must be a positive integer")
	}
	if n > s.limits.MaxMatrixN {
		return nil, exhausted()
	}
	if n > int(^uint(0)>>1)/n {
		return nil, argument("matrix dimension overflows")
	}
	a, err := numberArray[float64](p["a"], n*n)
	if err != nil {
		return nil, err
	}
	b, err := numberArray[float64](p["b"], n*n)
	if err != nil {
		return nil, err
	}
	result, computeErr := operations.MatrixMultiply(operations.MatrixParams{N: n, A: a, B: b})
	if errors.Is(computeErr, operations.ErrNumeric) {
		return nil, &protocol.RPCError{Code: protocol.NumericError, Message: computeErr.Error()}
	}
	if computeErr != nil {
		return nil, argument(computeErr.Error())
	}
	return result, nil
}
func (s *Server) compressHandler(raw json.RawMessage) (any, *protocol.RPCError) {
	p, err := fields(raw)
	if err != nil {
		return nil, err
	}
	var algorithm string
	if json.Unmarshal(p["algorithm"], &algorithm) != nil || (algorithm != "zlib" && algorithm != "lz4") {
		return nil, argument("algorithm must be zlib or lz4")
	}
	data, err := dataBytes(p["data"], s.limits.MaxDataBytes)
	if err != nil {
		return nil, err
	}
	result, computeErr := operations.CompressData(operations.CompressParams{Algorithm: algorithm, Data: data})
	if computeErr != nil {
		return nil, &protocol.RPCError{Code: protocol.InternalError, Message: "compression failed"}
	}
	return result, nil
}
