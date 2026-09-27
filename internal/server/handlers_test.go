package server

import (
	"encoding/json"
	"rpc_handler/internal/protocol"
	"testing"
)

func TestHandlerValidationAndLimits(t *testing.T) {
	s, err := NewWithLimits(Limits{MaxDataBytes: 3, MaxSortValues: 3, MaxMatrixN: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, params string
		code           protocol.Code
	}{
		{"hash_compute", `{"data":"YWJjZA=="}`, protocol.ResourceExhausted},
		{"compress_data", `{"algorithm":"zlib","data":"YWJjZA=="}`, protocol.ResourceExhausted},
		{"compress_data", `{"algorithm":"gzip","data":""}`, protocol.InvalidArgument},
		{"compress_data", `{"algorithm":"lz4","data":"!"}`, protocol.InvalidArgument},
		{"compress_data", `{"data":""}`, protocol.InvalidArgument},
		{"sort_array", `{}`, protocol.InvalidArgument},
		{"sort_array", `{"values":null}`, protocol.InvalidArgument},
		{"sort_array", `{"values":[null]}`, protocol.InvalidArgument},
		{"sort_array", `{"values":[1.5]}`, protocol.InvalidArgument},
		{"sort_array", `{"values":[2147483648]}`, protocol.InvalidArgument},
		{"sort_array", `{"values":[-2147483649]}`, protocol.InvalidArgument},
		{"sort_array", `{"values":[1,2,3,4]}`, protocol.ResourceExhausted},
		{"matrix_multiply", `{"n":0,"a":[],"b":[]}`, protocol.InvalidArgument},
		{"matrix_multiply", `{"n":1.5,"a":[],"b":[]}`, protocol.InvalidArgument},
		{"matrix_multiply", `{"n":3,"a":[],"b":[]}`, protocol.ResourceExhausted},
		{"matrix_multiply", `{"n":2,"a":[1],"b":[1]}`, protocol.InvalidArgument},
		{"matrix_multiply", `{"n":1,"a":[null],"b":[1]}`, protocol.InvalidArgument},
		{"matrix_multiply", `{"n":1,"a":[1e309],"b":[1]}`, protocol.InvalidArgument},
		{"matrix_multiply", `{"n":1,"a":[1e308],"b":[2]}`, protocol.NumericError},
	} {
		response := s.Dispatch(protocol.Request{ID: "1", Method: tc.method, Params: json.RawMessage(tc.params)})
		if response.Error == nil || response.Error.Code != tc.code {
			t.Errorf("%s %s: %+v, want %s", tc.method, tc.params, response, tc.code)
		}
	}
	for _, tc := range []struct{ method, params string }{
		{"hash_compute", `{"data":"YWJj"}`}, {"sort_array", `{"values":[3,1,2]}`}, {"sort_array", `{"values":[]}`},
		{"matrix_multiply", `{"n":2,"a":[1,0,0,1],"b":[1,2,3,4]}`},
	} {
		if got := s.Dispatch(protocol.Request{ID: "1", Method: tc.method, Params: json.RawMessage(tc.params)}); got.Error != nil {
			t.Errorf("boundary rejected: %v", got.Error)
		}
	}
	if _, err := NewWithLimits(Limits{MaxDataBytes: -1}); err == nil {
		t.Fatal("accepted negative limit")
	}
}
