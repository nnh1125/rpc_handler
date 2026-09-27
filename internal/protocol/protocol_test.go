package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

type fragmentWriter struct{ bytes.Buffer }

func (w *fragmentWriter) Write(p []byte) (int, error) {
	if len(p) > 2 {
		p = p[:2]
	}
	return w.Buffer.Write(p)
}

type fragmentReader struct{ io.Reader }

func (r fragmentReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestRoundTripFragmentedAndConsecutive(t *testing.T) {
	c := Codec{}
	var stream fragmentWriter
	req := Request{ID: "9007199254740993", Method: "sort_array", Params: json.RawMessage(`{"values":[3,1]}`)}
	responses := []Response{
		{ID: req.ID, Result: json.RawMessage(`{"values":[1,3]}`)},
		{ID: "other", Error: &RPCError{Code: InvalidArgument, Message: "bad argument"}},
	}
	if err := c.WriteRequest(&stream, req); err != nil {
		t.Fatal(err)
	}
	for _, response := range responses {
		if err := c.WriteResponse(&stream, response); err != nil {
			t.Fatal(err)
		}
	}
	r := fragmentReader{&stream.Buffer}
	got, err := c.ReadRequest(r)
	if err != nil || got.ID != req.ID || got.Method != req.Method || !bytes.Equal(got.Params, req.Params) {
		t.Fatalf("request = %+v, %v", got, err)
	}
	for _, want := range responses {
		got, err := c.ReadResponse(r)
		if err != nil || got.ID != want.ID || !bytes.Equal(got.Result, want.Result) {
			t.Fatalf("response = %+v, %v", got, err)
		}
		if want.Error != nil && (got.Error == nil || *got.Error != *want.Error) {
			t.Fatalf("error = %+v", got.Error)
		}
	}
	if _, err := c.ReadRequest(r); !errors.Is(err, io.EOF) {
		t.Fatalf("EOF = %v", err)
	}
}

func TestFrameBoundaries(t *testing.T) {
	c := Codec{MaxFrameSize: 8}
	var stream bytes.Buffer
	if err := c.WriteFrame(&stream, []byte("12345678")); err != nil {
		t.Fatal(err)
	}
	frame := append([]byte(nil), stream.Bytes()...)
	if binary.BigEndian.Uint32(frame[:4]) != 8 {
		t.Fatal("incorrect header")
	}
	if got, err := c.ReadFrame(&stream); err != nil || string(got) != "12345678" {
		t.Fatalf("read = %q, %v", got, err)
	}
	for n := 1; n < len(frame); n++ {
		_, err := c.ReadFrame(bytes.NewReader(frame[:n]))
		if n == 4 { // No payload bytes: io.ReadFull returns EOF.
			if !errors.Is(err, io.EOF) {
				t.Fatalf("cut %d: %v", n, err)
			}
		} else if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("cut %d: %v", n, err)
		}
	}
	for _, tc := range []struct {
		size uint32
		want error
	}{{0, ErrEmptyFrame}, {9, ErrFrameTooLarge}, {^uint32(0), ErrFrameTooLarge}} {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], tc.size)
		// Header-only input proves rejection happens before attempting a payload read.
		if _, err := c.ReadFrame(bytes.NewReader(h[:])); !errors.Is(err, tc.want) {
			t.Fatalf("size %d: %v", tc.size, err)
		}
	}
	for _, data := range [][]byte{nil, []byte("123456789")} {
		var out bytes.Buffer
		if err := c.WriteFrame(&out, data); err == nil || out.Len() != 0 {
			t.Fatalf("invalid frame wrote bytes: %v", err)
		}
	}
	if (Codec{}).limit() != 16<<20 {
		t.Fatal("incorrect default limit")
	}
}

func TestWriteFailures(t *testing.T) {
	c := Codec{}
	if err := c.WriteFrame(writerFunc(func([]byte) (int, error) { return 0, nil }), []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero progress: %v", err)
	}
	failure := errors.New("connection lost")
	for _, failAt := range []int{1, 2} {
		calls := 0
		w := writerFunc(func(p []byte) (int, error) {
			calls++
			if calls == failAt {
				return 0, failure
			}
			return len(p), nil
		})
		if err := c.WriteFrame(w, []byte("x")); !errors.Is(err, failure) {
			t.Fatalf("write failure: %v", err)
		}
	}
}

func TestInvalidRequests(t *testing.T) {
	cases := []string{
		`null`, `[]`, `{`, `{} {}`, `{}`, `{"id":42,"method":"x","params":{}}`,
		`{"id":" ","method":"x","params":{}}`, `{"id":"x","params":{}}`,
		`{"id":"x","method":1,"params":{}}`, `{"id":"x","method":"","params":{}}`,
		`{"id":"x","method":"m"}`, `{"id":"x","method":"m","params":null}`,
		`{"id":"x","method":"m","params":[]}`, `{"id":"x","id":"y","method":"m","params":{}}`,
	}
	for _, data := range cases {
		t.Run(data, func(t *testing.T) {
			var wire bytes.Buffer
			c := Codec{}
			if err := c.WriteFrame(&wire, []byte(data)); err != nil {
				t.Fatal(err)
			}
			if _, err := c.ReadRequest(&wire); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("accepted %s: %v", data, err)
			}
		})
	}
	r, err := decodeRequest([]byte(`{"id":"keep-me","method":false,"params":{}}`))
	if r.ID != "keep-me" || !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("lost usable ID: %+v %v", r, err)
	}
}

func TestResponseValidation(t *testing.T) {
	invalid := []string{
		`null`, `{}`, `{"id":1,"result":{}}`, `{"id":"x"}`,
		`{"id":"x","result":{},"error":null}`, `{"id":"x","error":null}`,
		`{"id":"x","error":{}}`, `{"id":"x","error":{"code":"X","message":""}}`,
		`{"id":"x","error":{"code":1,"message":"bad"}}`,
		`{"id":"x","result":{},"result":null}`,
	}
	for _, data := range invalid {
		if _, err := decodeResponse([]byte(data)); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("accepted %s: %v", data, err)
		}
	}
	for _, data := range []string{`{"id":"x","result":null}`, `{"id":"x","result":{}}`, `{"id":"x","error":{"code":"INVALID_REQUEST","message":"bad"}}`} {
		if _, err := decodeResponse([]byte(data)); err != nil {
			t.Errorf("rejected %s: %v", data, err)
		}
	}
}

func TestInvalidOutboundMessagesWriteNothing(t *testing.T) {
	var out bytes.Buffer
	c := Codec{}
	if err := c.WriteRequest(&out, Request{ID: "x", Method: "m", Params: json.RawMessage(`null`)}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatal(err)
	}
	for _, r := range []Response{
		{ID: "x"}, {ID: "x", Result: json.RawMessage(`{`)},
		{ID: "x", Result: json.RawMessage(`null`), Error: &RPCError{Code: InternalError, Message: "bad"}},
		{ID: "x", Error: &RPCError{}},
	} {
		if err := c.WriteResponse(&out, r); !errors.Is(err, ErrInvalidMessage) {
			t.Fatal(err)
		}
	}
	if out.Len() != 0 {
		t.Fatal("invalid messages wrote bytes")
	}
}

func TestRPCErrorIsDistinctFromLocalErrors(t *testing.T) {
	remote := &RPCError{Code: Overloaded, Message: "queue full"}
	var target *RPCError
	if !errors.As(remote, &target) || remote.Error() != "OVERLOADED: queue full" {
		t.Fatal("RPC error classification")
	}
	if errors.As(ErrFrameTooLarge, &target) || errors.As(io.EOF, &target) {
		t.Fatal("local error classified as RPC error")
	}
}
