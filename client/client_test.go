package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"rpc_handler/client"
	"rpc_handler/internal/operations"
	"rpc_handler/internal/protocol"
	"rpc_handler/internal/server"
)

// Each test owns one real TCP connection and waits for its server to exit.
func connect(t *testing.T, serve func(net.Conn)) *client.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			serve(conn)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Close()
		listener.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	})
	return c
}

func TestHashOverTCPAndConnectionReuse(t *testing.T) {
	c := connect(t, func(conn net.Conn) {
		srv := server.New()
		defer srv.Shutdown(context.Background())
		_ = srv.ServeConn(conn)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tc := range []struct{ input, want string }{
		{"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	} {
		var result operations.HashResult
		if err := c.Call(ctx, "hash_compute", operations.HashParams{Data: []byte(tc.input)}, &result); err != nil {
			t.Fatal(err)
		}
		if result.Hex != tc.want {
			t.Errorf("hash(%q) = %q", tc.input, result.Hex)
		}
	}
	for _, tc := range []struct {
		method, params string
		code           protocol.Code
	}{
		{"missing", `{}`, protocol.MethodNotFound},
		{"hash_compute", `{}`, protocol.InvalidArgument},
		{"hash_compute", `{"data":null}`, protocol.InvalidArgument},
		{"hash_compute", `{"data":[1,2]}`, protocol.InvalidArgument},
		{"hash_compute", `{"data":"!"}`, protocol.InvalidArgument},
	} {
		err := c.Call(ctx, tc.method, json.RawMessage(tc.params), nil)
		var rpcErr *client.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != tc.code {
			t.Fatalf("error = %v, want %s", err, tc.code)
		}
	}
	if err := c.Call(ctx, "hash_compute", operations.HashParams{Data: []byte("abc")}, nil); err != nil {
		t.Fatalf("connection unusable after RPC errors: %v", err)
	}
}

func TestMismatchedResponseID(t *testing.T) {
	c := connect(t, func(conn net.Conn) {
		codec := protocol.Codec{}
		if _, err := codec.ReadRequest(conn); err == nil {
			_ = codec.WriteResponse(conn, protocol.Response{ID: "wrong", Result: json.RawMessage(`{}`)})
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Call(ctx, "hash_compute", operations.HashParams{Data: []byte{}}, nil); err == nil {
		t.Fatal("accepted wrong response ID")
	}
}

func TestCancelCall(t *testing.T) {
	received := make(chan struct{})
	c := connect(t, func(conn net.Conn) {
		_, _ = (protocol.Codec{}).ReadRequest(conn)
		close(received)
		var b [1]byte
		_, _ = conn.Read(b[:]) // Wait for test cleanup to close the shared connection.
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Call(ctx, "hash_compute", operations.HashParams{Data: []byte{}}, nil) }()
	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("request not received")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not unblock call")
	}
}
