package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"rpc_handler/internal/protocol"
)

func testServer(t *testing.T, workers, queue int) *Server {
	t.Helper()
	s, err := NewWithConfig(Config{Workers: workers, QueueSize: queue, ReadTimeout: time.Second, WriteTimeout: 50 * time.Millisecond, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}
func pipe(t *testing.T, s *Server) net.Conn {
	t.Helper()
	peer, conn := net.Pipe()
	go func() { _ = s.ServeConn(conn) }()
	t.Cleanup(func() { _ = peer.Close() })
	_ = peer.SetDeadline(time.Now().Add(2 * time.Second))
	return peer
}
func send(t *testing.T, c net.Conn, id, method string) {
	t.Helper()
	if err := (protocol.Codec{}).WriteRequest(c, protocol.Request{ID: id, Method: method, Params: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
}
func receive(t *testing.T, c net.Conn) protocol.Response {
	t.Helper()
	r, err := (protocol.Codec{}).ReadResponse(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func await(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestOutOfOrderAndSerializedResponses(t *testing.T) {
	s := testServer(t, 2, 4)
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	_ = s.Register("slow", func(json.RawMessage) (any, *protocol.RPCError) { close(entered); <-release; return "slow", nil })
	_ = s.Register("fast", func(json.RawMessage) (any, *protocol.RPCError) { return "fast", nil })
	c := pipe(t, s)
	send(t, c, "1", "slow")
	await(t, entered)
	send(t, c, "2", "fast")
	if r := receive(t, c); r.ID != "2" {
		t.Fatalf("first response = %+v", r)
	}
	close(release)
	released = true
	if r := receive(t, c); r.ID != "1" {
		t.Fatalf("second response = %+v", r)
	}
}

func TestQueueOverloadAndDrain(t *testing.T) {
	s := testServer(t, 1, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	_ = s.Register("slow", func(json.RawMessage) (any, *protocol.RPCError) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return true, nil
	})
	c := pipe(t, s)
	send(t, c, "1", "slow")
	await(t, entered)
	send(t, c, "2", "slow")
	send(t, c, "3", "slow")
	if r := receive(t, c); r.ID != "3" || r.Error == nil || r.Error.Code != protocol.Overloaded {
		t.Fatalf("overload = %+v", r)
	}
	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		drained <- s.Shutdown(ctx)
	}()
	close(release)
	released = true
	for _, id := range []string{"1", "2"} {
		if r := receive(t, c); r.ID != id || r.Error != nil {
			t.Fatalf("drain = %+v", r)
		}
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}

func TestSlowReaderReleasesWorker(t *testing.T) {
	s := testServer(t, 1, 4)
	entered := make(chan struct{})
	_ = s.Register("ok", func(json.RawMessage) (any, *protocol.RPCError) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		return true, nil
	})
	slow := pipe(t, s)
	send(t, slow, "1", "ok")
	await(t, entered)
	fast := pipe(t, s)
	send(t, fast, "2", "ok")
	if r := receive(t, fast); r.ID != "2" || r.Error != nil {
		t.Fatalf("worker not released: %+v", r)
	}
}

func TestShutdownDeadlineAndAdmission(t *testing.T) {
	s := testServer(t, 1, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	_ = s.Register("slow", func(json.RawMessage) (any, *protocol.RPCError) { close(entered); <-release; return true, nil })
	c := pipe(t, s)
	send(t, c, "1", "slow")
	await(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown = %v", err)
	}
	if _, err := (protocol.Codec{}).ReadResponse(c); err == nil {
		t.Fatal("connection stayed open")
	}
	peer, conn := net.Pipe()
	defer peer.Close()
	if err := s.ServeConn(conn); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("admission = %v", err)
	}
}

func TestConnectionLimitAndReadDeadline(t *testing.T) {
	s := testServer(t, 1, 1)
	s.config.MaxConnections = 1
	s.config.ReadTimeout = 20 * time.Millisecond
	peer, conn := net.Pipe()
	defer peer.Close()
	admitted, err := s.admit(conn)
	if err != nil {
		t.Fatal(err)
	}
	extra, other := net.Pipe()
	defer extra.Close()
	if err := s.ServeConn(other); !errors.Is(err, ErrConnectionLimit) {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = s.readLoop(admitted); close(done) }()
	await(t, done)
	if err := s.Register("late", func(json.RawMessage) (any, *protocol.RPCError) { return nil, nil }); err == nil {
		t.Fatal("late registration allowed")
	}
}

func TestShutdownClosesListener(t *testing.T) {
	s := testServer(t, 1, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// A completed request ensures Serve has registered the listener.
	if err := (protocol.Codec{}).WriteRequest(conn, protocol.Request{ID: "1", Method: "hash_compute", Params: json.RawMessage(`{"data":""}`)}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	receive(t, conn)
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentResponseFramesRemainIntact(t *testing.T) {
	s := testServer(t, 4, 64)
	// Larger frames exercise interleaving of headers and payloads if write locking
	// is removed. Allow enough time for race-instrumented CI runs.
	s.config.WriteTimeout = time.Second
	payload := make([]int, 2048)
	_ = s.Register("large", func(json.RawMessage) (any, *protocol.RPCError) { return payload, nil })
	c := pipe(t, s)
	sent := make(chan error, 1)
	go func() {
		for i := 0; i < 32; i++ {
			if err := (protocol.Codec{}).WriteRequest(c, protocol.Request{ID: fmt.Sprint(i), Method: "large", Params: json.RawMessage(`{}`)}); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	seen := make(map[string]bool)
	for i := 0; i < 32; i++ {
		r := receive(t, c)
		var values []int
		if r.Error != nil || seen[r.ID] || json.Unmarshal(r.Result, &values) != nil || len(values) != len(payload) {
			t.Fatalf("corrupt/duplicate response: %+v", r)
		}
		seen[r.ID] = true
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
}

func TestMalformedJSONClosesConnection(t *testing.T) {
	s := testServer(t, 1, 1)
	c := pipe(t, s)
	if err := (protocol.Codec{}).WriteFrame(c, []byte(`{"id":`)); err != nil {
		t.Fatal(err)
	}
	if _, err := (protocol.Codec{}).ReadResponse(c); err == nil {
		t.Fatal("malformed JSON did not close connection")
	}
}
