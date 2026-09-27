package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"rpc_handler/internal/protocol"
)

func pipeClient(t *testing.T, capacity int) (*Client, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	options, _ := normalize(Options{MaxInFlight: capacity, WriteTimeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	s := &session{conn: local, writer: make(chan struct{}, 1), pending: make(map[string]chan Response)}
	c := &Client{options: options, ctx: ctx, cancel: cancel, slots: make(chan struct{}, capacity), dialGate: make(chan struct{}, 1), session: s}
	go c.readResponses(s)
	_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
	t.Cleanup(func() { _ = c.Close(); _ = peer.Close() })
	return c, peer
}
func terminal(t *testing.T, ch <-chan Response) Response {
	t.Helper()
	select {
	case r, ok := <-ch:
		if !ok {
			t.Fatal("channel closed without response")
		}
		select {
		case _, open := <-ch:
			if open {
				t.Fatal("more than one terminal response")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("terminal channel did not close")
		}
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("call did not finish")
		return Response{}
	}
}
func read(t *testing.T, conn net.Conn) protocol.Request {
	t.Helper()
	r, err := (protocol.Codec{}).ReadRequest(conn)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func reply(t *testing.T, conn net.Conn, id, value string) {
	t.Helper()
	if err := (protocol.Codec{}).WriteResponse(conn, protocol.Response{ID: id, Result: json.RawMessage(value)}); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncRoutesOutOfOrder(t *testing.T) {
	c, peer := pipeClient(t, 8)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := c.CallAsync(ctx, "first", struct{}{})
	r1 := read(t, peer)
	second := c.CallAsync(ctx, "second", struct{}{})
	r2 := read(t, peer)
	reply(t, peer, r2.ID, `"second"`)
	if got := terminal(t, second); got.Error != nil || string(got.Result) != `"second"` {
		t.Fatal(got)
	}
	select {
	case <-first:
		t.Fatal("first completed early")
	default:
	}
	reply(t, peer, r1.ID, `"first"`)
	if got := terminal(t, first); got.Error != nil || string(got.Result) != `"first"` {
		t.Fatal(got)
	}
}

func TestCancelWaitingResponseIgnoresLateResult(t *testing.T) {
	c, peer := pipeClient(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	first := c.CallAsync(ctx, "first", struct{}{})
	r1 := read(t, peer)
	waitForWrite(t, c)
	cancel()
	if got := terminal(t, first); !errors.Is(got.Error, context.Canceled) {
		t.Fatal(got)
	}
	c.session.mu.Lock()
	pending := len(c.session.pending)
	c.session.mu.Unlock()
	if pending != 0 {
		t.Fatal("cancelled request leaked")
	}
	second := c.CallAsync(context.Background(), "second", struct{}{})
	r2 := read(t, peer)
	reply(t, peer, r1.ID, `"late"`)
	reply(t, peer, r2.ID, `"current"`)
	if got := terminal(t, second); got.Error != nil || string(got.Result) != `"current"` {
		t.Fatal(got)
	}
}

func TestBoundedCallsAndCancelWhileQueued(t *testing.T) {
	c, _ := pipeClient(t, 2)
	// Hold the writer gate so both calls wait for permission to send.
	c.session.writer <- struct{}{}
	// CallAsync still returns immediately; cancellation can remove queued work.
	first := c.CallAsync(context.Background(), "first", struct{}{})
	ctx, cancel := context.WithCancel(context.Background())
	second := c.CallAsync(ctx, "second", struct{}{})
	if got := terminal(t, c.CallAsync(context.Background(), "full", struct{}{})); !errors.Is(got.Error, ErrCapacity) {
		t.Fatal(got)
	}
	cancel()
	if got := terminal(t, second); !errors.Is(got.Error, context.Canceled) {
		t.Fatal(got)
	}
	_ = c.Close()
	if got := terminal(t, first); !errors.Is(got.Error, ErrClosed) {
		t.Fatal(got)
	}
	if got := terminal(t, c.CallAsync(context.Background(), "closed", struct{}{})); !errors.Is(got.Error, ErrClosed) {
		t.Fatal(got)
	}
}

func TestDisconnectFailsAllPending(t *testing.T) {
	c, peer := pipeClient(t, 8)
	var calls []<-chan Response
	for i := 0; i < 4; i++ {
		calls = append(calls, c.CallAsync(context.Background(), "x", struct{}{}))
		read(t, peer)
	}
	_ = peer.Close()
	for _, call := range calls {
		if got := terminal(t, call); got.Error == nil {
			t.Fatal("disconnect succeeded")
		}
	}
	c.session.mu.Lock()
	pending := len(c.session.pending)
	c.session.mu.Unlock()
	if pending != 0 {
		t.Fatal("pending calls leaked")
	}
}

func retryServer(t *testing.T, handle func(net.Conn, int)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; ; n++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				handle(conn, n)
			}(n)
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done; wg.Wait() })
	return listener.Addr().String()
}

func TestRetryLostResponseUsesNewID(t *testing.T) {
	ids := make(chan string, 3)
	address := retryServer(t, func(conn net.Conn, n int) {
		r, err := (protocol.Codec{}).ReadRequest(conn)
		if err != nil {
			return
		}
		ids <- r.ID
		if n == 0 {
			return
		} // Computation could have happened; response was lost.
		_ = (protocol.Codec{}).WriteResponse(conn, protocol.Response{ID: r.ID, Result: json.RawMessage(`"retried"`)})
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := DialWithOptions(ctx, address, Options{MaxRetries: 2, BaseBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var result string
	if err := c.Call(ctx, "x", struct{}{}, &result); err != nil || result != "retried" {
		t.Fatalf("result %q: %v", result, err)
	}
	if first, second := <-ids, <-ids; first == second {
		t.Fatal("retry reused ID")
	}
}

func TestRetryLimitAndApplicationErrors(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport", true: "overload"}[remote], func(t *testing.T) {
			attempts := make(chan struct{}, 8)
			address := retryServer(t, func(conn net.Conn, _ int) {
				r, err := (protocol.Codec{}).ReadRequest(conn)
				if err != nil {
					return
				}
				attempts <- struct{}{}
				if remote {
					_ = (protocol.Codec{}).WriteResponse(conn, protocol.Response{ID: r.ID, Error: &protocol.RPCError{Code: protocol.Overloaded, Message: "full"}})
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			c, err := DialWithOptions(ctx, address, Options{MaxRetries: 2, BaseBackoff: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.Call(ctx, "x", struct{}{}, nil); err == nil {
				t.Fatal("expected failure")
			}
			want := 3
			if remote {
				want = 1
			}
			if len(attempts) != want {
				t.Fatalf("attempts=%d want %d", len(attempts), want)
			}
		})
	}
}

func TestRetryBackoffRespectsOriginalDeadline(t *testing.T) {
	address := retryServer(t, func(conn net.Conn, _ int) { _, _ = (protocol.Codec{}).ReadRequest(conn) })
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	c, err := DialWithOptions(ctx, address, Options{MaxRetries: 2, BaseBackoff: time.Second, MaxBackoff: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Call(ctx, "x", struct{}{}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
}

func TestCancellationDuringPartialWrite(t *testing.T) {
	c, peer := pipeClient(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := c.CallAsync(ctx, "blocked", struct{}{})
	var firstByte [1]byte
	if _, err := peer.Read(firstByte[:]); err != nil {
		t.Fatal(err)
	} // Frame header is now partially written.
	cancel()
	if got := terminal(t, call); !errors.Is(got.Error, context.Canceled) {
		t.Fatal(got)
	}
	if _, err := peer.Read(firstByte[:]); err == nil {
		t.Fatal("partial frame connection remained open")
	}
}

func TestReconnectForLaterCallWithRetriesDisabled(t *testing.T) {
	address := retryServer(t, func(conn net.Conn, n int) {
		r, err := (protocol.Codec{}).ReadRequest(conn)
		if err != nil {
			return
		}
		if n == 0 {
			return
		}
		_ = (protocol.Codec{}).WriteResponse(conn, protocol.Response{ID: r.ID, Result: json.RawMessage(`true`)})
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := Dial(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Call(ctx, "x", struct{}{}, nil); err == nil {
		t.Fatal("first call should fail without retry")
	}
	var result bool
	if err := c.Call(ctx, "x", struct{}{}, &result); err != nil || !result {
		t.Fatalf("reconnect = %t, %v", result, err)
	}
}

func TestInvalidOptionsAndLocalParameters(t *testing.T) {
	for _, o := range []Options{{MaxRetries: 3}, {MaxInFlight: -1}, {WriteTimeout: -1}, {BaseBackoff: time.Second, MaxBackoff: time.Millisecond}} {
		if _, err := normalize(o); err == nil {
			t.Fatalf("accepted %+v", o)
		}
	}
	c, _ := pipeClient(t, 1)
	if r := terminal(t, c.CallAsync(context.Background(), "", struct{}{})); !errors.Is(r.Error, protocol.ErrInvalidMessage) {
		t.Fatal(r)
	}
	if len(c.slots) != 0 {
		t.Fatal("invalid call consumed capacity")
	}
}

// A peer reading a full frame does not prove WriteRequest has returned yet.
// Synchronize with the writer before testing cancellation while awaiting reply.
func waitForWrite(t *testing.T, c *Client) {
	t.Helper()
	select {
	case c.session.writer <- struct{}{}:
		<-c.session.writer
	case <-time.After(time.Second):
		t.Fatal("writer did not finish")
	}
}
