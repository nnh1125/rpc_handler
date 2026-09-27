package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"rpc_handler/internal/protocol"
)

func TestMalformedResponseFailsPendingCalls(t *testing.T) {
	for _, payload := range []string{`{"id":`, `{"id":"1","result":{},"error":{"code":"X","message":"bad"}}`} {
		t.Run(payload, func(t *testing.T) {
			c, peer := pipeClient(t, 4)
			first := c.CallAsync(context.Background(), "first", struct{}{})
			read(t, peer)
			second := c.CallAsync(context.Background(), "second", struct{}{})
			read(t, peer)
			if err := (protocol.Codec{}).WriteFrame(peer, []byte(payload)); err != nil {
				t.Fatal(err)
			}
			for _, ch := range []<-chan Response{first, second} {
				if r := terminal(t, ch); !errors.Is(r.Error, protocol.ErrInvalidMessage) {
					t.Fatalf("malformed response: %+v", r)
				}
			}
			c.session.mu.Lock()
			pending := len(c.session.pending)
			c.session.mu.Unlock()
			if pending != 0 {
				t.Fatal("malformed response leaked pending calls")
			}
		})
	}
}

func TestResponseCancellationRaceCompletesOnce(t *testing.T) {
	c, peer := pipeClient(t, 4)
	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch := c.CallAsync(ctx, "race", struct{}{})
		request := read(t, peer)
		waitForWrite(t, c)
		start := make(chan struct{})
		written := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cancel() }()
		go func() {
			defer wg.Done()
			<-start
			written <- (protocol.Codec{}).WriteResponse(peer, protocol.Response{ID: request.ID, Result: json.RawMessage(`true`)})
		}()
		close(start)
		result := terminal(t, ch)
		if result.Error != nil && !errors.Is(result.Error, context.Canceled) {
			t.Fatal(result.Error)
		}
		if result.Error == nil && string(result.Result) != "true" {
			t.Fatal("incorrect success result")
		}
		wg.Wait()
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		c.session.mu.Lock()
		pending := len(c.session.pending)
		c.session.mu.Unlock()
		if pending != 0 || len(c.slots) != 0 {
			t.Fatal("completion leaked pending work")
		}
	}
}

func TestLostResponseRetryCannotConsumeOldOrOtherCallResult(t *testing.T) {
	oldID := make(chan string, 1)
	replacementReady := make(chan struct{})
	handlerErrors := make(chan error, 1)
	address := retryServer(t, func(conn net.Conn, n int) {
		codec := protocol.Codec{}
		first, err := codec.ReadRequest(conn)
		if err != nil {
			handlerErrors <- err
			return
		}
		if n == 0 {
			oldID <- first.ID
			return
		} // Drop the connection after accepting the first attempt.
		old := <-oldID
		if old == first.ID {
			handlerErrors <- fmt.Errorf("retry reused ID")
			return
		}
		// Inject an old ID on the replacement connection. It must not complete the
		// retry, nor a subsequent concurrent call on that connection.
		if err := codec.WriteResponse(conn, protocol.Response{ID: old, Result: json.RawMessage(`"stale"`)}); err != nil {
			handlerErrors <- err
			return
		}
		close(replacementReady)
		second, err := codec.ReadRequest(conn)
		if err != nil {
			handlerErrors <- err
			return
		}
		if err := codec.WriteResponse(conn, protocol.Response{ID: second.ID, Result: json.RawMessage(`"other"`)}); err != nil {
			handlerErrors <- err
			return
		}
		handlerErrors <- codec.WriteResponse(conn, protocol.Response{ID: first.ID, Result: json.RawMessage(`"retried"`)})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := DialWithOptions(ctx, address, Options{MaxRetries: 1, BaseBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	retry := c.CallAsync(ctx, "retry", struct{}{})
	select {
	case <-replacementReady:
	case <-ctx.Done():
		t.Fatal("retry did not reach replacement connection")
	}
	other := c.CallAsync(ctx, "other", struct{}{})
	if r := terminal(t, other); r.Error != nil || string(r.Result) != `"other"` {
		t.Fatalf("other = %+v", r)
	}
	if r := terminal(t, retry); r.Error != nil || string(r.Result) != `"retried"` {
		t.Fatalf("retry = %+v", r)
	}
	if err := <-handlerErrors; err != nil {
		t.Fatal(err)
	}
}
