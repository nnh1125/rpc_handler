package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"rpc_handler/client"
	"rpc_handler/internal/protocol"
	"rpc_handler/internal/server"
)

func TestConcurrentClientsThroughSharedWorkerPool(t *testing.T) {
	srv, err := server.NewWithConfig(server.Config{Workers: 4, QueueSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := srv.Register("echo", func(raw json.RawMessage) (any, *protocol.RPCError) { return raw, nil }); err != nil {
		t.Fatal(err)
	}
	clients := []*client.Client{
		connect(t, func(conn net.Conn) { _ = srv.ServeConn(conn) }),
		connect(t, func(conn net.Conn) { _ = srv.ServeConn(conn) }),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := make(chan struct{})
	failures := make(chan error, 32)
	var wg sync.WaitGroup
	for index, c := range clients {
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(c *client.Client, value string) {
				defer wg.Done()
				<-start
				params := struct {
					Value string `json:"value"`
				}{value}
				var result struct {
					Value string `json:"value"`
				}
				if err := c.Call(ctx, "echo", params, &result); err != nil {
					failures <- err
					return
				}
				if result.Value != value {
					failures <- fmt.Errorf("got %q want %q", result.Value, value)
				}
			}(c, fmt.Sprintf("client-%d-call-%d", index, i))
		}
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}
