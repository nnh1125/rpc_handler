package server

import (
	"context"
	"encoding/json"
	"net"
	"rpc_handler/internal/protocol"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {
	s := New()
	handler := func(json.RawMessage) (any, *protocol.RPCError) {
		return struct {
			OK bool `json:"ok"`
		}{true}, nil
	}
	if err := s.Register("custom", handler); err != nil {
		t.Fatal(err)
	}
	if err := s.Register("custom", handler); err == nil {
		t.Fatal("accepted duplicate")
	}
	if err := s.Register("", handler); err == nil {
		t.Fatal("accepted empty method")
	}
	if err := s.Register("nil", nil); err == nil {
		t.Fatal("accepted nil handler")
	}
	response := s.Dispatch(protocol.Request{ID: "1", Method: "custom", Params: json.RawMessage(`{}`)})
	if string(response.Result) != `{"ok":true}` || response.ID != "1" {
		t.Fatalf("response = %+v", response)
	}
}

func TestInvalidRequestThenValidRequest(t *testing.T) {
	peer, conn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv := New()
		defer srv.Shutdown(context.Background())
		_ = srv.ServeConn(conn)
	}()
	defer func() { peer.Close(); <-done }()
	_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
	codec := protocol.Codec{}
	if err := codec.WriteFrame(peer, []byte(`{"id":"bad","method":false,"params":{}}`)); err != nil {
		t.Fatal(err)
	}
	response, err := codec.ReadResponse(peer)
	if err != nil || response.ID != "bad" || response.Error == nil || response.Error.Code != protocol.InvalidRequest {
		t.Fatalf("response = %+v, %v", response, err)
	}
	if err := codec.WriteRequest(peer, protocol.Request{ID: "good", Method: "hash_compute", Params: json.RawMessage(`{"data":""}`)}); err != nil {
		t.Fatal(err)
	}
	response, err = codec.ReadResponse(peer)
	if err != nil || response.ID != "good" || response.Error != nil {
		t.Fatalf("response = %+v, %v", response, err)
	}
}
