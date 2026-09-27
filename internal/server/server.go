// Package server dispatches framed RPC requests over TCP.
package server

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"

	"rpc_handler/internal/protocol"
)

type Handler func(json.RawMessage) (any, *protocol.RPCError)

// Configure Codec before serving. Register rejects changes after serving starts.
// A Server is single-use; Shutdown permanently stops admission.
type Server struct {
	Codec        protocol.Codec
	handlers     map[string]Handler
	limits       Limits
	config       Config
	mu           sync.Mutex
	started      bool
	stopping     bool
	queue        chan job
	conns        map[*connection]struct{}
	listeners    map[net.Listener]struct{}
	workers      sync.WaitGroup
	requests     sync.WaitGroup
	readers      sync.WaitGroup
	shutdownOnce sync.Once
	shutdownDone chan struct{}
}

func New() *Server {
	s, _ := NewWithLimits(Limits{})
	return s
}

func NewWithLimits(limits Limits) (*Server, error) {
	return NewWithConfig(Config{Limits: limits})
}

func NewWithConfig(config Config) (*Server, error) {
	var err error
	config, err = normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	limits := config.Limits
	if limits.MaxDataBytes < 0 || limits.MaxSortValues < 0 || limits.MaxMatrixN < 0 {
		return nil, fmt.Errorf("limits must not be negative")
	}
	defaults := DefaultLimits()
	if limits.MaxDataBytes == 0 {
		limits.MaxDataBytes = defaults.MaxDataBytes
	}
	if limits.MaxSortValues == 0 {
		limits.MaxSortValues = defaults.MaxSortValues
	}
	if limits.MaxMatrixN == 0 {
		limits.MaxMatrixN = defaults.MaxMatrixN
	}
	if int64(limits.MaxDataBytes) == int64(^uint64(0)>>1) {
		return nil, fmt.Errorf("byte limit too large")
	}
	s := &Server{handlers: make(map[string]Handler), limits: limits, config: config, conns: make(map[*connection]struct{}), listeners: make(map[net.Listener]struct{}), shutdownDone: make(chan struct{})}
	s.handlers["hash_compute"] = s.hashHandler
	s.handlers["sort_array"] = s.sortHandler
	s.handlers["matrix_multiply"] = s.matrixHandler
	s.handlers["compress_data"] = s.compressHandler
	return s, nil
}

func (s *Server) Register(method string, handler Handler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopping {
		return fmt.Errorf("registration closed")
	}
	if strings.TrimSpace(method) == "" || handler == nil {
		return fmt.Errorf("method and handler are required")
	}
	if s.handlers == nil {
		s.handlers = make(map[string]Handler)
	}
	if _, exists := s.handlers[method]; exists {
		return fmt.Errorf("method %q already registered", method)
	}
	s.handlers[method] = handler
	return nil
}

func (s *Server) Dispatch(request protocol.Request) protocol.Response {
	response := protocol.Response{ID: request.ID}
	handler, ok := s.handlers[request.Method]
	if !ok {
		response.Error = &protocol.RPCError{Code: protocol.MethodNotFound, Message: "unknown method: " + request.Method}
		return response
	}
	result, rpcErr := handler(request.Params)
	if rpcErr != nil {
		response.Error = rpcErr
		return response
	}
	data, err := json.Marshal(result)
	if err != nil {
		response.Error = &protocol.RPCError{Code: protocol.InternalError, Message: "could not encode result"}
		return response
	}
	response.Result = data
	return response
}
