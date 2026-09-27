package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"time"

	"rpc_handler/internal/protocol"
)

type Config struct {
	Limits         Limits
	Workers        int
	QueueSize      int
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	Verbose        bool
	Logger         *log.Logger
}

func DefaultConfig() Config {
	return Config{Limits: DefaultLimits(), Workers: runtime.GOMAXPROCS(0), QueueSize: 128, MaxConnections: 128, ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Second}
}
func normalizeConfig(c Config) (Config, error) {
	if c.Workers < 0 || c.QueueSize < 0 || c.MaxConnections < 0 || c.ReadTimeout < 0 || c.WriteTimeout < 0 {
		return c, fmt.Errorf("server settings must not be negative")
	}
	d := DefaultConfig()
	if c.Workers == 0 {
		c.Workers = d.Workers
	}
	if c.QueueSize == 0 {
		c.QueueSize = d.QueueSize
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = d.MaxConnections
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = d.ReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = d.WriteTimeout
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
	return c, nil
}

var ErrShuttingDown = errors.New("server shutting down")
var ErrConnectionLimit = errors.New("server connection limit reached")

type connection struct {
	net.Conn
	writeMu sync.Mutex
}
type job struct {
	conn    *connection
	request protocol.Request
	started time.Time
}

// startLocked initializes the shared pool; s.mu must be held.
func (s *Server) startLocked() {
	if s.started {
		return
	}
	s.started = true
	s.queue = make(chan job, s.config.QueueSize)
	for i := 0; i < s.config.Workers; i++ {
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			for j := range s.queue {
				response := s.Dispatch(j.request)
				_ = s.reply(j.conn, j.request, response, j.started)
				s.requests.Done()
			}
		}()
	}
}

func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		_ = listener.Close()
		return ErrShuttingDown
	}
	s.startLocked()
	s.listeners[listener] = struct{}{}
	s.mu.Unlock()
	defer func() { _ = listener.Close(); s.mu.Lock(); delete(s.listeners, listener); s.mu.Unlock() }()
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopping := s.stopping
			s.mu.Unlock()
			if stopping {
				return nil
			}
			return err
		}
		c, err := s.admit(conn)
		if err != nil {
			_ = conn.Close()
			continue
		}
		go func() { _ = s.readLoop(c) }()
	}
}

func (s *Server) admit(conn net.Conn) (*connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return nil, ErrShuttingDown
	}
	if len(s.conns) >= s.config.MaxConnections {
		return nil, ErrConnectionLimit
	}
	s.startLocked()
	c := &connection{Conn: conn}
	s.conns[c] = struct{}{}
	s.readers.Add(1)
	return c, nil
}

// ServeConn owns conn. Call Shutdown when the server is no longer needed.
func (s *Server) ServeConn(conn net.Conn) error {
	c, err := s.admit(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	return s.readLoop(c)
}

func (s *Server) readLoop(c *connection) error {
	defer s.readers.Done()
	defer func() {
		s.mu.Lock()
		// Shutdown keeps connections open until accepted responses are delivered.
		if !s.stopping {
			delete(s.conns, c)
			_ = c.Close()
		}
		s.mu.Unlock()
	}()
	for {
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return nil
		}
		err := c.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		s.mu.Unlock()
		if err != nil {
			return err
		}
		request, err := s.Codec.ReadRequest(c)
		started := time.Now()
		if err != nil {
			if !errors.Is(err, protocol.ErrInvalidMessage) || request.ID == "" {
				if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					s.config.Logger.Printf("read peer=%s error=%v", c.RemoteAddr(), err)
				}
				return err
			}
			if err := s.reply(c, request, errorResponse(request.ID, protocol.InvalidRequest, err.Error()), started); err != nil {
				return err
			}
			continue
		}
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return nil
		}
		_, known := s.handlers[request.Method]
		if !known {
			s.mu.Unlock()
			if err := s.reply(c, request, errorResponse(request.ID, protocol.MethodNotFound, "unknown method: "+request.Method), started); err != nil {
				return err
			}
			continue
		}
		s.requests.Add(1)
		select {
		case s.queue <- job{c, request, started}:
			s.mu.Unlock()
		default:
			s.requests.Done()
			s.mu.Unlock()
			if err := s.reply(c, request, errorResponse(request.ID, protocol.Overloaded, "work queue is full"), started); err != nil {
				return err
			}
		}
	}
}

func errorResponse(id string, code protocol.Code, message string) protocol.Response {
	return protocol.Response{ID: id, Error: &protocol.RPCError{Code: code, Message: message}}
}

func (s *Server) reply(c *connection, request protocol.Request, response protocol.Response, started time.Time) error {
	c.writeMu.Lock()
	// Set the deadline only after owning the writer so another response cannot
	// extend the deadline of a blocked write. Each holder has bounded write time.
	err := c.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	if err == nil {
		err = s.Codec.WriteResponse(c, response)
	}
	if errors.Is(err, protocol.ErrFrameTooLarge) {
		response = errorResponse(request.ID, protocol.ResourceExhausted, "response exceeds frame limit")
		err = s.Codec.WriteResponse(c, response)
	}
	if err != nil {
		_ = c.Close()
	}
	c.writeMu.Unlock()
	code := "OK"
	if response.Error != nil {
		code = string(response.Error.Code)
	}
	if s.config.Verbose || response.Error != nil || err != nil {
		s.config.Logger.Printf("request id=%q method=%q duration=%s code=%s write_error=%v", request.ID, request.Method, time.Since(started), code, err)
	}
	return err
}

// Shutdown stops admission and drains accepted work. On context expiry it
// closes connections and returns; an already executing handler is not forcibly
// interrupted. Workers exit once those handlers and queued work finish.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		for listener := range s.listeners {
			_ = listener.Close()
		}
		for c := range s.conns {
			_ = c.SetReadDeadline(time.Now())
		}
		if s.started {
			close(s.queue)
		}
		s.mu.Unlock()
		go func() {
			s.requests.Wait()
			s.workers.Wait()
			s.closeConnections()
			s.readers.Wait()
			close(s.shutdownDone)
		}()
	})
	select {
	case <-s.shutdownDone:
		return nil
	case <-ctx.Done():
		s.closeConnections()
		return ctx.Err()
	}
}
func (s *Server) closeConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
		delete(s.conns, c)
	}
}
