// Package client provides synchronous and asynchronous multiplexed RPC calls.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"rpc_handler/internal/protocol"
)

type RPCError = protocol.RPCError

// Response contains exactly one result or error. A CallAsync channel receives
// one terminal Response and then closes, even if the caller never receives it.
type Response struct {
	Result json.RawMessage
	Error  error
}

var ErrClosed = errors.New("RPC client is closed")
var ErrCapacity = errors.New("RPC client in-flight limit reached")

// Zero fields select defaults, except MaxRetries: zero disables retries.
type Options struct {
	MaxInFlight  int
	MaxRetries   int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	DialTimeout  time.Duration
	WriteTimeout time.Duration
}

func normalize(o Options) (Options, error) {
	if o.MaxInFlight < 0 || o.MaxRetries < 0 || o.MaxRetries > 2 || o.BaseBackoff < 0 || o.MaxBackoff < 0 || o.DialTimeout < 0 || o.WriteTimeout < 0 {
		return o, fmt.Errorf("invalid client options (MaxRetries must be 0..2)")
	}
	if o.MaxInFlight == 0 {
		o.MaxInFlight = 128
	}
	if o.BaseBackoff == 0 {
		o.BaseBackoff = 25 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 250 * time.Millisecond
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.BaseBackoff > o.MaxBackoff {
		return o, fmt.Errorf("BaseBackoff exceeds MaxBackoff")
	}
	return o, nil
}

type Client struct {
	address  string
	options  Options
	codec    protocol.Codec
	ctx      context.Context
	cancel   context.CancelFunc
	slots    chan struct{}
	dialGate chan struct{}
	mu       sync.Mutex
	session  *session
	nextID   atomic.Uint64
}

type session struct {
	conn    net.Conn
	writer  chan struct{}
	mu      sync.Mutex
	pending map[string]chan Response
	failed  error
}

func Dial(ctx context.Context, address string) (*Client, error) {
	return DialWithOptions(ctx, address, Options{})
}
func DialWithOptions(ctx context.Context, address string, options Options) (*Client, error) {
	options, err := normalize(options)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	c := &Client{address: address, options: options, ctx: lifetime, cancel: cancel, slots: make(chan struct{}, options.MaxInFlight), dialGate: make(chan struct{}, 1)}
	if _, err := c.connection(ctx); err != nil {
		cancel()
		return nil, err
	}
	return c, nil
}

func (c *Client) Close() error {
	c.cancel()
	c.mu.Lock()
	s := c.session
	c.mu.Unlock()
	if s != nil {
		s.fail(ErrClosed)
	}
	return nil
}

// CallAsync never waits for network I/O or a free capacity slot. Parameters are
// serialized before return; admitted calls use at most MaxInFlight goroutines.
func (c *Client) CallAsync(ctx context.Context, method string, params any) <-chan Response {
	out := make(chan Response, 1)
	finish := func(err error) <-chan Response { out <- Response{Error: err}; close(out); return out }
	if c.ctx.Err() != nil {
		return finish(ErrClosed)
	}
	if err := ctx.Err(); err != nil {
		return finish(err)
	}
	select {
	case c.slots <- struct{}{}:
	default:
		return finish(ErrCapacity)
	}
	data, err := json.Marshal(params)
	if err == nil {
		err = (protocol.Request{ID: "validate", Method: method, Params: data}).Validate()
	}
	if err != nil {
		<-c.slots
		return finish(err)
	}
	go func() {
		callCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(c.ctx, cancel)
		defer cancel()
		defer stop()
		response := c.run(callCtx, method, data)
		if c.ctx.Err() != nil {
			response = Response{Error: ErrClosed}
		}
		<-c.slots
		out <- response
		close(out)
	}()
	return out
}

func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	response := <-c.CallAsync(ctx, method, params)
	if response.Error != nil {
		return response.Error
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(response.Result, result)
}

func (c *Client) run(ctx context.Context, method string, data json.RawMessage) Response {
	delay := c.options.BaseBackoff
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return Response{Error: err}
		}
		response := c.attempt(ctx, method, data)
		if ctx.Err() != nil {
			return Response{Error: ctx.Err()}
		}
		if attempt >= c.options.MaxRetries || !transient(response.Error) {
			return response
		}
		// Jitter is in [half delay, delay), and delay never exceeds MaxBackoff.
		wait := delay/2 + time.Duration(rand.Int64N(int64(delay-delay/2)))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Response{Error: ctx.Err()}
		case <-timer.C:
		}
		if delay > c.options.MaxBackoff/2 {
			delay = c.options.MaxBackoff
		} else {
			delay *= 2
		}
	}
}

func transient(err error) bool {
	if err == nil {
		return false
	}
	var remote *RPCError
	if errors.As(err, &remote) || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) && (network.Timeout() || network.Temporary())
}

func (c *Client) connection(ctx context.Context) (*session, error) {
	select {
	case c.dialGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
	defer func() { <-c.dialGate }()
	c.mu.Lock()
	s := c.session
	c.mu.Unlock()
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if s != nil {
		s.mu.Lock()
		healthy := s.failed == nil
		s.mu.Unlock()
		if healthy {
			return s, nil
		}
	}
	dialCtx, cancel := context.WithTimeout(ctx, c.options.DialTimeout)
	stop := context.AfterFunc(c.ctx, cancel)
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", c.address)
	stop()
	cancel()
	if err != nil {
		return nil, err
	}
	s = &session{conn: conn, writer: make(chan struct{}, 1), pending: make(map[string]chan Response)}
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return nil, ErrClosed
	}
	c.session = s
	c.mu.Unlock()
	go c.readResponses(s)
	return s, nil
}

func (c *Client) attempt(ctx context.Context, method string, data json.RawMessage) Response {
	s, err := c.connection(ctx)
	if err != nil {
		return Response{Error: err}
	}
	id := strconv.FormatUint(c.nextID.Add(1), 10)
	result := make(chan Response, 1)
	s.mu.Lock()
	if s.failed != nil {
		err = s.failed
		s.mu.Unlock()
		return Response{Error: err}
	}
	s.pending[id] = result
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.pending, id); s.mu.Unlock() }()
	select {
	case s.writer <- struct{}{}:
	case response := <-result:
		return response
	case <-ctx.Done():
		return Response{Error: ctx.Err()}
	}
	if ctx.Err() != nil {
		<-s.writer
		return Response{Error: ctx.Err()}
	}
	deadline := time.Now().Add(c.options.WriteTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	err = s.conn.SetWriteDeadline(deadline)
	if err == nil {
		// Cancellation during a partial write requires closing this session: its
		// frame may be incomplete. Waiting-for-response cancellation does not.
		stopped := make(chan struct{})
		stop := context.AfterFunc(ctx, func() { s.fail(net.ErrClosed); close(stopped) })
		err = c.codec.WriteRequest(s.conn, protocol.Request{ID: id, Method: method, Params: data})
		if !stop() {
			<-stopped
		}
	}
	<-s.writer
	if err != nil {
		// These failures occur before any bytes are written and preserve framing.
		if !errors.Is(err, protocol.ErrFrameTooLarge) && !errors.Is(err, protocol.ErrInvalidMessage) {
			s.fail(err)
		}
		return Response{Error: err}
	}
	select {
	case response := <-result:
		return response
	case <-ctx.Done():
		return Response{Error: ctx.Err()}
	}
}

func (c *Client) readResponses(s *session) {
	for {
		response, err := c.codec.ReadResponse(s.conn)
		if err != nil {
			s.fail(err)
			return
		}
		s.mu.Lock()
		target := s.pending[response.ID]
		delete(s.pending, response.ID)
		if target != nil {
			if response.Error != nil {
				target <- Response{Error: response.Error}
			} else {
				target <- Response{Result: response.Result}
			}
		}
		s.mu.Unlock()
		// Unknown IDs include cancelled calls' late responses; safely ignore them.
	}
}

func (s *session) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return
	}
	s.failed = err
	_ = s.conn.Close()
	for id, target := range s.pending {
		target <- Response{Error: err}
		delete(s.pending, id)
	}
}
