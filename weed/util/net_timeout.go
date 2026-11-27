package util

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// Listener wraps a net.Listener, and gives a place to store the timeout
// parameters. On Accept, it will wrap the net.Conn with our own Conn for us.
type Listener struct {
	net.Listener
	Timeout time.Duration
}

func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	stats.ConnectionOpen()
	tc := &Conn{
		Conn:    c,
		Timeout: l.Timeout,
	}
	return tc, nil
}

// Conn wraps a net.Conn and implements a "no activity timeout".
// Any activity (read or write) resets the deadline, so the connection
// only times out when there's no activity in either direction.
//
// Callers may also set deadlines directly — net/http's server does this
// to interrupt a blocked read (abortPendingRead sets a deadline in the
// past) and to enforce its own Read/Write timeouts. The activity
// extension must never overwrite such an externally-set deadline, or the
// interrupt is lost and the read stays blocked until the activity
// timeout fires (wedging connection drain on shutdown for up to the full
// idle timeout). mu serializes deadline updates, and while an external
// deadline is in force on a direction the activity extension for that
// direction is suspended; it resumes once the caller clears the deadline
// (sets the zero time).
//
// While a handler works on a request it has read in full, the read side is
// out of the timeout as well, see PauseIdleTimeoutWhileHandling.
type Conn struct {
	net.Conn
	Timeout  time.Duration
	isClosed bool

	mu                    sync.Mutex
	externalReadDeadline  bool
	externalWriteDeadline bool
	// handling counts the requests on this connection whose body has been
	// read to its end and whose handler has not returned yet.
	handling int

	readEndedByServer atomic.Bool
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.externalReadDeadline = !t.IsZero()
	c.externalWriteDeadline = !t.IsZero()
	return c.Conn.SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.externalReadDeadline = !t.IsZero()
	return c.Conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.externalWriteDeadline = !t.IsZero()
	return c.Conn.SetWriteDeadline(t)
}

// extendDeadline extends the connection deadline from now.
// This implements "no activity timeout" - any activity keeps the
// connection alive in both directions (a long write-only response must
// keep the read deadline alive too, or net/http's background read would
// time out and cancel the in-flight request). Each direction is extended
// independently: an externally-managed deadline on one side (e.g. a
// server WriteTimeout) must not leave the other side's deadline stale.
// While a handler works on a request it has read in full, the read side
// keeps no deadline (see pauseIdleTimeout).
func (c *Conn) extendDeadline() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Timeout <= 0 {
		return nil
	}
	extendRead := !c.externalReadDeadline && c.handling == 0
	extendWrite := !c.externalWriteDeadline
	deadline := time.Now().Add(c.Timeout)
	switch {
	case extendRead && extendWrite:
		return c.Conn.SetDeadline(deadline)
	case extendRead:
		return c.Conn.SetReadDeadline(deadline)
	case extendWrite:
		return c.Conn.SetWriteDeadline(deadline)
	}
	return nil
}

// pauseIdleTimeout takes reading out of the activity timeout while a handler
// works on a request it has read in full, and lifts the deadline that the
// read already waiting (net/http's background read) got from it.
func (c *Conn) pauseIdleTimeout() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handling++
	if c.handling == 1 && c.Timeout > 0 && !c.externalReadDeadline {
		_ = c.Conn.SetReadDeadline(time.Time{})
	}
}

// resumeIdleTimeout puts reading back under the activity timeout once the
// last such handler has returned, including a read that is already waiting.
func (c *Conn) resumeIdleTimeout() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handling--
	if c.handling == 0 && c.Timeout > 0 && !c.externalReadDeadline {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	}
}

func (c *Conn) Read(b []byte) (count int, e error) {
	// Extend deadline before reading - any activity keeps connection alive
	if e = c.extendDeadline(); e == nil {
		if count, e = c.Conn.Read(b); e == nil {
			stats.BytesIn(int64(count))
		}
	}
	c.readEndedByServer.Store(errors.Is(e, os.ErrDeadlineExceeded) || errors.Is(e, net.ErrClosed))
	return
}

func (c *Conn) Write(b []byte) (count int, e error) {
	// Extend deadline before writing - any activity keeps connection alive
	if err := c.extendDeadline(); err != nil {
		return 0, err
	}
	count, e = c.Conn.Write(b)
	if e == nil {
		stats.BytesOut(int64(count))
	}
	return
}

func (c *Conn) Close() error {
	err := c.Conn.Close()
	if err == nil {
		if !c.isClosed {
			stats.ConnectionClose()
			c.isClosed = true
		}
	}
	return err
}

type connContextKey struct{}

// ConnContext is an http.Server.ConnContext hook that lets ClientGone and
// PauseIdleTimeoutWhileHandling see the connection serving a request.
func ConnContext(ctx context.Context, c net.Conn) context.Context {
	if tlsConn, ok := c.(interface{ NetConn() net.Conn }); ok {
		c = tlsConn.NetConn()
	}
	if tc, ok := c.(*Conn); ok {
		return context.WithValue(ctx, connContextKey{}, tc)
	}
	return ctx
}

// ClientGone reports whether the client is why the request context ctx is
// canceled. net/http cancels it when a read of the connection fails or the
// HTTP/2 stream is reset; a read the server ended itself, by the idle timeout
// or by closing the connection, is not the client's doing.
func ClientGone(ctx context.Context) bool {
	tc, _ := ctx.Value(connContextKey{}).(*Conn)
	return tc != nil && ctx.Err() == context.Canceled && !tc.readEndedByServer.Load()
}

// ConnFromContext returns the connection serving a request, as ConnContext
// recorded it, or nil.
func ConnFromContext(ctx context.Context) net.Conn {
	if tc, ok := ctx.Value(connContextKey{}).(*Conn); ok {
		return tc
	}
	return nil
}

// PauseIdleTimeoutWhileHandling wraps the handler of an http.Server that
// serves a Listener and has ConnContext as its ConnContext hook. From the
// moment a request's body has been read to its end (at once when it has
// none) until its handler returns, the connection's activity timeout no
// longer applies to reading: the client is waiting for the answer, so a
// connection that carries nothing means the server is still at work, not
// that the client is idle. Reading a body, writing an answer and waiting
// for the next request stay under the timeout.
//
// Without it, a request the server works on for longer than the timeout
// without sending a byte (a large CopyObject into a versioned bucket, which
// answers only once the copy is done) lets the timeout end net/http's
// background read, the read the server keeps waiting while a handler runs,
// to see a client hang up. net/http takes that as the client's connection
// failing and cancels the connection's context: the request in progress,
// and every later request the client sends on the kept-alive connection,
// then run with a canceled context.
//
// Only HTTP/1.x requests are paused. HTTP/2 has no background read: when
// its connection carries nothing for the timeout, the frame reader's
// deadline closes the connection, and no later request runs on it. That
// deadline is also what ends a stream whose client stopped reading, since
// such a stream waits for flow-control credit, not on the socket.
func PauseIdleTimeoutWhileHandling(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, _ := r.Context().Value(connContextKey{}).(*Conn)
		if tc == nil || tc.Timeout <= 0 || r.ProtoMajor != 1 {
			h.ServeHTTP(w, r)
			return
		}
		pause := &idleTimeoutPause{conn: tc}
		defer pause.end()
		if r.Body == nil || r.Body == http.NoBody {
			pause.begin()
		} else {
			// net/http starts the background read in the same body read
			// that returns io.EOF. WithContext gives a shallow copy, so the
			// Request the server passed in stays as it is. net/http removes
			// the temporary files of a multipart form parsed on that Request
			// only, so remove those of one parsed on the copy.
			withBody := r.WithContext(r.Context())
			withBody.Body = &pauseAtBodyEnd{ReadCloser: r.Body, pause: pause}
			defer func(orig *http.Request) {
				if withBody.MultipartForm != nil && withBody.MultipartForm != orig.MultipartForm {
					_ = withBody.MultipartForm.RemoveAll()
				}
			}(r)
			r = withBody
		}
		h.ServeHTTP(w, r)
	})
}

// idleTimeoutPause holds a connection's activity timeout off reading for
// one request, from begin until end, at most once.
type idleTimeoutPause struct {
	conn *Conn

	mu     sync.Mutex
	paused bool
	ended  bool
}

func (p *idleTimeoutPause) begin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused || p.ended {
		return
	}
	p.paused = true
	p.conn.pauseIdleTimeout()
}

func (p *idleTimeoutPause) end() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ended = true
	if p.paused {
		p.paused = false
		p.conn.resumeIdleTimeout()
	}
}

// pauseAtBodyEnd begins the pause when the request body has been read to
// its end.
type pauseAtBodyEnd struct {
	io.ReadCloser
	pause *idleTimeoutPause
}

func (b *pauseAtBodyEnd) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.pause.begin()
	}
	return n, err
}

func NewListener(addr string, timeout time.Duration) (ipListener net.Listener, err error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	ipListener = &Listener{
		Listener: listener,
		Timeout:  timeout,
	}

	return
}

// IsLoopbackOrWildcardAddress reports whether host already covers loopback
// traffic — empty (all interfaces), a wildcard address, or a loopback address —
// in which case a separate localhost listener is unnecessary.
func IsLoopbackOrWildcardAddress(host string) bool {
	switch host {
	case "", "localhost", "0.0.0.0", "127.0.0.1", "[::]", "[::1]":
		return true
	}
	return false
}

// NewIpAndLocalListeners binds host:port and, unless listenOnLocalhost is given
// as false, a second listener on localhost:port so that local clients still
// reach the service when it binds a specific address. Callers pass false when
// the user explicitly chose the bind address (e.g. -ip.bind), honoring exactly
// that address.
func NewIpAndLocalListeners(host string, port int, timeout time.Duration, listenOnLocalhost ...bool) (ipListener net.Listener, localListener net.Listener, err error) {
	listener, err := net.Listen("tcp", JoinHostPort(host, port))
	if err != nil {
		return
	}

	ipListener = &Listener{
		Listener: listener,
		Timeout:  timeout,
	}

	if (len(listenOnLocalhost) == 0 || listenOnLocalhost[0]) && !IsLoopbackOrWildcardAddress(host) {
		listener, err = net.Listen("tcp", JoinHostPort("localhost", port))
		if err != nil {
			glog.V(0).Infof("skip starting localhost listener on :%d: %v", port, err)
			return ipListener, nil, nil
		}

		localListener = &Listener{
			Listener: listener,
			Timeout:  timeout,
		}
	}

	return
}
