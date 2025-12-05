package util

import (
	"net"
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
type Conn struct {
	net.Conn
	Timeout  time.Duration
	isClosed bool
}

// extendDeadline extends the connection deadline from now.
// This implements "no activity timeout" - any activity keeps the connection alive.
func (c *Conn) extendDeadline() error {
	if c.Timeout != 0 {
		return c.Conn.SetDeadline(time.Now().Add(c.Timeout))
	}
	return nil
}

func (c *Conn) Read(b []byte) (count int, e error) {
	// Extend deadline before reading - any activity keeps connection alive
	if err := c.extendDeadline(); err != nil {
		return 0, err
	}
	count, e = c.Conn.Read(b)
	if e == nil {
		stats.BytesIn(int64(count))
	}
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

func NewIpAndLocalListeners(host string, port int, timeout time.Duration) (ipListener net.Listener, localListener net.Listener, err error) {
	listener, err := net.Listen("tcp", JoinHostPort(host, port))
	if err != nil {
		return
	}

	ipListener = &Listener{
		Listener: listener,
		Timeout:  timeout,
	}

	if host != "localhost" && host != "" && host != "0.0.0.0" && host != "127.0.0.1" && host != "[::]" && host != "[::1]" {
		listener, err = net.Listen("tcp", JoinHostPort("localhost", port))
		if err != nil {
			glog.V(0).Infof("skip starting on %s:%d: %v", host, port, err)
			return ipListener, nil, nil
		}

		localListener = &Listener{
			Listener: listener,
			Timeout:  timeout,
		}
	}

	return
}
