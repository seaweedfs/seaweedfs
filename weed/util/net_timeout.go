package util

import (
	"net"
	"time"

	"github.com/chrislusf/seaweedfs/weed/stats"
)

// Listener wraps a net.Listener, and gives a place to store the timeout
// parameters. On Accept, it will wrap the net.Conn with our own Conn for us.
type Listener struct {
	net.Listener
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	stats.ConnectionOpen()
	tc := &Conn{
		Conn:         c,
		ReadTimeout:  l.ReadTimeout,
		WriteTimeout: l.WriteTimeout,
	}
	return tc, nil
}

// Conn wraps a net.Conn, and sets a deadline for every read
// and write operation.
type Conn struct {
	net.Conn
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	isClosed     bool
}

func (c *Conn) Read(b []byte) (count int, e error) {
	if c.ReadTimeout != 0 {
		err := c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout))
		if err != nil {
			return 0, err
		}
	}
	count, e = c.Conn.Read(b)
	if e == nil {
		stats.BytesIn(int64(count))
	}
	return
}

func (c *Conn) Write(b []byte) (count int, e error) {
	if c.WriteTimeout != 0 {
		// minimum 4KB/s
		err := c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout * time.Duration(len(b)/40000+1)))
		if err != nil {
			return 0, err
		}
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

func NewListener(addr string, timeout time.Duration) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	tl := &Listener{
		Listener:     l,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	return tl, nil
}
