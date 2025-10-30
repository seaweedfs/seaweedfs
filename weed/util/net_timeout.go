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
	bytesRead    int64
	bytesWritten int64
	lastWrite    time.Time
}

func (c *Conn) Read(b []byte) (count int, e error) {
	if c.ReadTimeout != 0 {
		err := c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout * time.Duration(c.bytesRead/40000+1)))
		if err != nil {
			return 0, err
		}
	}
	count, e = c.Conn.Read(b)
	if e == nil {
		stats.BytesIn(int64(count))
		c.bytesRead += int64(count)
	}
	return
}

func (c *Conn) Write(b []byte) (count int, e error) {
	if c.WriteTimeout != 0 {
		now := time.Now()
		// Calculate timeout with two components:
		// 1. Base timeout scaled by cumulative data (minimum 4KB/s throughput)
		// 2. Additional grace period if there was a gap since last write (for chunk fetch delays)
		timeoutMultiplier := time.Duration(c.bytesWritten/40000 + 1)
		baseTimeout := c.WriteTimeout * timeoutMultiplier

		// If it's been a while since last write, add grace time for server-side chunk fetches
		// But cap it to avoid keeping slow clients connected indefinitely
		if !c.lastWrite.IsZero() {
			timeSinceLastWrite := now.Sub(c.lastWrite)
			if timeSinceLastWrite > c.WriteTimeout {
				// Add grace time, but cap at 3x base timeout to handle slow clients
				graceTime := timeSinceLastWrite
				if graceTime > baseTimeout*3 {
					graceTime = baseTimeout * 3
				}
				baseTimeout += graceTime
			}
		}

		err := c.Conn.SetWriteDeadline(now.Add(baseTimeout))
		if err != nil {
			return 0, err
		}
	}
	count, e = c.Conn.Write(b)
	if e == nil {
		stats.BytesOut(int64(count))
		c.bytesWritten += int64(count)
		c.lastWrite = time.Now()
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
		Listener:     listener,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}

	return
}

func NewIpAndLocalListeners(host string, port int, timeout time.Duration) (ipListener net.Listener, localListener net.Listener, err error) {
	listener, err := net.Listen("tcp", JoinHostPort(host, port))
	if err != nil {
		return
	}

	ipListener = &Listener{
		Listener:     listener,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}

	if host != "localhost" && host != "" && host != "0.0.0.0" && host != "127.0.0.1" && host != "[::]" && host != "[::1]" {
		listener, err = net.Listen("tcp", JoinHostPort("localhost", port))
		if err != nil {
			glog.V(0).Infof("skip starting on %s:%d: %v", host, port, err)
			return ipListener, nil, nil
		}

		localListener = &Listener{
			Listener:     listener,
			ReadTimeout:  timeout,
			WriteTimeout: timeout,
		}
	}

	return
}
