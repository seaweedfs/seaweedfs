package util

import (
	"net"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

const (
	// minThroughputBytesPerSecond defines the minimum expected throughput (4KB/s)
	// Used to calculate timeout scaling based on data transferred
	minThroughputBytesPerSecond = 4000

	// graceTimeCapMultiplier caps the grace period for slow clients at 3x base timeout
	// This prevents indefinite connections while allowing time for server-side chunk fetches
	graceTimeCapMultiplier = 3
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

// calculateBytesPerTimeout calculates the expected number of bytes that should
// be transferred during one timeout period, based on the minimum throughput.
// Returns at least 1 to prevent division by zero.
func calculateBytesPerTimeout(timeout time.Duration) int64 {
	bytesPerTimeout := int64(float64(minThroughputBytesPerSecond) * timeout.Seconds())
	if bytesPerTimeout <= 0 {
		return 1 // Prevent division by zero
	}
	return bytesPerTimeout
}

func (c *Conn) Read(b []byte) (count int, e error) {
	if c.ReadTimeout != 0 {
		// Calculate expected bytes per timeout period based on minimum throughput (4KB/s)
		// Example: with ReadTimeout=30s, bytesPerTimeout = 4000 * 30 = 120KB
		// After reading 1MB: multiplier = 1,000,000/120,000 + 1 ≈ 9, deadline = 30s * 9 = 270s
		bytesPerTimeout := calculateBytesPerTimeout(c.ReadTimeout)
		timeoutMultiplier := time.Duration(c.bytesRead/bytesPerTimeout + 1)
		err := c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout * timeoutMultiplier))
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
		// 1. Base timeout scaled by cumulative data (minimum throughput of 4KB/s)
		// 2. Additional grace period if there was a gap since last write (for chunk fetch delays)

		// Calculate expected bytes per timeout period based on minimum throughput (4KB/s)
		// Example: with WriteTimeout=30s, bytesPerTimeout = 4000 * 30 = 120KB
		// After writing 1MB: multiplier = 1,000,000/120,000 + 1 ≈ 9, baseTimeout = 30s * 9 = 270s
		bytesPerTimeout := calculateBytesPerTimeout(c.WriteTimeout)
		timeoutMultiplier := time.Duration(c.bytesWritten/bytesPerTimeout + 1)
		baseTimeout := c.WriteTimeout * timeoutMultiplier

		// If it's been a while since last write, add grace time for server-side chunk fetches
		// But cap it to avoid keeping slow clients connected indefinitely
		//
		// The comparison uses unscaled WriteTimeout intentionally: triggers grace when idle time
		// exceeds base timeout, independent of throughput scaling.
		if !c.lastWrite.IsZero() {
			timeSinceLastWrite := now.Sub(c.lastWrite)
			if timeSinceLastWrite > c.WriteTimeout {
				// Add grace time capped at graceTimeCapMultiplier * scaled timeout.
				// This allows total deadline up to 4x scaled timeout for server-side delays.
				//
				// Example: WriteTimeout=30s, 1MB written (multiplier≈9), baseTimeout=270s
				// If 400s gap occurs fetching chunks: graceTime capped at 270s*3=810s
				// Final deadline: 270s + 810s = 1080s (~18min) to accommodate slow storage
				// But if only 50s gap: graceTime = 50s, final deadline = 270s + 50s = 320s
				graceTime := timeSinceLastWrite
				if graceTime > baseTimeout*graceTimeCapMultiplier {
					graceTime = baseTimeout * graceTimeCapMultiplier
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
