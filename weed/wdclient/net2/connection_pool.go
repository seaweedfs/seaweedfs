package net2

import (
	"net"
	"time"
)

type ConnectionOptions struct {
	// The maximum number of connections that can be active per host at any
	// given time (A non-positive value indicates the number of connections
	// is unbounded).
	MaxActiveConnections int32

	// The maximum number of idle connections per host that are kept alive by
	// the connection pool.
	MaxIdleConnections uint32

	// The maximum amount of time an idle connection can alive (if specified).
	MaxIdleTime *time.Duration

	// This limits the number of concurrent Dial calls (there's no limit when
	// DialMaxConcurrency is non-positive).
	DialMaxConcurrency int

	// Dial specifies the dial function for creating network connections.
	// If Dial is nil, net.DialTimeout is used, with timeout set to 1 second.
	Dial func(network string, address string) (net.Conn, error)

	// This specifies the now time function.  When the function is non-nil, the
	// connection pool will use the specified function instead of time.Now to
	// generate the current time.
	NowFunc func() time.Time

	// This specifies the timeout for any Read() operation.
	// Note that setting this to 0 (i.e. not setting it) will make
	// read operations block indefinitely.
	ReadTimeout time.Duration

	// This specifies the timeout for any Write() operation.
	// Note that setting this to 0 (i.e. not setting it) will make
	// write operations block indefinitely.
	WriteTimeout time.Duration
}

func (o ConnectionOptions) getCurrentTime() time.Time {
	if o.NowFunc == nil {
		return time.Now()
	} else {
		return o.NowFunc()
	}
}

// A generic interface for managed connection pool.  All connection pool
// implementations must be threadsafe.
type ConnectionPool interface {
	// This returns the number of active connections that are on loan.
	NumActive() int32

	// This returns the highest number of active connections for the entire
	// lifetime of the pool.
	ActiveHighWaterMark() int32

	// This returns the number of idle connections that are in the pool.
	NumIdle() int

	// This associates (network, address) to the connection pool; afterwhich,
	// the user can get connections to (network, address).
	Register(network string, address string) error

	// This dissociate (network, address) from the connection pool;
	// afterwhich, the user can no longer get connections to
	// (network, address).
	Unregister(network string, address string) error

	// This returns the list of registered (network, address) entries.
	ListRegistered() []NetworkAddress

	// This gets an active connection from the connection pool.  The connection
	// will remain active until one of the following is called:
	//  1. conn.ReleaseConnection()
	//  2. conn.DiscardConnection()
	//  3. pool.Release(conn)
	//  4. pool.Discard(conn)
	Get(network string, address string) (ManagedConn, error)

	// This releases an active connection back to the connection pool.
	Release(conn ManagedConn) error

	// This discards an active connection from the connection pool.
	Discard(conn ManagedConn) error

	// Enter the connection pool into lame duck mode.  The connection pool
	// will no longer return connections, and all idle connections are closed
	// immediately (including active connections that are released back to the
	// pool afterward).
	EnterLameDuckMode()
}
