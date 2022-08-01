package net2

import (
	"net"
	"strings"
	"time"

	rp "github.com/seaweedfs/seaweedfs/weed/wdclient/resource_pool"
)

const defaultDialTimeout = 1 * time.Second

func defaultDialFunc(network string, address string) (net.Conn, error) {
	return net.DialTimeout(network, address, defaultDialTimeout)
}

func parseResourceLocation(resourceLocation string) (
	network string,
	address string) {

	idx := strings.Index(resourceLocation, " ")
	if idx >= 0 {
		return resourceLocation[:idx], resourceLocation[idx+1:]
	}

	return "", resourceLocation
}

// A thin wrapper around the underlying resource pool.
type connectionPoolImpl struct {
	options ConnectionOptions

	pool rp.ResourcePool
}

// This returns a connection pool where all connections are connected
// to the same (network, address)
func newBaseConnectionPool(
	options ConnectionOptions,
	createPool func(rp.Options) rp.ResourcePool) ConnectionPool {

	dial := options.Dial
	if dial == nil {
		dial = defaultDialFunc
	}

	openFunc := func(loc string) (interface{}, error) {
		network, address := parseResourceLocation(loc)
		return dial(network, address)
	}

	closeFunc := func(handle interface{}) error {
		return handle.(net.Conn).Close()
	}

	poolOptions := rp.Options{
		MaxActiveHandles:   options.MaxActiveConnections,
		MaxIdleHandles:     options.MaxIdleConnections,
		MaxIdleTime:        options.MaxIdleTime,
		OpenMaxConcurrency: options.DialMaxConcurrency,
		Open:               openFunc,
		Close:              closeFunc,
		NowFunc:            options.NowFunc,
	}

	return &connectionPoolImpl{
		options: options,
		pool:    createPool(poolOptions),
	}
}

// This returns a connection pool where all connections are connected
// to the same (network, address)
func NewSimpleConnectionPool(options ConnectionOptions) ConnectionPool {
	return newBaseConnectionPool(options, rp.NewSimpleResourcePool)
}

// This returns a connection pool that manages multiple (network, address)
// entries.  The connections to each (network, address) entry acts
// independently. For example ("tcp", "localhost:11211") could act as memcache
// shard 0 and ("tcp", "localhost:11212") could act as memcache shard 1.
func NewMultiConnectionPool(options ConnectionOptions) ConnectionPool {
	return newBaseConnectionPool(
		options,
		func(poolOptions rp.Options) rp.ResourcePool {
			return rp.NewMultiResourcePool(poolOptions, nil)
		})
}

// See ConnectionPool for documentation.
func (p *connectionPoolImpl) NumActive() int32 {
	return p.pool.NumActive()
}

// See ConnectionPool for documentation.
func (p *connectionPoolImpl) ActiveHighWaterMark() int32 {
	return p.pool.ActiveHighWaterMark()
}

// This returns the number of alive idle connections.  This method is not part
// of ConnectionPool's API.  It is used only for testing.
func (p *connectionPoolImpl) NumIdle() int {
	return p.pool.NumIdle()
}

// BaseConnectionPool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *connectionPoolImpl) Register(network string, address string) error {
	return p.pool.Register(network + " " + address)
}

// BaseConnectionPool has nothing to do on Unregister.
func (p *connectionPoolImpl) Unregister(network string, address string) error {
	return nil
}

func (p *connectionPoolImpl) ListRegistered() []NetworkAddress {
	result := make([]NetworkAddress, 0, 1)
	for _, location := range p.pool.ListRegistered() {
		network, address := parseResourceLocation(location)

		result = append(
			result,
			NetworkAddress{
				Network: network,
				Address: address,
			})
	}
	return result
}

// This gets an active connection from the connection pool.  Note that network
// and address arguments are ignored (The connections with point to the
// network/address provided by the first Register call).
func (p *connectionPoolImpl) Get(
	network string,
	address string) (ManagedConn, error) {

	handle, err := p.pool.Get(network + " " + address)
	if err != nil {
		return nil, err
	}
	return NewManagedConn(network, address, handle, p, p.options), nil
}

// See ConnectionPool for documentation.
func (p *connectionPoolImpl) Release(conn ManagedConn) error {
	return conn.ReleaseConnection()
}

// See ConnectionPool for documentation.
func (p *connectionPoolImpl) Discard(conn ManagedConn) error {
	return conn.DiscardConnection()
}

// See ConnectionPool for documentation.
func (p *connectionPoolImpl) EnterLameDuckMode() {
	p.pool.EnterLameDuckMode()
}
