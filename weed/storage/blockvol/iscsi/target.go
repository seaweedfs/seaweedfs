package iscsi

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrTargetClosed   = errors.New("iscsi: target server closed")
	ErrVolumeNotFound = errors.New("iscsi: volume not found")
)

// TargetServer manages the iSCSI target: TCP listener, volume registry,
// and active sessions.
type TargetServer struct {
	mu           sync.RWMutex
	listener     net.Listener
	config       TargetConfig
	volumes      map[string]BlockDevice // target IQN -> device
	addr         string
	portalAddr   string // advertised address for discovery (if empty, uses listener addr)

	// Active session tracking for graceful shutdown
	activeMu sync.Mutex
	active   map[uint64]*Session
	nextID   atomic.Uint64

	sessions sync.WaitGroup
	closed   chan struct{}
	logger   *log.Logger
}

// NewTargetServer creates a target server bound to the given address.
func NewTargetServer(addr string, config TargetConfig, logger *log.Logger) *TargetServer {
	if logger == nil {
		logger = log.Default()
	}
	return &TargetServer{
		config:  config,
		volumes: make(map[string]BlockDevice),
		active:  make(map[uint64]*Session),
		addr:    addr,
		closed:  make(chan struct{}),
		logger:  logger,
	}
}

// AddVolume registers a block device under the given target IQN.
func (ts *TargetServer) AddVolume(iqn string, dev BlockDevice) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.volumes[iqn] = dev
	ts.logger.Printf("volume added: %s", iqn)
}

// RemoveVolume unregisters a target IQN.
func (ts *TargetServer) RemoveVolume(iqn string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.volumes, iqn)
	ts.logger.Printf("volume removed: %s", iqn)
}

// HasTarget implements TargetResolver.
func (ts *TargetServer) HasTarget(name string) bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	_, ok := ts.volumes[name]
	return ok
}

// SetPortalAddr sets the address advertised in discovery responses.
// Use this when the listen address is 0.0.0.0 or [::] and clients
// need a routable IP. Format: "ip:port,portal-group" (e.g. "10.0.0.1:3260,1").
func (ts *TargetServer) SetPortalAddr(addr string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.portalAddr = addr
}

// ListTargets implements TargetLister.
func (ts *TargetServer) ListTargets() []DiscoveryTarget {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	addr := ts.portalAddr
	if addr == "" {
		addr = ts.ListenAddr()
	}
	targets := make([]DiscoveryTarget, 0, len(ts.volumes))
	for iqn := range ts.volumes {
		targets = append(targets, DiscoveryTarget{
			Name:    iqn,
			Address: addr,
		})
	}
	return targets
}

// ListenAndServe starts listening for iSCSI connections.
// Blocks until Close() is called or an error occurs.
func (ts *TargetServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", ts.addr)
	if err != nil {
		return fmt.Errorf("iscsi target: listen: %w", err)
	}
	ts.mu.Lock()
	ts.listener = ln
	ts.mu.Unlock()

	ts.logger.Printf("iSCSI target listening on %s", ln.Addr())
	return ts.acceptLoop(ln)
}

// Serve accepts connections on an existing listener.
func (ts *TargetServer) Serve(ln net.Listener) error {
	ts.mu.Lock()
	ts.listener = ln
	ts.mu.Unlock()
	return ts.acceptLoop(ln)
}

func (ts *TargetServer) acceptLoop(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ts.closed:
				return nil
			default:
				return fmt.Errorf("iscsi target: accept: %w", err)
			}
		}

		ts.sessions.Add(1)
		go ts.handleConn(conn)
	}
}

func (ts *TargetServer) handleConn(conn net.Conn) {
	defer ts.sessions.Done()
	defer conn.Close()

	ts.logger.Printf("new connection from %s", conn.RemoteAddr())

	sess := NewSession(conn, ts.config, ts, ts, ts.logger)

	id := ts.nextID.Add(1)
	ts.activeMu.Lock()
	ts.active[id] = sess
	ts.activeMu.Unlock()

	defer func() {
		ts.activeMu.Lock()
		delete(ts.active, id)
		ts.activeMu.Unlock()
	}()

	if err := sess.HandleConnection(); err != nil {
		ts.logger.Printf("session error (%s): %v", conn.RemoteAddr(), err)
	}

	ts.logger.Printf("connection closed: %s", conn.RemoteAddr())
}

// LookupDevice implements DeviceLookup. Returns the BlockDevice for the given IQN,
// or a nullDevice if not found.
func (ts *TargetServer) LookupDevice(iqn string) BlockDevice {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if dev, ok := ts.volumes[iqn]; ok {
		return dev
	}
	return &nullDevice{}
}

// Close gracefully shuts down the target server.
func (ts *TargetServer) Close() error {
	select {
	case <-ts.closed:
		return nil
	default:
	}
	close(ts.closed)

	ts.mu.RLock()
	ln := ts.listener
	ts.mu.RUnlock()

	if ln != nil {
		ln.Close()
	}

	// Close all active sessions to unblock ReadPDU
	ts.activeMu.Lock()
	for _, sess := range ts.active {
		sess.Close()
	}
	ts.activeMu.Unlock()

	ts.sessions.Wait()
	return nil
}

// ListenAddr returns the actual listen address (useful when port=0).
func (ts *TargetServer) ListenAddr() string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if ts.listener != nil {
		return ts.listener.Addr().String()
	}
	return ts.addr
}

// nullDevice is a stub BlockDevice used when no volumes are registered.
type nullDevice struct{}

func (d *nullDevice) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return make([]byte, length), nil
}
func (d *nullDevice) WriteAt(lba uint64, data []byte) error { return nil }
func (d *nullDevice) Trim(lba uint64, length uint32) error  { return nil }
func (d *nullDevice) SyncCache() error                      { return nil }
func (d *nullDevice) BlockSize() uint32                     { return 4096 }
func (d *nullDevice) VolumeSize() uint64                    { return 0 }
func (d *nullDevice) IsHealthy() bool                       { return false }
