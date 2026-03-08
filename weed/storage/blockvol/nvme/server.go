package nvme

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds NVMe/TCP target configuration.
type Config struct {
	ListenAddr      string
	NQNPrefix       string
	MaxH2CDataLength uint32
	MaxIOQueues     uint16
	Enabled         bool
}

// DefaultConfig returns the default NVMe target configuration.
func DefaultConfig() Config {
	return Config{
		ListenAddr:       "0.0.0.0:4420",
		NQNPrefix:        "nqn.2024-01.com.seaweedfs:vol.",
		MaxH2CDataLength: maxH2CDataLen,
		MaxIOQueues:      4,
		Enabled:          false,
	}
}

// adminSession stores state from an admin queue connection that IO queue
// connections need to look up (they arrive on separate TCP connections).
type adminSession struct {
	cntlID    uint16
	subsystem *Subsystem
	subNQN    string
	hostNQN   string
	regCAP    uint64
	regCC     uint32
	regCSTS   uint32
	regVS     uint32
	katoMs    uint32
}

// Server is the NVMe/TCP target server.
type Server struct {
	cfg        Config
	listener   net.Listener
	mu         sync.RWMutex
	subsystems map[string]*Subsystem // NQN → Subsystem
	sessions   map[*Controller]struct{}
	adminMu    sync.RWMutex
	admins     map[uint16]*adminSession // CNTLID → admin session
	nextCNTLID atomic.Uint32
	closed     atomic.Bool
	wg         sync.WaitGroup
}

// NewServer creates a new NVMe/TCP target server.
func NewServer(cfg Config) *Server {
	return &Server{
		cfg:        cfg,
		subsystems: make(map[string]*Subsystem),
		sessions:   make(map[*Controller]struct{}),
		admins:     make(map[uint16]*adminSession),
	}
}

// AddVolume registers a block device as an NVMe subsystem.
func (s *Server) AddVolume(nqn string, dev BlockDevice, nguid [16]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subsystems[nqn] = &Subsystem{
		NQN:   nqn,
		Dev:   dev,
		NGUID: nguid,
	}
}

// RemoveVolume unregisters an NVMe subsystem.
func (s *Server) RemoveVolume(nqn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subsystems, nqn)
}

// ListenAndServe starts the NVMe/TCP listener.
// If not enabled, returns nil immediately.
func (s *Server) ListenAndServe() error {
	if !s.cfg.Enabled {
		return nil
	}

	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("nvme listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = ln
	log.Printf("nvme: listening on %s", s.cfg.ListenAddr)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop()
	}()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			log.Printf("nvme: accept error: %v", err)
			continue
		}

		ctrl := newController(conn, s)
		s.addSession(ctrl)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := ctrl.Serve(); err != nil {
				if !s.closed.Load() {
					log.Printf("nvme: session error: %v", err)
				}
			}
		}()
	}
}

func (s *Server) addSession(ctrl *Controller) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[ctrl] = struct{}{}
}

func (s *Server) removeSession(ctrl *Controller) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, ctrl)
}

// registerAdmin stores admin queue state so IO queue connections can look it up.
func (s *Server) registerAdmin(sess *adminSession) {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	s.admins[sess.cntlID] = sess
}

// unregisterAdmin removes an admin session by CNTLID.
func (s *Server) unregisterAdmin(cntlID uint16) {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	delete(s.admins, cntlID)
}

// lookupAdmin returns the admin session for the given CNTLID.
func (s *Server) lookupAdmin(cntlID uint16) *adminSession {
	s.adminMu.RLock()
	defer s.adminMu.RUnlock()
	return s.admins[cntlID]
}

// Close gracefully shuts down the server.
func (s *Server) Close() error {
	if !s.cfg.Enabled {
		return nil
	}
	s.closed.Store(true)

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all active sessions
	s.mu.RLock()
	sessions := make([]*Controller, 0, len(s.sessions))
	for ctrl := range s.sessions {
		sessions = append(sessions, ctrl)
	}
	s.mu.RUnlock()

	for _, ctrl := range sessions {
		ctrl.conn.Close()
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("nvme: shutdown timed out after 5s")
	}
	return nil
}

// NQN returns the full NQN for a volume name.
func (s *Server) NQN(volName string) string {
	return s.cfg.NQNPrefix + volName
}
