package gateway

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

// resolveAdvertisedAddress resolves the appropriate address to advertise to Kafka clients
// when the server binds to all interfaces (:: or 0.0.0.0)
func resolveAdvertisedAddress() string {
	// Try to find a non-loopback interface
	interfaces, err := net.Interfaces()
	if err != nil {
		glog.V(1).Infof("Failed to get network interfaces, using localhost: %v", err)
		return "127.0.0.1"
	}

	for _, iface := range interfaces {
		// Skip loopback and inactive interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
				// Prefer IPv4 addresses for better Kafka client compatibility
				if ipv4 := ipNet.IP.To4(); ipv4 != nil {
					return ipv4.String()
				}
			}
		}
	}

	// Fallback to localhost if no suitable interface found
	glog.V(1).Infof("No non-loopback interface found, using localhost")
	return "127.0.0.1"
}

type Options struct {
	Listen     string
	Masters    string // SeaweedFS master servers (required for production mode)
	FilerGroup string // filer group name (optional)
	TestMode   bool   // Use in-memory handler for testing (optional)
}

type Server struct {
	opts    Options
	ln      net.Listener
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	handler *protocol.Handler
}

func NewServer(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	var handler *protocol.Handler
	var err error

	if opts.TestMode || opts.Masters == "" {
		// Use in-memory handler for testing or when no masters are configured
		handler = protocol.NewHandler()
		glog.V(1).Infof("Created Kafka gateway with in-memory handler for testing")
	} else {
		// Create broker-based SeaweedMQ handler for production
		handler, err = protocol.NewSeaweedMQBrokerHandler(opts.Masters, opts.FilerGroup)
		if err != nil {
			glog.Fatalf("Failed to create SeaweedMQ broker handler: %v", err)
		}
		glog.V(1).Infof("Created Kafka gateway with SeaweedMQ brokers via masters %s", opts.Masters)
	}

	return &Server{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.opts.Listen)
	if err != nil {
		return err
	}
	s.ln = ln

	// Update handler with actual broker address for Metadata responses
	host, port := s.GetListenerAddr()
	s.handler.SetBrokerAddress(host, port)
	glog.V(1).Infof("Kafka gateway advertising broker at %s:%d", host, port)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.ln.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
					return
				}
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				if err := s.handler.HandleConn(c); err != nil {
					glog.V(1).Infof("handle conn %v: %v", c.RemoteAddr(), err)
				}
			}(conn)
		}
	}()
	return nil
}

func (s *Server) Wait() error {
	s.wg.Wait()
	return nil
}

func (s *Server) Close() error {
	s.cancel()
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.wg.Wait()

	// Close the handler (important for SeaweedMQ mode)
	if s.handler != nil {
		if err := s.handler.Close(); err != nil {
			glog.Warningf("Error closing handler: %v", err)
		}
	}

	return nil
}

// Addr returns the bound address of the server listener, or empty if not started.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// GetHandler returns the protocol handler (for testing)
func (s *Server) GetHandler() *protocol.Handler {
	return s.handler
}

// GetListenerAddr returns the actual listening address and port
func (s *Server) GetListenerAddr() (string, int) {
	if s.ln == nil {
		return "localhost", 9092 // fallback
	}

	addr := s.ln.Addr().String()
	// Parse [::]:port or host:port format - use exact match for kafka-go compatibility
	if strings.HasPrefix(addr, "[::]:") {
		port := strings.TrimPrefix(addr, "[::]:")
		if p, err := strconv.Atoi(port); err == nil {
			// Resolve appropriate address when bound to IPv6 all interfaces
			return resolveAdvertisedAddress(), p
		}
	}

	// Handle host:port format
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if p, err := strconv.Atoi(port); err == nil {
			// Resolve appropriate address when bound to all interfaces
			if host == "::" || host == "" || host == "0.0.0.0" {
				host = resolveAdvertisedAddress()
			}
			return host, p
		}
	}

	return "localhost", 9092 // fallback
}
