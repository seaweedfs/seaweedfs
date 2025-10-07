package gateway

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	Listen            string
	Masters           string // SeaweedFS master servers
	FilerGroup        string // filer group name (optional)
	SchemaRegistryURL string // Schema Registry URL (optional)
	DefaultPartitions int32  // Default number of partitions for new topics
}

type Server struct {
	opts                Options
	ln                  net.Listener
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
	handler             *protocol.Handler
	coordinatorRegistry *CoordinatorRegistry
}

func NewServer(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	var handler *protocol.Handler
	var err error

	// Create SeaweedMQ handler - masters are required for production
	if opts.Masters == "" {
		glog.Fatalf("SeaweedMQ masters are required for Kafka gateway - provide masters addresses")
	}

	// Use the intended listen address as the client host for master registration
	clientHost := opts.Listen
	if clientHost == "" {
		clientHost = "127.0.0.1:9092" // Default Kafka port
	}

	handler, err = protocol.NewSeaweedMQBrokerHandler(opts.Masters, opts.FilerGroup, clientHost)
	if err != nil {
		glog.Fatalf("Failed to create SeaweedMQ handler with masters %s: %v", opts.Masters, err)
	}

	glog.V(1).Infof("Created Kafka gateway with SeaweedMQ brokers via masters %s", opts.Masters)

	// Initialize schema management if Schema Registry URL is provided
	// Note: This is done lazily on first use if it fails here (e.g., if Schema Registry isn't ready yet)
	if opts.SchemaRegistryURL != "" {
		schemaConfig := schema.ManagerConfig{
			RegistryURL: opts.SchemaRegistryURL,
		}
		if err := handler.EnableSchemaManagement(schemaConfig); err != nil {
			glog.Warningf("Schema management initialization deferred (Schema Registry may not be ready yet): %v", err)
			glog.V(1).Infof("Will retry schema management initialization on first schema-related operation")
			// Store schema registry URL for lazy initialization
			handler.SetSchemaRegistryURL(opts.SchemaRegistryURL)
		} else {
			glog.V(1).Infof("Schema management enabled with Schema Registry at %s", opts.SchemaRegistryURL)
		}
	}

	server := &Server{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
	}

	return server
}

// NewTestServerForUnitTests creates a test server with a minimal mock handler for unit tests
// This allows basic gateway functionality testing without requiring SeaweedMQ masters
func NewTestServerForUnitTests(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a minimal handler with mock SeaweedMQ backend
	handler := NewMinimalTestHandler()

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

	// Get gateway address for coordinator registry
	// CRITICAL FIX: Use the actual bound address from listener, not the requested listen address
	// This is important when using port 0 (random port) for testing
	actualListenAddr := s.ln.Addr().String()
	host, port := s.handler.GetAdvertisedAddress(actualListenAddr)
	gatewayAddress := fmt.Sprintf("%s:%d", host, port)
	glog.V(1).Infof("Kafka gateway listening on %s, advertising as %s in Metadata responses", actualListenAddr, gatewayAddress)

	// Set gateway address in handler for coordinator registry
	s.handler.SetGatewayAddress(gatewayAddress)

	// Initialize coordinator registry for distributed coordinator assignment (only if masters are configured)
	if s.opts.Masters != "" {
		// Parse all masters from the comma-separated list using pb.ServerAddresses
		masters := pb.ServerAddresses(s.opts.Masters).ToAddresses()

		grpcDialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

		s.coordinatorRegistry = NewCoordinatorRegistry(gatewayAddress, masters, grpcDialOption)
		s.handler.SetCoordinatorRegistry(s.coordinatorRegistry)

		// Start coordinator registry
		if err := s.coordinatorRegistry.Start(); err != nil {
			glog.Errorf("Failed to start coordinator registry: %v", err)
			return err
		}

		glog.V(1).Infof("Started coordinator registry for gateway %s", gatewayAddress)
	} else {
		glog.V(1).Infof("No masters configured, skipping coordinator registry setup (test mode)")
	}
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
			// Simple accept log to trace client connections (useful for JoinGroup debugging)
			if conn != nil {
				glog.V(1).Infof("accepted conn %s -> %s", conn.RemoteAddr(), conn.LocalAddr())
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				if err := s.handler.HandleConn(s.ctx, c); err != nil {
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

	// Stop coordinator registry
	if s.coordinatorRegistry != nil {
		if err := s.coordinatorRegistry.Stop(); err != nil {
			glog.Warningf("Error stopping coordinator registry: %v", err)
		}
	}

	if s.ln != nil {
		_ = s.ln.Close()
	}

	// Wait for goroutines to finish with a timeout to prevent hanging
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Normal shutdown
	case <-time.After(5 * time.Second):
		// Timeout - force shutdown
		glog.Warningf("Server shutdown timed out after 5 seconds, forcing close")
	}

	// Close the handler (important for SeaweedMQ mode)
	if s.handler != nil {
		if err := s.handler.Close(); err != nil {
			glog.Warningf("Error closing handler: %v", err)
		}
	}

	return nil
}

// Removed registerWithBrokerLeader - no longer needed

// Addr returns the bound address of the server listener, or empty if not started.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	// Normalize to an address reachable by clients
	host, port := s.GetListenerAddr()
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// GetHandler returns the protocol handler (for testing)
func (s *Server) GetHandler() *protocol.Handler {
	return s.handler
}

// GetListenerAddr returns the actual listening address and port
func (s *Server) GetListenerAddr() (string, int) {
	if s.ln == nil {
		// Return empty values to indicate address not available yet
		// The caller should handle this appropriately
		return "", 0
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

	// This should not happen if the listener was set up correctly
	glog.Warningf("Unable to parse listener address: %s", addr)
	return "", 0
}
