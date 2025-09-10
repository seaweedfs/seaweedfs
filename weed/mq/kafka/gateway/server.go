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

type Options struct {
	Listen       string
	AgentAddress string // Optional: SeaweedMQ Agent address for production mode
	UseSeaweedMQ bool   // Use SeaweedMQ backend instead of in-memory stub
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
	if opts.UseSeaweedMQ && opts.AgentAddress != "" {
		// Try to create SeaweedMQ handler
		smqHandler, err := protocol.NewSeaweedMQHandler(opts.AgentAddress)
		if err != nil {
			glog.Warningf("Failed to create SeaweedMQ handler, falling back to in-memory mode: %v", err)
			handler = protocol.NewHandler()
		} else {
			handler = smqHandler
			glog.V(1).Infof("Created Kafka gateway with SeaweedMQ backend at %s", opts.AgentAddress)
		}
	} else {
		// Use in-memory mode
		handler = protocol.NewHandler()
		glog.V(1).Infof("Created Kafka gateway with in-memory backend")
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
			// Revert to 127.0.0.1 for broader compatibility  
			return "127.0.0.1", p
		}
	}
	
	// Handle host:port format
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if p, err := strconv.Atoi(port); err == nil {
			// Use 127.0.0.1 instead of localhost for better kafka-go compatibility
			if host == "::" || host == "" {
				host = "127.0.0.1"
			}
			return host, p
		}
	}
	
	return "localhost", 9092 // fallback
}
