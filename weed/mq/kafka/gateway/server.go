package gateway

import (
	"context"
	"net"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

type Options struct {
	Listen            string
	AgentAddress      string // Optional: SeaweedMQ Agent address for production mode
	UseSeaweedMQ      bool   // Use SeaweedMQ backend instead of in-memory stub
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
