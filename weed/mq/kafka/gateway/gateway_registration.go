package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GatewayRegistration handles registration with SMQ broker leader
type GatewayRegistration struct {
	gatewayID      string
	gatewayAddress string
	brokerAddress  string
	grpcDialOption grpc.DialOption

	// Registration state
	isRegistered    bool
	heartbeatTicker *time.Ticker
	stopChan        chan struct{}

	// Broker connection
	brokerConn   *grpc.ClientConn
	brokerClient mq_pb.SeaweedMessagingClient
}

// NewGatewayRegistration creates a new gateway registration manager
func NewGatewayRegistration(gatewayID, gatewayAddress, brokerAddress string) *GatewayRegistration {
	return &GatewayRegistration{
		gatewayID:      gatewayID,
		gatewayAddress: gatewayAddress,
		brokerAddress:  brokerAddress,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		stopChan:       make(chan struct{}),
	}
}

// Start begins the heartbeat process with the SMQ broker leader (auto-registers on first heartbeat)
func (gr *GatewayRegistration) Start() error {
	// Connect to broker
	conn, err := grpc.NewClient(gr.brokerAddress, gr.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to broker %s: %v", gr.brokerAddress, err)
	}

	gr.brokerConn = conn
	gr.brokerClient = mq_pb.NewSeaweedMessagingClient(conn)

	// Start heartbeat immediately (will auto-register)
	gr.heartbeatTicker = time.NewTicker(30 * time.Second) // Default interval
	go gr.heartbeatLoop()

	// Send first heartbeat immediately for auto-registration
	if err := gr.sendHeartbeat(); err != nil {
		gr.brokerConn.Close()
		return fmt.Errorf("failed to send initial heartbeat: %v", err)
	}

	glog.V(1).Infof("Kafka gateway %s started heartbeats with broker leader at %s", gr.gatewayID, gr.brokerAddress)

	return nil
}

// Stop stops the registration and heartbeat process
func (gr *GatewayRegistration) Stop() error {
	// Signal stop
	close(gr.stopChan)

	// Stop heartbeat ticker
	if gr.heartbeatTicker != nil {
		gr.heartbeatTicker.Stop()
	}

	// No explicit unregistration needed - gateway will be auto-removed after heartbeat timeout

	// Close broker connection
	if gr.brokerConn != nil {
		gr.brokerConn.Close()
	}

	return nil
}

// No separate registration method needed - auto-registration happens on first heartbeat

// heartbeatLoop sends periodic heartbeats to maintain registration
func (gr *GatewayRegistration) heartbeatLoop() {
	defer func() {
		if gr.heartbeatTicker != nil {
			gr.heartbeatTicker.Stop()
		}
	}()

	for {
		select {
		case <-gr.stopChan:
			return
		case <-gr.heartbeatTicker.C:
			if err := gr.sendHeartbeat(); err != nil {
				glog.V(1).Infof("Heartbeat failed for gateway %s: %v", gr.gatewayID, err)
				// TODO: Implement reconnection logic if needed
			}
		}
	}
}

// sendHeartbeat sends a heartbeat to the broker leader (includes gateway address for auto-registration)
func (gr *GatewayRegistration) sendHeartbeat() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &mq_pb.KafkaGatewayHeartbeatRequest{
		GatewayId:      gr.gatewayID,
		GatewayAddress: gr.gatewayAddress, // Include for auto-registration
		HeartbeatTime:  time.Now().Unix(),
		Metadata: map[string]string{
			"version": "1.0",
			"type":    "kafka-gateway",
		},
		Status: map[string]string{
			"status": "active",
			// TODO: Add more status information (connections, load, etc.)
		},
	}

	resp, err := gr.brokerClient.KafkaGatewayHeartbeat(ctx, req)
	if err != nil {
		return fmt.Errorf("heartbeat RPC failed: %v", err)
	}

	if !resp.Success {
		glog.V(1).Infof("Heartbeat rejected for gateway %s: %s", gr.gatewayID, resp.ErrorMessage)

		// If broker requests unregistration, stop heartbeats
		if resp.ShouldUnregister {
			glog.V(1).Infof("Broker requested unregistration for gateway %s", gr.gatewayID)
			gr.isRegistered = false
			return fmt.Errorf("broker requested unregistration")
		}
	} else {
		// Mark as registered on successful heartbeat
		if !gr.isRegistered {
			glog.V(1).Infof("Gateway %s auto-registered successfully", gr.gatewayID)
			gr.isRegistered = true
		}

		// Adjust heartbeat interval if broker suggests a different one
		if resp.HeartbeatIntervalSeconds > 0 {
			newInterval := time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
			if gr.heartbeatTicker != nil && newInterval != 30*time.Second {
				gr.heartbeatTicker.Stop()
				gr.heartbeatTicker = time.NewTicker(newInterval)
				glog.V(2).Infof("Adjusted heartbeat interval to %v for gateway %s", newInterval, gr.gatewayID)
			}
		}
	}

	return nil
}

// IsRegistered returns whether the gateway is currently registered
func (gr *GatewayRegistration) IsRegistered() bool {
	return gr.isRegistered
}
