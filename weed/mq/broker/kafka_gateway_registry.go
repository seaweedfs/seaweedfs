package broker

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// KafkaGatewayRegistry manages registered Kafka gateway instances
type KafkaGatewayRegistry struct {
	mu                       sync.RWMutex
	gateways                 map[string]*KafkaGatewayEntry
	heartbeatTimeout         time.Duration
	cleanupInterval          time.Duration
	defaultHeartbeatInterval int32
}

// KafkaGatewayEntry represents a registered Kafka gateway
type KafkaGatewayEntry struct {
	Info              *mq_pb.KafkaGatewayInfo
	LastHeartbeatTime time.Time
	RegistrationTime  time.Time
}

// NewKafkaGatewayRegistry creates a new gateway registry
func NewKafkaGatewayRegistry() *KafkaGatewayRegistry {
	registry := &KafkaGatewayRegistry{
		gateways:                 make(map[string]*KafkaGatewayEntry),
		heartbeatTimeout:         60 * time.Second, // 60 seconds timeout
		cleanupInterval:          30 * time.Second, // cleanup every 30 seconds
		defaultHeartbeatInterval: 30,               // 30 seconds heartbeat interval
	}

	// Start cleanup goroutine
	go registry.cleanupInactiveGateways()

	return registry
}

// No separate registration methods needed - auto-registration happens in ProcessHeartbeat

// ProcessHeartbeat processes a heartbeat from a gateway (auto-registers if needed)
func (r *KafkaGatewayRegistry) ProcessHeartbeat(req *mq_pb.KafkaGatewayHeartbeatRequest) (*mq_pb.KafkaGatewayHeartbeatResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	entry, exists := r.gateways[req.GatewayId]

	if !exists {
		// Auto-register gateway on first heartbeat
		glog.V(1).Infof("Auto-registering Kafka gateway %s at %s", req.GatewayId, req.GatewayAddress)

		gatewayInfo := &mq_pb.KafkaGatewayInfo{
			GatewayId:         req.GatewayId,
			GatewayAddress:    req.GatewayAddress,
			RegistrationTime:  now.Unix(),
			LastHeartbeatTime: now.Unix(),
			IsActive:          true,
			Metadata:          req.Metadata,
			Status:            req.Status,
		}

		entry = &KafkaGatewayEntry{
			Info:              gatewayInfo,
			LastHeartbeatTime: now,
			RegistrationTime:  now,
		}

		r.gateways[req.GatewayId] = entry
	} else {
		// Update existing gateway
		entry.LastHeartbeatTime = now
		entry.Info.LastHeartbeatTime = now.Unix()
		entry.Info.IsActive = true

		// Update metadata if provided
		if req.Metadata != nil {
			for k, v := range req.Metadata {
				entry.Info.Metadata[k] = v
			}
		}

		// Update status if provided
		if req.Status != nil {
			for k, v := range req.Status {
				entry.Info.Status[k] = v
			}
		}
	}

	return &mq_pb.KafkaGatewayHeartbeatResponse{
		Success:                  true,
		HeartbeatIntervalSeconds: r.defaultHeartbeatInterval,
	}, nil
}

// No ListGateways RPC needed - GetActiveGatewayAddresses provides what we need internally

// GetActiveGatewayAddresses returns addresses of all active gateways
func (r *KafkaGatewayRegistry) GetActiveGatewayAddresses() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var addresses []string
	now := time.Now()

	for _, entry := range r.gateways {
		// Only include active gateways
		if now.Sub(entry.LastHeartbeatTime) <= r.heartbeatTimeout {
			addresses = append(addresses, entry.Info.GatewayAddress)
		}
	}

	return addresses
}

// cleanupInactiveGateways periodically removes inactive gateways
func (r *KafkaGatewayRegistry) cleanupInactiveGateways() {
	ticker := time.NewTicker(r.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		now := time.Now()
		var toRemove []string

		for gatewayId, entry := range r.gateways {
			if now.Sub(entry.LastHeartbeatTime) > r.heartbeatTimeout*2 { // Double timeout for cleanup
				toRemove = append(toRemove, gatewayId)
			}
		}

		for _, gatewayId := range toRemove {
			delete(r.gateways, gatewayId)
			glog.V(1).Infof("Cleaned up inactive Kafka gateway %s", gatewayId)
		}
		r.mu.Unlock()
	}
}
