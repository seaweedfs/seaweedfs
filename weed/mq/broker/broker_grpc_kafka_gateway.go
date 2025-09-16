package broker

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// Only KafkaGatewayHeartbeat is needed - handles auto-registration

// KafkaGatewayHeartbeat handles gateway heartbeat requests
func (b *MessageQueueBroker) KafkaGatewayHeartbeat(ctx context.Context, req *mq_pb.KafkaGatewayHeartbeatRequest) (*mq_pb.KafkaGatewayHeartbeatResponse, error) {
	// Only the broker leader should handle gateway heartbeats
	if !b.isLockOwner() {
		// Proxy to the leader broker
		return b.proxyKafkaGatewayHeartbeat(ctx, req)
	}

	return b.gatewayRegistry.ProcessHeartbeat(req)
}

// Helper method for proxying heartbeats to leader
func (b *MessageQueueBroker) proxyKafkaGatewayHeartbeat(ctx context.Context, req *mq_pb.KafkaGatewayHeartbeatRequest) (*mq_pb.KafkaGatewayHeartbeatResponse, error) {
	var resp *mq_pb.KafkaGatewayHeartbeatResponse
	var err error

	proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
		resp, err = client.KafkaGatewayHeartbeat(ctx, req)
		return err
	})

	if proxyErr != nil {
		return &mq_pb.KafkaGatewayHeartbeatResponse{
			Success:      false,
			ErrorMessage: proxyErr.Error(),
		}, nil
	}

	return resp, err
}
