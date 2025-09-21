package broker

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ConfigureTopic Runs on any broker, but proxied to the balancer if not the balancer
// It generates an assignments based on existing allocations,
// and then assign the partitions to the brokers.
func (b *MessageQueueBroker) ConfigureTopic(ctx context.Context, request *mq_pb.ConfigureTopicRequest) (resp *mq_pb.ConfigureTopicResponse, err error) {
	if !b.isLockOwner() {
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ConfigureTopic(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	// validate the schemas
	if request.ValueRecordType != nil {
		// TODO: Add validation for value schema
	}
	if request.KeyRecordType != nil {
		// TODO: Add validation for key schema
	}

	t := topic.FromPbTopic(request.Topic)
	var readErr, assignErr error
	resp, readErr = b.fca.ReadTopicConfFromFiler(t)
	if readErr != nil {
		glog.V(0).Infof("read topic %s conf: %v", request.Topic, readErr)
	}

	if resp != nil {
		assignErr = b.ensureTopicActiveAssignments(t, resp)
		// no need to assign directly.
		// The added or updated assignees will read from filer directly.
		// The gone assignees will die by themselves.
	}

	if readErr == nil && assignErr == nil && len(resp.BrokerPartitionAssignments) == int(request.PartitionCount) {
		// Check if schemas (key or value) need to be updated
		keySchemaChanged := false
		valueSchemaChanged := false

		// Check value schema changes
		if request.ValueRecordType != nil && resp.ValueRecordType != nil {
			if !proto.Equal(request.ValueRecordType, resp.ValueRecordType) {
				valueSchemaChanged = true
			}
		} else if request.ValueRecordType != nil || resp.ValueRecordType != nil {
			valueSchemaChanged = true
		}

		// Check key schema changes
		if request.KeyRecordType != nil && resp.KeyRecordType != nil {
			if !proto.Equal(request.KeyRecordType, resp.KeyRecordType) {
				keySchemaChanged = true
			}
		} else if request.KeyRecordType != nil || resp.KeyRecordType != nil {
			keySchemaChanged = true
		}

		if !keySchemaChanged && !valueSchemaChanged {
			glog.V(0).Infof("existing topic partitions %d: %+v", len(resp.BrokerPartitionAssignments), resp.BrokerPartitionAssignments)
			return
		}

		// Update schemas in existing configuration
		resp.KeyRecordType = request.KeyRecordType
		resp.ValueRecordType = request.ValueRecordType
		if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
			return nil, fmt.Errorf("update topic schemas: %w", err)
		}

		glog.V(0).Infof("updated schemas for topic %s (key: %v, value: %v)", request.Topic, keySchemaChanged, valueSchemaChanged)
		return resp, nil
	}

	if resp != nil && len(resp.BrokerPartitionAssignments) > 0 {
		if cancelErr := b.assignTopicPartitionsToBrokers(ctx, request.Topic, resp.BrokerPartitionAssignments, false); cancelErr != nil {
			glog.V(1).Infof("cancel old topic %s partitions assignments %v : %v", request.Topic, resp.BrokerPartitionAssignments, cancelErr)
		}
	}
	resp = &mq_pb.ConfigureTopicResponse{}
	if b.PubBalancer.Brokers.IsEmpty() {
		return nil, status.Errorf(codes.Unavailable, "no broker available: %v", pub_balancer.ErrNoBroker)
	}
	resp.BrokerPartitionAssignments = pub_balancer.AllocateTopicPartitions(b.PubBalancer.Brokers, request.PartitionCount)
	resp.KeyRecordType = request.KeyRecordType
	resp.ValueRecordType = request.ValueRecordType
	resp.Retention = request.Retention

	// save the topic configuration on filer
	if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
		return nil, fmt.Errorf("configure topic: %w", err)
	}

	b.PubBalancer.OnPartitionChange(request.Topic, resp.BrokerPartitionAssignments)

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, resp.BrokerPartitionAssignments)

	return resp, err
}
