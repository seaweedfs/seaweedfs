package broker

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
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

	// Validate flat schema format
	if request.MessageRecordType != nil && len(request.KeyColumns) > 0 {
		if err := schema.ValidateKeyColumns(request.MessageRecordType, request.KeyColumns); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid key columns: %v", err)
		}
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
		// Check if schema needs to be updated
		schemaChanged := false

		if request.MessageRecordType != nil && resp.MessageRecordType != nil {
			if !proto.Equal(request.MessageRecordType, resp.MessageRecordType) {
				schemaChanged = true
			}
		} else if request.MessageRecordType != nil || resp.MessageRecordType != nil {
			schemaChanged = true
		}

		if !schemaChanged {
			glog.V(0).Infof("existing topic partitions %d: %+v", len(resp.BrokerPartitionAssignments), resp.BrokerPartitionAssignments)
			return resp, nil
		}

		// Update schema in existing configuration
		resp.MessageRecordType = request.MessageRecordType
		resp.KeyColumns = request.KeyColumns
		resp.SchemaFormat = request.SchemaFormat

		if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
			return nil, fmt.Errorf("update topic schemas: %w", err)
		}

		// Invalidate topic cache since we just updated the topic
		b.invalidateTopicCache(t)

		glog.V(0).Infof("updated schemas for topic %s", request.Topic)
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
	// Set flat schema format
	resp.MessageRecordType = request.MessageRecordType
	resp.KeyColumns = request.KeyColumns
	resp.SchemaFormat = request.SchemaFormat
	resp.Retention = request.Retention

	// save the topic configuration on filer
	if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
		return nil, fmt.Errorf("configure topic: %w", err)
	}

	// Invalidate topic cache since we just created/updated the topic
	b.invalidateTopicCache(t)

	b.PubBalancer.OnPartitionChange(request.Topic, resp.BrokerPartitionAssignments)

	// Actually assign the new partitions to brokers and add to localTopicManager
	if assignErr := b.assignTopicPartitionsToBrokers(ctx, request.Topic, resp.BrokerPartitionAssignments, true); assignErr != nil {
		glog.Errorf("assign topic %s partitions to brokers: %v", request.Topic, assignErr)
		return nil, fmt.Errorf("assign topic partitions: %w", assignErr)
	}

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, resp.BrokerPartitionAssignments)

	return resp, nil
}
