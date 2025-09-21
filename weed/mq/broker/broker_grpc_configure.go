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
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
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

	// Validate and normalize schemas
	// Handle both new flat schema format and legacy dual schema format
	var normalizedKeySchema *schema_pb.RecordType
	var normalizedValueSchema *schema_pb.RecordType
	var normalizedFlatSchema *schema_pb.RecordType
	var normalizedKeyColumns []string
	
	if request.MessageRecordType != nil && len(request.KeyColumns) > 0 {
		// New flat schema format
		if err := schema.ValidateKeyColumns(request.MessageRecordType, request.KeyColumns); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid key columns: %v", err)
		}
		
		var err error
		normalizedKeySchema, normalizedValueSchema, err = schema.SplitFlatSchemaToKeyValue(request.MessageRecordType, request.KeyColumns)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to split flat schema: %v", err)
		}
		normalizedFlatSchema = request.MessageRecordType
		normalizedKeyColumns = request.KeyColumns
	} else if request.KeyRecordType != nil || request.ValueRecordType != nil {
		// Legacy dual schema format
		normalizedKeySchema = request.KeyRecordType
		normalizedValueSchema = request.ValueRecordType
		normalizedFlatSchema, normalizedKeyColumns = schema.CombineFlatSchemaFromKeyValue(request.KeyRecordType, request.ValueRecordType)
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
		// Check if schemas need to be updated
		schemaChanged := false
		
		// Compare flat schemas if available
		if normalizedFlatSchema != nil && resp.MessageRecordType != nil {
			if !proto.Equal(normalizedFlatSchema, resp.MessageRecordType) {
				schemaChanged = true
			}
		} else if normalizedFlatSchema != nil || resp.MessageRecordType != nil {
			schemaChanged = true
		} else {
			// Fall back to comparing individual key/value schemas
			keySchemaChanged := false
			valueSchemaChanged := false
			
			if normalizedKeySchema != nil && resp.KeyRecordType != nil {
				if !proto.Equal(normalizedKeySchema, resp.KeyRecordType) {
					keySchemaChanged = true
				}
			} else if normalizedKeySchema != nil || resp.KeyRecordType != nil {
				keySchemaChanged = true
			}
			
			if normalizedValueSchema != nil && resp.ValueRecordType != nil {
				if !proto.Equal(normalizedValueSchema, resp.ValueRecordType) {
					valueSchemaChanged = true
				}
			} else if normalizedValueSchema != nil || resp.ValueRecordType != nil {
				valueSchemaChanged = true
			}
			
			schemaChanged = keySchemaChanged || valueSchemaChanged
		}

		if !schemaChanged {
			glog.V(0).Infof("existing topic partitions %d: %+v", len(resp.BrokerPartitionAssignments), resp.BrokerPartitionAssignments)
			return resp, nil
		}

		// Update schemas in existing configuration - populate both formats for compatibility
		resp.KeyRecordType = normalizedKeySchema
		resp.ValueRecordType = normalizedValueSchema
		resp.MessageRecordType = normalizedFlatSchema
		resp.KeyColumns = normalizedKeyColumns
		
		if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
			return nil, fmt.Errorf("update topic schemas: %w", err)
		}

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
	// Populate both legacy and new schema formats for compatibility
	resp.KeyRecordType = normalizedKeySchema
	resp.ValueRecordType = normalizedValueSchema
	resp.MessageRecordType = normalizedFlatSchema
	resp.KeyColumns = normalizedKeyColumns
	resp.Retention = request.Retention

	// save the topic configuration on filer
	if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
		return nil, fmt.Errorf("configure topic: %w", err)
	}

	b.PubBalancer.OnPartitionChange(request.Topic, resp.BrokerPartitionAssignments)

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, resp.BrokerPartitionAssignments)

	return resp, err
}
