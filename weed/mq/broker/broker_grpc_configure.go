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

	// validate the schema
	if request.RecordType != nil {
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
		glog.V(0).Infof("existing topic partitions %d: %+v", len(resp.BrokerPartitionAssignments), resp.BrokerPartitionAssignments)
		return
	}

	if resp != nil && len(resp.BrokerPartitionAssignments) > 0 {
		if cancelErr := b.assignTopicPartitionsToBrokers(ctx, request.Topic, resp.BrokerPartitionAssignments, false); cancelErr != nil {
			glog.V(1).Infof("cancel old topic %s partitions assignments %v : %v", request.Topic, resp.BrokerPartitionAssignments, cancelErr)
		}
	}
	resp = &mq_pb.ConfigureTopicResponse{}
	if b.PubBalancer.Brokers.IsEmpty() {
		return nil, status.Errorf(codes.Unavailable, pub_balancer.ErrNoBroker.Error())
	}
	resp.BrokerPartitionAssignments = pub_balancer.AllocateTopicPartitions(b.PubBalancer.Brokers, request.PartitionCount)
	resp.RecordType = request.RecordType

	// save the topic configuration on filer
	if err := b.fca.SaveTopicConfToFiler(t, resp); err != nil {
		return nil, fmt.Errorf("configure topic: %v", err)
	}

	b.PubBalancer.OnPartitionChange(request.Topic, resp.BrokerPartitionAssignments)

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, resp.BrokerPartitionAssignments)

	return resp, err
}
