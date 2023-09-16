package broker

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BrokerConnectToBalancer receives connections from brokers and collects stats
func (broker *MessageQueueBroker) ConnectToBalancer(stream mq_pb.SeaweedMessaging_ConnectToBalancerServer) error {
	if !broker.lockAsBalancer.IsLocked() {
		return status.Errorf(codes.Unavailable, "not current broker balancer")
	}
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	// process init message
	initMessage := req.GetInit()
	brokerStats := balancer.NewBrokerStats()
	if initMessage != nil {
		broker.Balancer.Brokers.Set(initMessage.Broker, brokerStats)
	} else {
		return status.Errorf(codes.InvalidArgument, "balancer init message is empty")
	}
	defer func() {
		broker.Balancer.Brokers.Remove(initMessage.Broker)
	}()

	// process stats message
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if !broker.lockAsBalancer.IsLocked() {
			return status.Errorf(codes.Unavailable, "not current broker balancer")
		}
		if receivedStats := req.GetStats(); receivedStats != nil {
			brokerStats.TopicPartitionCount = receivedStats.TopicPartitionCount
			brokerStats.ConsumerCount = receivedStats.ConsumerCount
			brokerStats.CpuUsagePercent = receivedStats.CpuUsagePercent

			glog.V(3).Infof("broker %s stats: %+v", initMessage.Broker, brokerStats)
		}
	}

	return nil
}
