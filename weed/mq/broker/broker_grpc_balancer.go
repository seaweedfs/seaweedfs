package broker

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (broker *MessageQueueBroker) ConnectToBalancer(stream mq_pb.SeaweedMessaging_ConnectToBalancerServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	response := &mq_pb.ConnectToBalancerResponse{}
	initMessage := req.GetInit()
	brokerStats := balancer.NewBrokerStats()
	if initMessage != nil {
		broker.Balancer.Brokers.Set(initMessage.Broker, brokerStats)
	} else {
		response.Error = "balancer init message is empty"
		return stream.Send(response)
	}
	defer func() {
		broker.Balancer.Brokers.Remove(initMessage.Broker)
	}()
	stream.Send(response)

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if receivedStats := req.GetStats(); receivedStats != nil {
			brokerStats.TopicPartitionCount = receivedStats.TopicPartitionCount
			brokerStats.MessageCount = receivedStats.MessageCount
			brokerStats.BytesCount = receivedStats.BytesCount
			brokerStats.CpuUsagePercent = receivedStats.CpuUsagePercent
		}
	}

	return nil
}
