package broker

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SubscriberToSubCoordinator coordinates the subscribers
func (broker *MessageQueueBroker) SubscriberToSubCoordinator(stream mq_pb.SeaweedMessaging_SubscriberToSubCoordinatorServer) error {
	if !broker.lockAsBalancer.IsLocked() {
		return status.Errorf(codes.Unavailable, "not current broker balancer")
	}
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	// process init message
	initMessage := req.GetInit()
	if initMessage != nil {
		broker.Coordinator.AddSubscriber(initMessage.ConsumerGroup, initMessage.ConsumerInstanceId, initMessage.Topic)
	} else {
		return status.Errorf(codes.InvalidArgument, "subscriber init message is empty")
	}
	defer func() {
		broker.Coordinator.RemoveSubscriber(initMessage.ConsumerGroup, initMessage.ConsumerInstanceId, initMessage.Topic)
	}()

	// process ack messages
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
	}

	return nil
}
