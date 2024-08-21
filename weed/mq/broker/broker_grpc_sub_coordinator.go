package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/sub_coordinator"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SubscriberToSubCoordinator coordinates the subscribers
func (b *MessageQueueBroker) SubscriberToSubCoordinator(stream mq_pb.SeaweedMessaging_SubscriberToSubCoordinatorServer) error {
	if !b.isLockOwner() {
		return status.Errorf(codes.Unavailable, "not current broker balancer")
	}
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	var cgi *sub_coordinator.ConsumerGroupInstance
	var cg *sub_coordinator.ConsumerGroup
	// process init message
	initMessage := req.GetInit()
	if initMessage != nil {
		cg, cgi, err = b.SubCoordinator.AddSubscriber(initMessage)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}
		glog.V(0).Infof("subscriber %s/%s/%s connected", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic)
	} else {
		return status.Errorf(codes.InvalidArgument, "subscriber init message is empty")
	}
	defer func() {
		b.SubCoordinator.RemoveSubscriber(initMessage)
		glog.V(0).Infof("subscriber %s/%s/%s disconnected: %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, err)
	}()

	ctx := stream.Context()

	go func() {
		// process ack messages
		for {
			req, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("subscriber %s/%s/%s receive: %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, err)
			}

			if ackUnAssignment := req.GetAckUnAssignment(); ackUnAssignment != nil {
				glog.V(0).Infof("subscriber %s/%s/%s ack close of %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, ackUnAssignment)
				cg.AckUnAssignment(cgi, ackUnAssignment)
			}
			if ackAssignment := req.GetAckAssignment(); ackAssignment != nil {
				glog.V(0).Infof("subscriber %s/%s/%s ack assignment %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, ackAssignment)
				cg.AckAssignment(cgi, ackAssignment)
			}

			select {
			case <-ctx.Done():
				err := ctx.Err()
				if err == context.Canceled {
					// Client disconnected
					return
				}
				return
			default:
				// Continue processing the request
			}
		}
	}()

	// send commands to subscriber
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.Canceled {
				// Client disconnected
				return err
			}
			glog.V(0).Infof("subscriber %s/%s/%s disconnected: %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, err)
			return err
		case message := <-cgi.ResponseChan:
			glog.V(0).Infof("subscriber %s/%s/%s send: %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, message)
			if err := stream.Send(message); err != nil {
				glog.V(0).Infof("subscriber %s/%s/%s send: %v", initMessage.ConsumerGroup, initMessage.ConsumerGroupInstanceId, initMessage.Topic, err)
			}
		}
	}
}
