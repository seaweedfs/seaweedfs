package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"time"
)

func (sub *TopicSubscriber) doKeepConnectedToSubCoordinator() {
	waitTime := 1 * time.Second
	for {
		for _, broker := range sub.bootstrapBrokers {

			select {
			case <-sub.ctx.Done():
				return
			default:
			}

			// lookup topic brokers
			var brokerLeader string
			err := pb.WithBrokerGrpcClient(false, broker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
				resp, err := client.FindBrokerLeader(sub.ctx, &mq_pb.FindBrokerLeaderRequest{})
				if err != nil {
					return err
				}
				brokerLeader = resp.Broker
				return nil
			})
			if err != nil {
				glog.V(0).Infof("broker coordinator on %s: %v", broker, err)
				continue
			}
			glog.V(0).Infof("found broker coordinator: %v", brokerLeader)

			// connect to the balancer
			pb.WithBrokerGrpcClient(true, brokerLeader, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {

				stream, err := client.SubscriberToSubCoordinator(sub.ctx)
				if err != nil {
					glog.V(0).Infof("subscriber %s: %v", sub.ContentConfig.Topic, err)
					return err
				}
				waitTime = 1 * time.Second

				// Maybe later: subscribe to multiple topics instead of just one

				if err := stream.Send(&mq_pb.SubscriberToSubCoordinatorRequest{
					Message: &mq_pb.SubscriberToSubCoordinatorRequest_Init{
						Init: &mq_pb.SubscriberToSubCoordinatorRequest_InitMessage{
							ConsumerGroup:           sub.SubscriberConfig.ConsumerGroup,
							ConsumerGroupInstanceId: sub.SubscriberConfig.ConsumerGroupInstanceId,
							Topic:                   sub.ContentConfig.Topic.ToPbTopic(),
							MaxPartitionCount:       sub.SubscriberConfig.MaxPartitionCount,
						},
					},
				}); err != nil {
					glog.V(0).Infof("subscriber %s send init: %v", sub.ContentConfig.Topic, err)
					return err
				}

				go func() {
					for reply := range sub.brokerPartitionAssignmentAckChan {

						select {
						case <-sub.ctx.Done():
							return
						default:
						}

						glog.V(0).Infof("subscriber instance %s ack %+v", sub.SubscriberConfig.ConsumerGroupInstanceId, reply)
						if err := stream.Send(reply); err != nil {
							glog.V(0).Infof("subscriber %s reply: %v", sub.ContentConfig.Topic, err)
							return
						}
					}
				}()

				// keep receiving messages from the sub coordinator
				for {
					resp, err := stream.Recv()
					if err != nil {
						glog.V(0).Infof("subscriber %s receive: %v", sub.ContentConfig.Topic, err)
						return err
					}

					select {
					case <-sub.ctx.Done():
						return nil
					default:
					}

					sub.brokerPartitionAssignmentChan <- resp
					glog.V(0).Infof("Received assignment: %+v", resp)
				}

				return nil
			})
		}
		glog.V(0).Infof("subscriber %s/%s waiting for more assignments", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup)
		if waitTime < 10*time.Second {
			waitTime += 1 * time.Second
		}
		time.Sleep(waitTime)
	}
}
