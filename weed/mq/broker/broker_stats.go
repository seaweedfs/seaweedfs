package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
	"math/rand"
	"time"
)

// BrokerConnectToBalancer connects to the broker balancer and sends stats
func (b *MessageQueueBroker) BrokerConnectToBalancer(self string) error {
	// find the lock owner
	var brokerBalancer string
	err := b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.FindLockOwner(context.Background(), &filer_pb.FindLockOwnerRequest{
			Name: pub_balancer.LockBrokerBalancer,
		})
		if err != nil {
			return err
		}
		brokerBalancer = resp.Owner
		return nil
	})
	if err != nil {
		return err
	}
	b.currentBalancer = pb.ServerAddress(brokerBalancer)

	glog.V(0).Infof("broker %s found balancer %s", self, brokerBalancer)
	if brokerBalancer == "" {
		return fmt.Errorf("no balancer found")
	}

	// connect to the lock owner
	err = pb.WithBrokerGrpcClient(false, brokerBalancer, b.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		stream, err := client.PublisherToPubBalancer(context.Background())
		if err != nil {
			return fmt.Errorf("connect to balancer %v: %v", brokerBalancer, err)
		}
		defer stream.CloseSend()
		err = stream.Send(&mq_pb.PublisherToPubBalancerRequest{
			Message: &mq_pb.PublisherToPubBalancerRequest_Init{
				Init: &mq_pb.PublisherToPubBalancerRequest_InitMessage{
					Broker: self,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("send init message: %v", err)
		}

		for {
			stats := b.localTopicManager.CollectStats(time.Second * 5)
			err = stream.Send(&mq_pb.PublisherToPubBalancerRequest{
				Message: &mq_pb.PublisherToPubBalancerRequest_Stats{
					Stats: stats,
				},
			})
			if err != nil {
				if err == io.EOF {
					return err
				}
				return fmt.Errorf("send stats message: %v", err)
			}
			glog.V(3).Infof("sent stats: %+v", stats)

			time.Sleep(time.Millisecond*5000 + time.Duration(rand.Intn(1000))*time.Millisecond)
		}

		return nil
	})

	return err
}
