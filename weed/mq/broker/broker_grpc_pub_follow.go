package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
	"math/rand"
	"time"
)

func (b *MessageQueueBroker)  PublishFollowMe(c context.Context, request *mq_pb.PublishFollowMeRequest) (*mq_pb.PublishFollowMeResponse, error){
	glog.V(0).Infof("PublishFollowMe %v", request)
	go b.withBrokerClient(true, pb.ServerAddress(request.BrokerSelf), func(client mq_pb.SeaweedMessagingClient) error {
		followerId := rand.Int31()
		subscribeClient, err := client.FollowInMemoryMessages(context.Background(), &mq_pb.FollowInMemoryMessagesRequest{
			Message: &mq_pb.FollowInMemoryMessagesRequest_Init{
				Init: &mq_pb.FollowInMemoryMessagesRequest_InitMessage{
					ConsumerGroup: string(b.option.BrokerAddress()),
					ConsumerId:    fmt.Sprintf("followMe-%d", followerId),
					FollowerId:    followerId,
					Topic:         request.Topic,
					PartitionOffset: &mq_pb.PartitionOffset{
						Partition: request.Partition,
						StartTsNs: 0,
						StartType: mq_pb.PartitionOffsetStartType_EARLIEST_IN_MEMORY,
					},
				},
			},
		})
		if err != nil {
			glog.Errorf("FollowInMemoryMessages error: %v", err)
			return err
		}

		b.doFollowInMemoryMessage(context.Background(), subscribeClient)

		return nil
	})
	return &mq_pb.PublishFollowMeResponse{}, nil
}

func (b *MessageQueueBroker) doFollowInMemoryMessage(c context.Context, client mq_pb.SeaweedMessaging_FollowInMemoryMessagesClient) {
	for {
		resp, err := client.Recv()
		if err != nil {
			if err != io.EOF {
				glog.V(0).Infof("doFollowInMemoryMessage error: %v", err)
			}
			return
		}
		if resp == nil {
			glog.V(0).Infof("doFollowInMemoryMessage nil response")
			return
		}
		if resp.Message != nil {
			// process ctrl message or data message
			switch m:= resp.Message.(type) {
			case *mq_pb.FollowInMemoryMessagesResponse_Data:
				// process data message
				print("d")
			case *mq_pb.FollowInMemoryMessagesResponse_Ctrl:
				// process ctrl message
				if m.Ctrl.FlushedSequence > 0 {
					flushTime := time.Unix(0, m.Ctrl.FlushedSequence)
					glog.V(0).Infof("doFollowInMemoryMessage flushTime: %v", flushTime)
				}
				if m.Ctrl.FollowerChangedToId != 0 {
					// follower changed
					glog.V(0).Infof("doFollowInMemoryMessage follower changed to %d", m.Ctrl.FollowerChangedToId)
					return
				}
			}
		}
	}
}
