package agent

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func (a *MessageQueueAgent) SubscribeRecord(stream mq_agent_pb.SeaweedMessagingAgent_SubscribeRecordServer) error {
	// the first message is the subscribe request
	// it should only contain the session id
	initMessage, err := stream.Recv()
	if err != nil {
		return err
	}

	subscriber := a.handleInitSubscribeRecordRequest(stream.Context(), initMessage.Init)

	var lastErr error
	executors := util.NewLimitedConcurrentExecutor(int(subscriber.SubscriberConfig.SlidingWindowSize))
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		executors.Execute(func() {
			record := &schema_pb.RecordValue{}
			err := proto.Unmarshal(m.Data.Value, record)
			if err != nil {
				glog.V(0).Infof("unmarshal record value: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				return
			}
			if sendErr := stream.Send(&mq_agent_pb.SubscribeRecordResponse{
				Key:   m.Data.Key,
				Value: record,
				TsNs:  m.Data.TsNs,
			}); sendErr != nil {
				glog.V(0).Infof("send record: %v", sendErr)
				if lastErr == nil {
					lastErr = sendErr
				}
			}
		})
	})

	go func() {
		subErr := subscriber.Subscribe()
		if subErr != nil {
			glog.V(0).Infof("subscriber %s subscribe: %v", subscriber.SubscriberConfig.String(), subErr)
			if lastErr == nil {
				lastErr = subErr
			}
		}
	}()

	for {
		m, err := stream.Recv()
		if err != nil {
			glog.V(0).Infof("subscriber %s receive: %v", subscriber.SubscriberConfig.String(), err)
			return err
		}
		if m != nil {
			subscriber.PartitionOffsetChan <- sub_client.KeyedOffset{
				Key:    m.AckKey,
				Offset: m.AckSequence,
			}
		}
	}
}

func (a *MessageQueueAgent) handleInitSubscribeRecordRequest(ctx context.Context, req *mq_agent_pb.SubscribeRecordRequest_InitSubscribeRecordRequest) *sub_client.TopicSubscriber {

	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           req.ConsumerGroup,
		ConsumerGroupInstanceId: req.ConsumerGroupInstanceId,
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		MaxPartitionCount:       req.MaxSubscribedPartitions,
		SlidingWindowSize:       req.SlidingWindowSize,
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:            topic.FromPbTopic(req.Topic),
		Filter:           req.Filter,
		PartitionOffsets: req.PartitionOffsets,
		OffsetType:       req.OffsetType,
		OffsetTsNs:       req.OffsetTsNs,
	}

	topicSubscriber := sub_client.NewTopicSubscriber(
		ctx,
		a.brokersList(),
		subscriberConfig,
		contentConfig,
		make(chan sub_client.KeyedOffset, 1024),
	)

	return topicSubscriber
}
