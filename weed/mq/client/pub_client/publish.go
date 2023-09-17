package pub_client

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (p *TopicPublisher) Publish(key, value []byte) error {
	hashKey := util.HashToInt32(key) % balancer.MaxPartitionCount
	if hashKey < 0 {
		hashKey = -hashKey
	}
	publishClient, found := p.partition2Broker.Floor(hashKey, hashKey)
	if !found {
		return fmt.Errorf("no broker found for key %d", hashKey)
	}
	p.Lock()
	defer p.Unlock()
	// dead lock here
	//google.golang.org/grpc/internal/transport.(*writeQuota).get(flowcontrol.go:59)
	//google.golang.org/grpc/internal/transport.(*http2Client).Write(http2_client.go:1047)
	//google.golang.org/grpc.(*csAttempt).sendMsg(stream.go:1040)
	//google.golang.org/grpc.(*clientStream).SendMsg.func2(stream.go:892)
	//google.golang.org/grpc.(*clientStream).withRetry(stream.go:752)
	//google.golang.org/grpc.(*clientStream).SendMsg(stream.go:894)
	//github.com/seaweedfs/seaweedfs/weed/pb/mq_pb.(*seaweedMessagingPublishClient).Send(mq_grpc.pb.go:141)
	//github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client.(*TopicPublisher).Publish(publish.go:19)
	if err := publishClient.Send(&mq_pb.PublishRequest{
		Message: &mq_pb.PublishRequest_Data{
			Data: &mq_pb.DataMessage{
				Key:   key,
				Value: value,
			},
		},
	}); err != nil {
		return fmt.Errorf("send publish request: %v", err)
	}
	return nil
}
