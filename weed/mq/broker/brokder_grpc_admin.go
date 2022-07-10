package broker

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/mq_pb"
)

func (broker *MessageQueueBroker) FindBrokerLeader(c context.Context, request *mq_pb.FindBrokerLeaderRequest) (*mq_pb.FindBrokerLeaderResponse, error) {
	panic("implement me")
}
