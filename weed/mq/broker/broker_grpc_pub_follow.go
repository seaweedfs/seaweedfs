package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) PublishFollowMe(c context.Context, request *mq_pb.PublishFollowMeRequest) (*mq_pb.PublishFollowMeResponse, error) {
	glog.V(0).Infof("PublishFollowMe %v", request)
	return &mq_pb.PublishFollowMeResponse{}, nil
}
