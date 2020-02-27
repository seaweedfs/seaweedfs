package weed_server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb/queue_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type QueueServerOption struct {
	Filers             []string
	DefaultReplication string
	MaxMB              int
	Port               int
}

type QueueServer struct {
	option         *QueueServerOption
	grpcDialOption grpc.DialOption
}

func (q *QueueServer) ConfigureTopic(context.Context, *queue_pb.ConfigureTopicRequest) (*queue_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (q *QueueServer) DeleteTopic(context.Context, *queue_pb.DeleteTopicRequest) (*queue_pb.DeleteTopicResponse, error) {
	panic("implement me")
}

func (q *QueueServer) StreamWrite(queue_pb.SeaweedQueue_StreamWriteServer) error {
	panic("implement me")
}

func (q *QueueServer) StreamRead(*queue_pb.ReadMessageRequest, queue_pb.SeaweedQueue_StreamReadServer) error {
	panic("implement me")
}

func NewQueueServer(option *QueueServerOption) (qs *QueueServer, err error) {

	qs = &QueueServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.queue"),
	}

	return qs, nil
}
