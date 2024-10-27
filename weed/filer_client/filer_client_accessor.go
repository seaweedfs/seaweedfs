package filer_client

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

type FilerClientAccessor struct {
	GetFiler          func() pb.ServerAddress
	GetGrpcDialOption func() grpc.DialOption
}

func (fca *FilerClientAccessor) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithFilerClient(streamingMode, 0, fca.GetFiler(), fca.GetGrpcDialOption(), fn)
}

func (fca *FilerClientAccessor) SaveTopicConfToFiler(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) error {

	glog.V(0).Infof("save conf for topic %v to filer", t)

	// save the topic configuration on filer
	return fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return t.WriteConfFile(client, conf)
	})
}

func (fca *FilerClientAccessor) ReadTopicConfFromFiler(t topic.Topic) (conf *mq_pb.ConfigureTopicResponse, err error) {

	glog.V(1).Infof("load conf for topic %v from filer", t)

	if err = fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		conf, err = t.ReadConfFile(client)
		return err
	}); err != nil {
		return nil, err
	}

	return conf, nil
}
