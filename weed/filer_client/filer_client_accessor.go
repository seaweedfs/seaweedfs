package filer_client

import (
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

type FilerClientAccessor struct {
	GetFiler          func() pb.ServerAddress
	GetGrpcDialOption func() grpc.DialOption
}

func (fca *FilerClientAccessor) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithFilerClient(streamingMode, 0, fca.GetFiler(), fca.GetGrpcDialOption(), fn)
}

func (fca *FilerClientAccessor) SaveTopicConfToFiler(t *mq_pb.Topic, conf *mq_pb.ConfigureTopicResponse) error {

	glog.V(0).Infof("save conf for topic %v to filer", t)

	// save the topic configuration on filer
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	if err := fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		filer.ProtoToText(&buf, conf)
		return filer.SaveInsideFiler(client, topicDir, "topic.conf", buf.Bytes())
	}); err != nil {
		return fmt.Errorf("save topic to %s: %v", topicDir, err)
	}
	return nil
}

func (fca *FilerClientAccessor) ReadTopicConfFromFiler(t topic.Topic) (conf *mq_pb.ConfigureTopicResponse, err error) {

	glog.V(0).Infof("load conf for topic %v from filer", t)

	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	if err = fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, topicDir, "topic.conf")
		if err == filer_pb.ErrNotFound {
			return err
		}
		if err != nil {
			return fmt.Errorf("read topic.conf of %v: %v", t, err)
		}
		// parse into filer conf object
		conf = &mq_pb.ConfigureTopicResponse{}
		if err = jsonpb.Unmarshal(data, conf); err != nil {
			return fmt.Errorf("unmarshal topic %v conf: %v", t, err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return conf, nil
}
