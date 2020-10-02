package broker

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (broker *MessageBroker) ConfigureTopic(c context.Context, request *messaging_pb.ConfigureTopicRequest) (*messaging_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageBroker) DeleteTopic(c context.Context, request *messaging_pb.DeleteTopicRequest) (*messaging_pb.DeleteTopicResponse, error) {
	resp := &messaging_pb.DeleteTopicResponse{}
	dir, entry := genTopicDirEntry(request.Namespace, request.Topic)
	if exists, err := filer_pb.Exists(broker, dir, entry, true); err != nil {
		return nil, err
	} else if exists {
		err = filer_pb.Remove(broker, dir, entry, true, true, true, false, nil)
	}
	return resp, nil
}

func (broker *MessageBroker) GetTopicConfiguration(c context.Context, request *messaging_pb.GetTopicConfigurationRequest) (*messaging_pb.GetTopicConfigurationResponse, error) {
	panic("implement me")
}

func genTopicDir(namespace, topic string) string {
	return fmt.Sprintf("%s/%s/%s", filer.TopicsDir, namespace, topic)
}

func genTopicDirEntry(namespace, topic string) (dir, entry string) {
	return fmt.Sprintf("%s/%s", filer.TopicsDir, namespace), topic
}
