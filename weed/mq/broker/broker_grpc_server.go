package broker

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/mq_pb"
)

func (broker *MessageQueueBroker) ConfigureTopic(c context.Context, request *mq_pb.ConfigureTopicRequest) (*mq_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageQueueBroker) DeleteTopic(c context.Context, request *mq_pb.DeleteTopicRequest) (*mq_pb.DeleteTopicResponse, error) {
	resp := &mq_pb.DeleteTopicResponse{}
	dir, entry := genTopicDirEntry(request.Namespace, request.Topic)
	if exists, err := filer_pb.Exists(broker, dir, entry, true); err != nil {
		return nil, err
	} else if exists {
		err = filer_pb.Remove(broker, dir, entry, true, true, true, false, nil)
	}
	return resp, nil
}

func (broker *MessageQueueBroker) GetTopicConfiguration(c context.Context, request *mq_pb.GetTopicConfigurationRequest) (*mq_pb.GetTopicConfigurationResponse, error) {
	panic("implement me")
}

func genTopicDir(namespace, topic string) string {
	return fmt.Sprintf("%s/%s/%s", filer.TopicsDir, namespace, topic)
}

func genTopicDirEntry(namespace, topic string) (dir, entry string) {
	return fmt.Sprintf("%s/%s", filer.TopicsDir, namespace), topic
}
