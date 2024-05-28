package topic

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type Topic struct {
	Namespace string
	Name      string
}

func NewTopic(namespace string, name string) Topic {
	return Topic{
		Namespace: namespace,
		Name:      name,
	}
}
func FromPbTopic(topic *mq_pb.Topic) Topic {
	return Topic{
		Namespace: topic.Namespace,
		Name:      topic.Name,
	}
}

func (tp Topic) ToPbTopic() *mq_pb.Topic {
	return &mq_pb.Topic{
		Namespace: tp.Namespace,
		Name:      tp.Name,
	}
}

func (tp Topic) String() string {
	return fmt.Sprintf("%s.%s", tp.Namespace, tp.Name)
}
