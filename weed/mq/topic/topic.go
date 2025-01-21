package topic

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
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
func FromPbTopic(topic *schema_pb.Topic) Topic {
	return Topic{
		Namespace: topic.Namespace,
		Name:      topic.Name,
	}
}

func (t Topic) ToPbTopic() *schema_pb.Topic {
	return &schema_pb.Topic{
		Namespace: t.Namespace,
		Name:      t.Name,
	}
}

func (t Topic) String() string {
	return fmt.Sprintf("%s.%s", t.Namespace, t.Name)
}

func (t Topic) Dir() string {
	return fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
}

func (t Topic) ReadConfFile(client filer_pb.SeaweedFilerClient) (*mq_pb.ConfigureTopicResponse, error) {
	data, err := filer.ReadInsideFiler(client, t.Dir(), filer.TopicConfFile)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read topic.conf of %v: %v", t, err)
	}
	// parse into filer conf object
	conf := &mq_pb.ConfigureTopicResponse{}
	if err = jsonpb.Unmarshal(data, conf); err != nil {
		return nil, fmt.Errorf("unmarshal topic %v conf: %v", t, err)
	}
	return conf, nil
}

func (t Topic) WriteConfFile(client filer_pb.SeaweedFilerClient, conf *mq_pb.ConfigureTopicResponse) error {
	var buf bytes.Buffer
	filer.ProtoToText(&buf, conf)
	if err := filer.SaveInsideFiler(client, t.Dir(), filer.TopicConfFile, buf.Bytes()); err != nil {
		return fmt.Errorf("save topic %v conf: %v", t, err)
	}
	return nil
}
