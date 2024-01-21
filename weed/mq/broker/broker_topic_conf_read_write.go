package broker

import (
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

func (b *MessageQueueBroker) saveTopicConfToFiler(t *mq_pb.Topic, conf *mq_pb.ConfigureTopicResponse) error {

	glog.V(0).Infof("save conf for topic %v to filer", t)

	// save the topic configuration on filer
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	if err := b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		filer.ProtoToText(&buf, conf)
		return filer.SaveInsideFiler(client, topicDir, "topic.conf", buf.Bytes())
	}); err != nil {
		return fmt.Errorf("save topic to %s: %v", topicDir, err)
	}
	return nil
}

func (b *MessageQueueBroker) readTopicConfFromFiler(t topic.Topic) (conf *mq_pb.ConfigureTopicResponse, err error) {

	glog.V(0).Infof("load conf for topic %v from filer", t)

	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	if err = b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, topicDir, "topic.conf")
		if err != nil {
			return fmt.Errorf("read topic %v partition %v conf: %v", t, err)
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

	// also fix assignee broker if invalid
	changedAssignments := pub_balancer.EnsureAssignmentsToActiveBrokers(b.Balancer.Brokers, conf.BrokerPartitionAssignments)
	if len(changedAssignments) > 0 {
		glog.V(0).Infof("topic %v partition assignments changed: %v", t, changedAssignments)
		if err = b.saveTopicConfToFiler(t.ToPbTopic(), conf); err != nil {
			return nil, err
		}
	}
	
	return conf, err
}
