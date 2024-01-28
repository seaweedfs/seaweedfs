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

// readTopicConfFromFiler reads the topic configuration from filer
// this should only be run in broker leader, to ensure correct active broker list.
func (b *MessageQueueBroker) readTopicConfFromFiler(t topic.Topic) (conf *mq_pb.ConfigureTopicResponse, err error) {

	glog.V(0).Infof("load conf for topic %v from filer", t)

	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	if err = b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
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

func (b *MessageQueueBroker) GetOrGenLocalPartition(t topic.Topic, partition topic.Partition) (localPartition *topic.LocalPartition, err error) {
	b.accessLock.Lock()
	defer b.accessLock.Unlock()

	if localPartition = b.localTopicManager.GetTopicPartition(t, partition); localPartition == nil {
		localPartition, err = b.genLocalPartitionFromFiler(t, partition)
		if err != nil {
			return nil, err
		}
	}
	return localPartition, nil
}

func (b *MessageQueueBroker) genLocalPartitionFromFiler(t topic.Topic, partition topic.Partition) (localPartition *topic.LocalPartition, err error) {
	self := b.option.BrokerAddress()
	conf, err := b.readTopicConfFromFiler(t)
	if err != nil {
		return nil, err
	}
	for _, assignment := range conf.BrokerPartitionAssignments {
		if assignment.LeaderBroker == string(self) && partition.Equals(topic.FromPbPartition(assignment.Partition)) {
			localPartition = topic.FromPbBrokerPartitionAssignment(b.option.BrokerAddress(), partition, assignment, b.genLogFlushFunc(t, assignment.Partition), b.genLogOnDiskReadFunc(t, assignment.Partition))
			b.localTopicManager.AddTopicPartition(t, localPartition)
			break
		}
	}

	return localPartition, nil
}

func (b *MessageQueueBroker) ensureTopicActiveAssignments(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) (err error) {
	// also fix assignee broker if invalid
	addedAssignments, updatedAssignments := pub_balancer.EnsureAssignmentsToActiveBrokers(b.Balancer.Brokers, conf.BrokerPartitionAssignments)
	if len(addedAssignments) > 0 || len(updatedAssignments) > 0 {
		glog.V(0).Infof("topic %v partition assignments added: %v updated: %v", t, addedAssignments, updatedAssignments)
		if err = b.saveTopicConfToFiler(t.ToPbTopic(), conf); err != nil {
			return err
		}
	}

	return err
}
