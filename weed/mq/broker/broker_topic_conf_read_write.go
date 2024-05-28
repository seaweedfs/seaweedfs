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

func (b *MessageQueueBroker) GetOrGenerateLocalPartition(t topic.Topic, partition topic.Partition) (localTopicPartition *topic.LocalPartition, getOrGenError error) {
	// get or generate a local partition
	conf, readConfErr := b.readTopicConfFromFiler(t)
	if readConfErr != nil {
		glog.Errorf("topic %v not found: %v", t, readConfErr)
		return nil, fmt.Errorf("topic %v not found: %v", t, readConfErr)
	}
	localTopicPartition, _, getOrGenError = b.doGetOrGenLocalPartition(t, partition, conf)
	if getOrGenError != nil {
		glog.Errorf("topic %v partition %v not setup: %v", t, partition, getOrGenError)
		return nil, fmt.Errorf("topic %v partition %v not setup: %v", t, partition, getOrGenError)
	}
	return localTopicPartition, nil
}

func (b *MessageQueueBroker) doGetOrGenLocalPartition(t topic.Topic, partition topic.Partition, conf *mq_pb.ConfigureTopicResponse) (localPartition *topic.LocalPartition, isGenerated bool, err error) {
	b.accessLock.Lock()
	defer b.accessLock.Unlock()

	if localPartition = b.localTopicManager.GetLocalPartition(t, partition); localPartition == nil {
		localPartition, isGenerated, err = b.genLocalPartitionFromFiler(t, partition, conf)
		if err != nil {
			return nil, false, err
		}
	}
	return localPartition, isGenerated, nil
}

func (b *MessageQueueBroker) genLocalPartitionFromFiler(t topic.Topic, partition topic.Partition, conf *mq_pb.ConfigureTopicResponse) (localPartition *topic.LocalPartition, isGenerated bool, err error) {
	self := b.option.BrokerAddress()
	for _, assignment := range conf.BrokerPartitionAssignments {
		if assignment.LeaderBroker == string(self) && partition.Equals(topic.FromPbPartition(assignment.Partition)) {
			localPartition = topic.NewLocalPartition(partition, b.genLogFlushFunc(t, assignment.Partition), b.genLogOnDiskReadFunc(t, assignment.Partition))
			b.localTopicManager.AddLocalPartition(t, localPartition)
			isGenerated = true
			break
		}
	}

	return localPartition, isGenerated, nil
}

func (b *MessageQueueBroker) ensureTopicActiveAssignments(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) (err error) {
	// also fix assignee broker if invalid
	hasChanges := pub_balancer.EnsureAssignmentsToActiveBrokers(b.Balancer.Brokers, 1, conf.BrokerPartitionAssignments)
	if hasChanges {
		glog.V(0).Infof("topic %v partition updated assignments: %v", t, conf.BrokerPartitionAssignments)
		if err = b.saveTopicConfToFiler(t.ToPbTopic(), conf); err != nil {
			return err
		}
	}

	return err
}
