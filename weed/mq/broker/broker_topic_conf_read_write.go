package broker

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/logstore"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) GetOrGenerateLocalPartition(t topic.Topic, partition topic.Partition) (localTopicPartition *topic.LocalPartition, getOrGenError error) {
	// get or generate a local partition
	conf, readConfErr := b.fca.ReadTopicConfFromFiler(t)
	if readConfErr != nil {
		glog.Errorf("topic %v not found: %v", t, readConfErr)
		return nil, fmt.Errorf("topic %v not found: %w", t, readConfErr)
	}
	localTopicPartition, _, getOrGenError = b.doGetOrGenLocalPartition(t, partition, conf)
	if getOrGenError != nil {
		glog.Errorf("topic %v partition %v not setup: %v", t, partition, getOrGenError)
		return nil, fmt.Errorf("topic %v partition %v not setup: %w", t, partition, getOrGenError)
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
	glog.V(0).Infof("🔍 DEBUG: genLocalPartitionFromFiler for %s %s, self=%s", t, partition, self)
	glog.V(0).Infof("🔍 DEBUG: conf.BrokerPartitionAssignments: %v", conf.BrokerPartitionAssignments)
	for _, assignment := range conf.BrokerPartitionAssignments {
		assignmentPartition := topic.FromPbPartition(assignment.Partition)
		glog.V(0).Infof("🔍 DEBUG: checking assignment: LeaderBroker=%s, Partition=%s", assignment.LeaderBroker, assignmentPartition)
		glog.V(0).Infof("🔍 DEBUG: comparing self=%s with LeaderBroker=%s: %v", self, assignment.LeaderBroker, assignment.LeaderBroker == string(self))
		glog.V(0).Infof("🔍 DEBUG: comparing partition=%s with assignmentPartition=%s: %v", partition.String(), assignmentPartition.String(), partition.Equals(assignmentPartition))
		glog.V(0).Infof("🔍 DEBUG: logical comparison (RangeStart, RangeStop only): %v", partition.LogicalEquals(assignmentPartition))
		glog.V(0).Infof("🔍 DEBUG: partition details: RangeStart=%d, RangeStop=%d, RingSize=%d, UnixTimeNs=%d", partition.RangeStart, partition.RangeStop, partition.RingSize, partition.UnixTimeNs)
		glog.V(0).Infof("🔍 DEBUG: assignmentPartition details: RangeStart=%d, RangeStop=%d, RingSize=%d, UnixTimeNs=%d", assignmentPartition.RangeStart, assignmentPartition.RangeStop, assignmentPartition.RingSize, assignmentPartition.UnixTimeNs)
		if assignment.LeaderBroker == string(self) && partition.LogicalEquals(assignmentPartition) {
			glog.V(0).Infof("🔍 DEBUG: Creating local partition for %s %s", t, partition)
			localPartition = topic.NewLocalPartition(partition, b.genLogFlushFunc(t, partition), logstore.GenMergedReadFunc(b, t, partition))
			b.localTopicManager.AddLocalPartition(t, localPartition)
			isGenerated = true
			glog.V(0).Infof("🔍 DEBUG: Successfully added local partition %s %s to localTopicManager", t, partition)
			break
		}
	}

	if !isGenerated {
		glog.V(0).Infof("🔍 DEBUG: No matching assignment found for %s %s", t, partition)
	}

	return localPartition, isGenerated, nil
}

func (b *MessageQueueBroker) ensureTopicActiveAssignments(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) (err error) {
	// also fix assignee broker if invalid
	hasChanges := pub_balancer.EnsureAssignmentsToActiveBrokers(b.PubBalancer.Brokers, 1, conf.BrokerPartitionAssignments)
	if hasChanges {
		glog.V(0).Infof("topic %v partition updated assignments: %v", t, conf.BrokerPartitionAssignments)
		if err = b.fca.SaveTopicConfToFiler(t, conf); err != nil {
			return err
		}
	}

	return err
}
