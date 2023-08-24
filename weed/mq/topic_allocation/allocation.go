package topic_allocation

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"modernc.org/mathutil"
)

const (
	DefaultBrokerCount = 4
)

// AllocateBrokersForTopicPartitions allocate brokers for a topic's all partitions
func AllocateBrokersForTopicPartitions(t topic.Topic, prevAssignment *mq_pb.TopicPartitionsAssignment, candidateBrokers []pb.ServerAddress) (assignment *mq_pb.TopicPartitionsAssignment, err error) {
	// create a previous assignment if not exists
	if prevAssignment == nil || len(prevAssignment.BrokerPartitions) == 0 {
		prevAssignment = &mq_pb.TopicPartitionsAssignment{
			PartitionCount: topic.PartitionCount,
		}
		partitionCountForEachBroker := topic.PartitionCount / DefaultBrokerCount
		for i := 0; i < DefaultBrokerCount; i++ {
			prevAssignment.BrokerPartitions = append(prevAssignment.BrokerPartitions, &mq_pb.BrokerPartitionsAssignment{
				PartitionStart: int32(i * partitionCountForEachBroker),
				PartitionStop:  mathutil.MaxInt32(int32((i+1)*partitionCountForEachBroker), topic.PartitionCount),
			})
		}
	}

	// create a new assignment
	assignment = &mq_pb.TopicPartitionsAssignment{
		PartitionCount: prevAssignment.PartitionCount,
	}

	// allocate partitions for each partition range
	for _, brokerPartition := range prevAssignment.BrokerPartitions {
		// allocate partitions for each partition range
		leader, followers, err := allocateBrokersForOneTopicPartition(t, brokerPartition, candidateBrokers)
		if err != nil {
			return nil, err
		}

		followerBrokers := make([]string, len(followers))
		for i, follower := range followers {
			followerBrokers[i] = string(follower)
		}

		assignment.BrokerPartitions = append(assignment.BrokerPartitions, &mq_pb.BrokerPartitionsAssignment{
			PartitionStart:  brokerPartition.PartitionStart,
			PartitionStop:   brokerPartition.PartitionStop,
			LeaderBroker:    string(leader),
			FollowerBrokers: followerBrokers,
		})
	}

	return
}

func allocateBrokersForOneTopicPartition(t topic.Topic, brokerPartition *mq_pb.BrokerPartitionsAssignment, candidateBrokers []pb.ServerAddress) (leader pb.ServerAddress, followers []pb.ServerAddress, err error) {
	// allocate leader
	leader, err = allocateLeaderForOneTopicPartition(t, brokerPartition, candidateBrokers)
	if err != nil {
		return
	}

	// allocate followers
	followers, err = allocateFollowersForOneTopicPartition(t, brokerPartition, candidateBrokers)
	if err != nil {
		return
	}

	return
}

func allocateFollowersForOneTopicPartition(t topic.Topic, partition *mq_pb.BrokerPartitionsAssignment, brokers []pb.ServerAddress) (followers []pb.ServerAddress, err error) {
	return
}

func allocateLeaderForOneTopicPartition(t topic.Topic, partition *mq_pb.BrokerPartitionsAssignment, brokers []pb.ServerAddress) (leader pb.ServerAddress, err error) {
	return
}
