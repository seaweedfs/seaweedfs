package pub_balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"math/rand"
	"time"
)

func AllocateTopicPartitions(brokers cmap.ConcurrentMap[string, *BrokerStats], partitionCount int32) (assignments []*mq_pb.BrokerPartitionAssignment) {
	// divide the ring into partitions
	now := time.Now().UnixNano()
	rangeSize := MaxPartitionCount / partitionCount
	for i := int32(0); i < partitionCount; i++ {
		assignment := &mq_pb.BrokerPartitionAssignment{
			Partition: &schema_pb.Partition{
				RingSize:   MaxPartitionCount,
				RangeStart: int32(i * rangeSize),
				RangeStop:  int32((i + 1) * rangeSize),
				UnixTimeNs: now,
			},
		}
		if i == partitionCount-1 {
			assignment.Partition.RangeStop = MaxPartitionCount
		}
		assignments = append(assignments, assignment)
	}

	EnsureAssignmentsToActiveBrokers(brokers, 1, assignments)

	glog.V(0).Infof("allocate topic partitions %d: %v", len(assignments), assignments)
	return
}

// randomly pick n brokers, which may contain duplicates
// TODO pick brokers based on the broker stats
func pickBrokers(brokers cmap.ConcurrentMap[string, *BrokerStats], count int32) []string {
	candidates := make([]string, 0, brokers.Count())
	for brokerStatsItem := range brokers.IterBuffered() {
		candidates = append(candidates, brokerStatsItem.Key)
	}
	pickedBrokers := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		p := rand.Intn(len(candidates))
		pickedBrokers = append(pickedBrokers, candidates[p])
	}
	return pickedBrokers
}

// reservoir sampling select N brokers from the active brokers, with exclusion of the excluded broker
func pickBrokersExcluded(brokers []string, count int, excludedLeadBroker string, excludedBroker string) []string {
	pickedBrokers := make([]string, 0, count)
	for i, broker := range brokers {
		if broker == excludedBroker {
			continue
		}
		if len(pickedBrokers) < count {
			pickedBrokers = append(pickedBrokers, broker)
		} else {
			j := rand.Intn(i + 1)
			if j < count {
				pickedBrokers[j] = broker
			}
		}
	}

	// shuffle the picked brokers
	count = len(pickedBrokers)
	for i := 0; i < count; i++ {
		j := rand.Intn(count)
		pickedBrokers[i], pickedBrokers[j] = pickedBrokers[j], pickedBrokers[i]
	}

	return pickedBrokers
}

// EnsureAssignmentsToActiveBrokers ensures the assignments are assigned to active brokers
func EnsureAssignmentsToActiveBrokers(activeBrokers cmap.ConcurrentMap[string, *BrokerStats], followerCount int, assignments []*mq_pb.BrokerPartitionAssignment) (hasChanges bool) {
	glog.V(0).Infof("EnsureAssignmentsToActiveBrokers: activeBrokers: %v, followerCount: %d, assignments: %v", activeBrokers.Count(), followerCount, assignments)

	candidates := make([]string, 0, activeBrokers.Count())
	for brokerStatsItem := range activeBrokers.IterBuffered() {
		candidates = append(candidates, brokerStatsItem.Key)
	}

	for _, assignment := range assignments {
		// count how many brokers are needed
		count := 0
		if assignment.LeaderBroker == "" {
			count++
		} else if _, found := activeBrokers.Get(assignment.LeaderBroker); !found {
			assignment.LeaderBroker = ""
			count++
		}
		if assignment.FollowerBroker == "" {
			count++
		} else if _, found := activeBrokers.Get(assignment.FollowerBroker); !found {
			assignment.FollowerBroker = ""
			count++
		}

		if count > 0 {
			pickedBrokers := pickBrokersExcluded(candidates, count, assignment.LeaderBroker, assignment.FollowerBroker)
			i := 0
			if assignment.LeaderBroker == "" {
				if i < len(pickedBrokers) {
					assignment.LeaderBroker = pickedBrokers[i]
					i++
					hasChanges = true
				}
			}
			if assignment.FollowerBroker == "" {
				if i < len(pickedBrokers) {
					assignment.FollowerBroker = pickedBrokers[i]
					i++
					hasChanges = true
				}
			}
		}

	}

	glog.V(0).Infof("EnsureAssignmentsToActiveBrokers: activeBrokers: %v, followerCount: %d, assignments: %v hasChanges: %v", activeBrokers.Count(), followerCount, assignments, hasChanges)
	return
}
