package pub_balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"math/rand"
	"time"
)

func AllocateTopicPartitions(brokers cmap.ConcurrentMap[string, *BrokerStats], partitionCount int32) (assignments []*mq_pb.BrokerPartitionAssignment) {
	// divide the ring into partitions
	now := time.Now().UnixNano()
	rangeSize := MaxPartitionCount / partitionCount
	for i := int32(0); i < partitionCount; i++ {
		assignment := &mq_pb.BrokerPartitionAssignment{
			Partition: &mq_pb.Partition{
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

// reservoir sampling select N brokers from the active brokers, with exclusion of the excluded brokers
func pickBrokersExcluded(brokers []string, count int, excludedLeadBroker string, excludedBrokers []string) []string {
	// convert the excluded brokers to a map
	excludedBrokerMap := make(map[string]bool)
	for _, broker := range excludedBrokers {
		excludedBrokerMap[broker] = true
	}
	if excludedLeadBroker != "" {
		excludedBrokerMap[excludedLeadBroker] = true
	}

	pickedBrokers := make([]string, 0, count)
	for i, broker := range brokers {
		if _, found := excludedBrokerMap[broker]; found {
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
		for i := 0; i < followerCount; i++ {
			if i >= len(assignment.FollowerBrokers) {
				count++
				continue
			}
			if assignment.FollowerBrokers[i] == "" {
				count++
			} else if _, found := activeBrokers.Get(assignment.FollowerBrokers[i]); !found {
				assignment.FollowerBrokers[i] = ""
				count++
			}
		}

		if count > 0 {
			pickedBrokers := pickBrokersExcluded(candidates, count, assignment.LeaderBroker, assignment.FollowerBrokers)
			i := 0
			if assignment.LeaderBroker == "" {
				if i < len(pickedBrokers) {
					assignment.LeaderBroker = pickedBrokers[i]
					i++
					hasChanges = true
				}
			}

			hasEmptyFollowers := false
			j := 0
			for ; j < len(assignment.FollowerBrokers); j++ {
				if assignment.FollowerBrokers[j] == "" {
					hasChanges = true
					if i < len(pickedBrokers) {
						assignment.FollowerBrokers[j] = pickedBrokers[i]
						i++
					} else {
						hasEmptyFollowers = true
					}
				}
			}
			if hasEmptyFollowers {
				var followerBrokers []string
				for _, follower := range assignment.FollowerBrokers {
					if follower != "" {
						followerBrokers = append(followerBrokers, follower)
					}
				}
				assignment.FollowerBrokers = followerBrokers
			}

			if i < len(pickedBrokers) {
				assignment.FollowerBrokers = append(assignment.FollowerBrokers, pickedBrokers[i:]...)
				hasChanges = true
			}
		}

	}

	glog.V(0).Infof("EnsureAssignmentsToActiveBrokers: activeBrokers: %v, followerCount: %d, assignments: %v hasChanges: %v", activeBrokers.Count(), followerCount, assignments, hasChanges)
	return
}
