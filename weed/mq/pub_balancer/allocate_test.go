package pub_balancer

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_allocateOneBroker(t *testing.T) {
	brokers := cmap.New[*BrokerStats]()
	brokers.SetIfAbsent("localhost:17777", &BrokerStats{
		TopicPartitionCount: 0,
		CpuUsagePercent:     0,
	})

	tests := []struct {
		name            string
		args            args
		wantAssignments []*mq_pb.BrokerPartitionAssignment
	}{
		{
			name: "test only one broker",
			args: args{
				brokers:        brokers,
				partitionCount: 1,
			},
			wantAssignments: []*mq_pb.BrokerPartitionAssignment{
				{
					LeaderBroker: "localhost:17777",
					Partition: &schema_pb.Partition{
						RingSize:   MaxPartitionCount,
						RangeStart: 0,
						RangeStop:  MaxPartitionCount,
					},
				},
			},
		},
	}
	testThem(t, tests)
}

type args struct {
	brokers        cmap.ConcurrentMap[string, *BrokerStats]
	partitionCount int32
}

func testThem(t *testing.T, tests []struct {
	name            string
	args            args
	wantAssignments []*mq_pb.BrokerPartitionAssignment
}) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAssignments := AllocateTopicPartitions(tt.args.brokers, tt.args.partitionCount)
			assert.Equal(t, len(tt.wantAssignments), len(gotAssignments))
			for i, gotAssignment := range gotAssignments {
				assert.Equal(t, tt.wantAssignments[i].LeaderBroker, gotAssignment.LeaderBroker)
				assert.Equal(t, tt.wantAssignments[i].Partition.RangeStart, gotAssignment.Partition.RangeStart)
				assert.Equal(t, tt.wantAssignments[i].Partition.RangeStop, gotAssignment.Partition.RangeStop)
				assert.Equal(t, tt.wantAssignments[i].Partition.RingSize, gotAssignment.Partition.RingSize)
			}
		})
	}
}

func TestEnsureAssignmentsToActiveBrokersX(t *testing.T) {
	type args struct {
		activeBrokers cmap.ConcurrentMap[string, *BrokerStats]
		followerCount int
		assignments   []*mq_pb.BrokerPartitionAssignment
	}
	activeBrokers := cmap.New[*BrokerStats]()
	activeBrokers.SetIfAbsent("localhost:1", &BrokerStats{})
	activeBrokers.SetIfAbsent("localhost:2", &BrokerStats{})
	activeBrokers.SetIfAbsent("localhost:3", &BrokerStats{})
	activeBrokers.SetIfAbsent("localhost:4", &BrokerStats{})
	activeBrokers.SetIfAbsent("localhost:5", &BrokerStats{})
	activeBrokers.SetIfAbsent("localhost:6", &BrokerStats{})
	lowActiveBrokers := cmap.New[*BrokerStats]()
	lowActiveBrokers.SetIfAbsent("localhost:1", &BrokerStats{})
	lowActiveBrokers.SetIfAbsent("localhost:2", &BrokerStats{})
	singleActiveBroker := cmap.New[*BrokerStats]()
	singleActiveBroker.SetIfAbsent("localhost:1", &BrokerStats{})
	tests := []struct {
		name       string
		args       args
		hasChanges bool
	}{
		{
			name: "test empty leader",
			args: args{
				activeBrokers: activeBrokers,
				followerCount: 1,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker:   "",
						Partition:      &schema_pb.Partition{},
						FollowerBroker: "localhost:2",
					},
				},
			},
			hasChanges: true,
		},
		{
			name: "test empty follower",
			args: args{
				activeBrokers: activeBrokers,
				followerCount: 1,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker:   "localhost:1",
						Partition:      &schema_pb.Partition{},
						FollowerBroker: "",
					},
				},
			},
			hasChanges: true,
		},
		{
			name: "test dead follower",
			args: args{
				activeBrokers: activeBrokers,
				followerCount: 1,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker:   "localhost:1",
						Partition:      &schema_pb.Partition{},
						FollowerBroker: "localhost:200",
					},
				},
			},
			hasChanges: true,
		},
		{
			name: "test dead leader and follower",
			args: args{
				activeBrokers: activeBrokers,
				followerCount: 1,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker:   "localhost:100",
						Partition:      &schema_pb.Partition{},
						FollowerBroker: "localhost:200",
					},
				},
			},
			hasChanges: true,
		},
		{
			name: "test low active brokers",
			args: args{
				activeBrokers: lowActiveBrokers,
				followerCount: 3,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker:   "localhost:1",
						Partition:      &schema_pb.Partition{},
						FollowerBroker: "localhost:2",
					},
				},
			},
			hasChanges: false,
		},
		{
			name: "test low active brokers with one follower",
			args: args{
				activeBrokers: lowActiveBrokers,
				followerCount: 1,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker: "localhost:1",
						Partition:    &schema_pb.Partition{},
					},
				},
			},
			hasChanges: true,
		},
		{
			name: "test single active broker",
			args: args{
				activeBrokers: singleActiveBroker,
				followerCount: 3,
				assignments: []*mq_pb.BrokerPartitionAssignment{
					{
						LeaderBroker:   "localhost:1",
						Partition:      &schema_pb.Partition{},
						FollowerBroker: "localhost:2",
					},
				},
			},
			hasChanges: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("%v before %v\n", tt.name, tt.args.assignments)
			hasChanges := EnsureAssignmentsToActiveBrokers(tt.args.activeBrokers, tt.args.followerCount, tt.args.assignments)
			assert.Equalf(t, tt.hasChanges, hasChanges, "EnsureAssignmentsToActiveBrokers(%v, %v, %v)", tt.args.activeBrokers, tt.args.followerCount, tt.args.assignments)
			fmt.Printf("%v after %v\n", tt.name, tt.args.assignments)
		})
	}
}
