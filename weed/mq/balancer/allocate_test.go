package balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"reflect"
	"testing"
)

func Test_allocateOneBroker(t *testing.T) {
	brokers := cmap.New[*BrokerStats]()
	brokers.SetIfAbsent("localhost:17777", &BrokerStats{
		TopicPartitionCount: 0,
		ConsumerCount:       0,
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
					Partition: &mq_pb.Partition{
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
			if gotAssignments := allocateTopicPartitions(tt.args.brokers, tt.args.partitionCount); !reflect.DeepEqual(gotAssignments, tt.wantAssignments) {
				t.Errorf("allocateTopicPartitions() = %v, want %v", gotAssignments, tt.wantAssignments)
			}
		})
	}
}
