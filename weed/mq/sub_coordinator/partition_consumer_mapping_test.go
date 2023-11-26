package sub_coordinator

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"reflect"
	"testing"
)

func Test_doBalanceSticky(t *testing.T) {
	type args struct {
		partitions          []*topic.Partition
		consumerInstanceIds []string
		prevMapping         *PartitionSlotList
	}
	tests := []struct {
		name               string
		args               args
		wantPartitionSlots []*PartitionSlot
	}{
		{
			name: "1 consumer instance, 1 partition",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1"},
				prevMapping:         nil,
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-1",
				},
			},
		},
		{
			name: "2 consumer instances, 1 partition",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1", "consumer-instance-2"},
				prevMapping:         nil,
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-1",
				},
			},
		},
		{
			name: "1 consumer instance, 2 partitions",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1"},
				prevMapping:         nil,
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          50,
					AssignedInstanceId: "consumer-instance-1",
				},
				{
					RangeStart:         50,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-1",
				},
			},
		},
		{
			name: "2 consumer instances, 2 partitions",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1", "consumer-instance-2"},
				prevMapping:         nil,
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          50,
					AssignedInstanceId: "consumer-instance-1",
				},
				{
					RangeStart:         50,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-2",
				},
			},
		},
		{
			name: "2 consumer instances, 2 partitions, 1 deleted consumer instance",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1", "consumer-instance-2"},
				prevMapping: &PartitionSlotList{
					PartitionSlots: []*PartitionSlot{
						{
							RangeStart:         0,
							RangeStop:          50,
							AssignedInstanceId: "consumer-instance-3",
						},
						{
							RangeStart:         50,
							RangeStop:          100,
							AssignedInstanceId: "consumer-instance-2",
						},
					},
				},
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          50,
					AssignedInstanceId: "consumer-instance-1",
				},
				{
					RangeStart:         50,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-2",
				},
			},
		},
		{
			name: "2 consumer instances, 2 partitions, 1 new consumer instance",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1", "consumer-instance-2", "consumer-instance-3"},
				prevMapping: &PartitionSlotList{
					PartitionSlots: []*PartitionSlot{
						{
							RangeStart:         0,
							RangeStop:          50,
							AssignedInstanceId: "consumer-instance-3",
						},
						{
							RangeStart:         50,
							RangeStop:          100,
							AssignedInstanceId: "consumer-instance-2",
						},
					},
				},
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          50,
					AssignedInstanceId: "consumer-instance-3",
				},
				{
					RangeStart:         50,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-2",
				},
			},
		},
		{
			name: "2 consumer instances, 2 partitions, 1 new partition",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
					{
						RangeStart: 100,
						RangeStop:  150,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1", "consumer-instance-2"},
				prevMapping: &PartitionSlotList{
					PartitionSlots: []*PartitionSlot{
						{
							RangeStart:         0,
							RangeStop:          50,
							AssignedInstanceId: "consumer-instance-1",
						},
						{
							RangeStart:         50,
							RangeStop:          100,
							AssignedInstanceId: "consumer-instance-2",
						},
					},
				},
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          50,
					AssignedInstanceId: "consumer-instance-1",
				},
				{
					RangeStart:         50,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-2",
				},
				{
					RangeStart:         100,
					RangeStop:          150,
					AssignedInstanceId: "consumer-instance-1",
				},
			},
		},
		{
			name: "2 consumer instances, 2 partitions, 1 new partition, 1 new consumer instance",
			args: args{
				partitions: []*topic.Partition{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
					{
						RangeStart: 100,
						RangeStop:  150,
					},
				},
				consumerInstanceIds: []string{"consumer-instance-1", "consumer-instance-2", "consumer-instance-3"},
				prevMapping: &PartitionSlotList{
					PartitionSlots: []*PartitionSlot{
						{
							RangeStart:         0,
							RangeStop:          50,
							AssignedInstanceId: "consumer-instance-1",
						},
						{
							RangeStart:         50,
							RangeStop:          100,
							AssignedInstanceId: "consumer-instance-2",
						},
					},
				},
			},
			wantPartitionSlots: []*PartitionSlot{
				{
					RangeStart:         0,
					RangeStop:          50,
					AssignedInstanceId: "consumer-instance-1",
				},
				{
					RangeStart:         50,
					RangeStop:          100,
					AssignedInstanceId: "consumer-instance-2",
				},
				{
					RangeStart:         100,
					RangeStop:          150,
					AssignedInstanceId: "consumer-instance-3",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotPartitionSlots := doBalanceSticky(tt.args.partitions, tt.args.consumerInstanceIds, tt.args.prevMapping); !reflect.DeepEqual(gotPartitionSlots, tt.wantPartitionSlots) {
				t.Errorf("doBalanceSticky() = %v, want %v", gotPartitionSlots, tt.wantPartitionSlots)
			}
		})
	}
}
