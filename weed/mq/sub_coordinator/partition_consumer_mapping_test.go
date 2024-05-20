package sub_coordinator

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"reflect"
	"testing"
)

func Test_doBalanceSticky(t *testing.T) {
	type args struct {
		partitions          []*pub_balancer.PartitionSlotToBroker
		consumerInstanceIds []*ConsumerGroupInstance
		prevMapping         *PartitionSlotToConsumerInstanceList
	}
	tests := []struct {
		name               string
		args               args
		wantPartitionSlots []*PartitionSlotToConsumerInstance
	}{
		{
			name: "1 consumer instance, 1 partition",
			args: args{
				partitions: []*pub_balancer.PartitionSlotToBroker{
					{
						RangeStart: 0,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: nil,
			},
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
					{
						RangeStart: 0,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-2",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: nil,
			},
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: nil,
			},
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-2",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: nil,
			},
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-2",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: &PartitionSlotToConsumerInstanceList{
					PartitionSlots: []*PartitionSlotToConsumerInstance{
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
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
					{
						RangeStart: 0,
						RangeStop:  50,
					},
					{
						RangeStart: 50,
						RangeStop:  100,
					},
				},
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-2",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-3",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: &PartitionSlotToConsumerInstanceList{
					PartitionSlots: []*PartitionSlotToConsumerInstance{
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
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
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
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-2",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: &PartitionSlotToConsumerInstanceList{
					PartitionSlots: []*PartitionSlotToConsumerInstance{
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
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
				partitions: []*pub_balancer.PartitionSlotToBroker{
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
				consumerInstanceIds: []*ConsumerGroupInstance{
					{
						InstanceId:        "consumer-instance-1",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-2",
						MaxPartitionCount: 1,
					},
					{
						InstanceId:        "consumer-instance-3",
						MaxPartitionCount: 1,
					},
				},
				prevMapping: &PartitionSlotToConsumerInstanceList{
					PartitionSlots: []*PartitionSlotToConsumerInstance{
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
			wantPartitionSlots: []*PartitionSlotToConsumerInstance{
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
