package pub_balancer

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"reflect"
	"testing"
)

func Test_findMissingPartitions(t *testing.T) {
	type args struct {
		partitions []topic.Partition
	}
	tests := []struct {
		name                  string
		args                  args
		wantMissingPartitions []topic.Partition
	}{
		{
			name: "one partition",
			args: args{
				partitions: []topic.Partition{
					{RingSize: 1024, RangeStart: 0, RangeStop: 1024},
				},
			},
			wantMissingPartitions: nil,
		},
		{
			name: "two partitions",
			args: args{
				partitions: []topic.Partition{
					{RingSize: 1024, RangeStart: 0, RangeStop: 512},
					{RingSize: 1024, RangeStart: 512, RangeStop: 1024},
				},
			},
			wantMissingPartitions: nil,
		},
		{
			name: "four partitions, missing last two",
			args: args{
				partitions: []topic.Partition{
					{RingSize: 1024, RangeStart: 0, RangeStop: 256},
					{RingSize: 1024, RangeStart: 256, RangeStop: 512},
				},
			},
			wantMissingPartitions: []topic.Partition{
				{RingSize: 1024, RangeStart: 512, RangeStop: 768},
				{RingSize: 1024, RangeStart: 768, RangeStop: 1024},
			},
		},
		{
			name: "four partitions, missing first two",
			args: args{
				partitions: []topic.Partition{
					{RingSize: 1024, RangeStart: 512, RangeStop: 768},
					{RingSize: 1024, RangeStart: 768, RangeStop: 1024},
				},
			},
			wantMissingPartitions: []topic.Partition{
				{RingSize: 1024, RangeStart: 0, RangeStop: 256},
				{RingSize: 1024, RangeStart: 256, RangeStop: 512},
			},
		},
		{
			name: "four partitions, missing middle two",
			args: args{
				partitions: []topic.Partition{
					{RingSize: 1024, RangeStart: 0, RangeStop: 256},
					{RingSize: 1024, RangeStart: 768, RangeStop: 1024},
				},
			},
			wantMissingPartitions: []topic.Partition{
				{RingSize: 1024, RangeStart: 256, RangeStop: 512},
				{RingSize: 1024, RangeStart: 512, RangeStop: 768},
			},
		},
		{
			name: "four partitions, missing three",
			args: args{
				partitions: []topic.Partition{
					{RingSize: 1024, RangeStart: 512, RangeStop: 768},
				},
			},
			wantMissingPartitions: []topic.Partition{
				{RingSize: 1024, RangeStart: 0, RangeStop: 256},
				{RingSize: 1024, RangeStart: 256, RangeStop: 512},
				{RingSize: 1024, RangeStart: 768, RangeStop: 1024},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotMissingPartitions := findMissingPartitions(tt.args.partitions, 1024); !reflect.DeepEqual(gotMissingPartitions, tt.wantMissingPartitions) {
				t.Errorf("findMissingPartitions() = %v, want %v", gotMissingPartitions, tt.wantMissingPartitions)
			}
		})
	}
}
