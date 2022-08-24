package mq

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"time"
)

type Namespace string

type Topic struct {
	Namespace Namespace
	Name      string
}

type Partition struct {
	RangeStart int32
	RangeStop  int32 // exclusive
	RingSize   int32
}

type Segment struct {
	Topic        Topic
	Id           int32
	Partition    Partition
	LastModified time.Time
}

func FromPbSegment(segment *mq_pb.Segment) *Segment {
	return &Segment{
		Topic: Topic{
			Namespace: Namespace(segment.Namespace),
			Name:      segment.Topic,
		},
		Id: segment.Id,
		Partition: Partition{
			RangeStart: segment.Partition.RangeStart,
			RangeStop:  segment.Partition.RangeStop,
			RingSize:   segment.Partition.RingSize,
		},
	}
}

func (segment *Segment) ToPbSegment() *mq_pb.Segment {
	return &mq_pb.Segment{
		Namespace: string(segment.Topic.Namespace),
		Topic:     segment.Topic.Name,
		Id:        segment.Id,
		Partition: &mq_pb.Partition{
			RingSize:   segment.Partition.RingSize,
			RangeStart: segment.Partition.RangeStart,
			RangeStop:  segment.Partition.RangeStop,
		},
	}
}

func (segment *Segment) DirAndName() (dir string, name string) {
	dir = fmt.Sprintf("%s/%s/%s", filer.TopicsDir, segment.Topic.Namespace, segment.Topic.Name)
	name = fmt.Sprintf("%4d.segment", segment.Id)
	return
}
