package mq

import (
	"github.com/chrislusf/seaweedfs/weed/pb/mq_pb"
	"time"
)

type Namespace string

type Topic struct {
	Namespace Namespace
	Name      string
}

type Partition struct {
	RangeStart int
	RangeStop  int // exclusive
	RingSize   int
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
	}
}
