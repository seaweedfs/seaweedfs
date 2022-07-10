package mq

import "time"

type Namespace string

type Topic struct {
	namespace Namespace
	name      string
}

type Partition struct {
	rangeStart int
	rangeStop  int // exclusive
	ringSize   int
}

type Segment struct {
	topic        Topic
	id           int32
	partition    Partition
	lastModified time.Time
}
