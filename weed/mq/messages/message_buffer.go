package messages

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/seaweedfs/seaweedfs/weed/mq/segment"
)

type MessageBuffer struct {
	fbsBuffer    *flatbuffers.Builder
	sequenceBase int64
	counter      int64
	bb           *segment.MessageBatchBuilder
	isSealed     bool
}

func NewMessageBuffer() *MessageBuffer {
	t := &MessageBuffer{
		fbsBuffer: flatbuffers.NewBuilder(4 * 1024 * 1024),
	}
	t.bb = segment.NewMessageBatchBuilder(t.fbsBuffer)
	return t
}

func (mb *MessageBuffer) Reset(sequenceBase int64) {
	mb.sequenceBase = sequenceBase
	mb.counter = 0
	mb.bb.Reset()
}

func (mb *MessageBuffer) AddMessage(message *Message) {
	mb.bb.AddMessage(mb.sequenceBase, message.Ts.UnixMilli(), message.Properties, message.Key, message.Content)
	mb.sequenceBase++
	mb.counter++
}

func (mb *MessageBuffer) Len() int {
	return int(mb.counter)
}

func (mb *MessageBuffer) Seal(producerId int32,
	producerEpoch int32,
	segmentId int32,
	flags int32) {
	mb.isSealed = true
	mb.bb.BuildMessageBatch(producerId, producerEpoch, segmentId, flags)
}

func (mb *MessageBuffer) Bytes() []byte {
	if !mb.isSealed {
		return nil
	}
	return mb.bb.GetBytes()
}
