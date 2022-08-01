package segment

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/seaweedfs/seaweedfs/weed/pb/message_fbs"
)

type MessageBatchBuilder struct {
	b              *flatbuffers.Builder
	producerId     int32
	producerEpoch  int32
	segmentId      int32
	flags          int32
	messageOffsets []flatbuffers.UOffsetT
	segmentSeqBase int64
	segmentSeqLast int64
	tsMsBase       int64
	tsMsLast       int64
}

func NewMessageBatchBuilder(b *flatbuffers.Builder,
	producerId int32,
	producerEpoch int32,
	segmentId int32,
	flags int32) *MessageBatchBuilder {

	b.Reset()

	return &MessageBatchBuilder{
		b:             b,
		producerId:    producerId,
		producerEpoch: producerEpoch,
		segmentId:     segmentId,
		flags:         flags,
	}
}

func (builder *MessageBatchBuilder) AddMessage(segmentSeq int64, tsMs int64, properties map[string][]byte, key []byte, value []byte) {
	if builder.segmentSeqBase == 0 {
		builder.segmentSeqBase = segmentSeq
	}
	builder.segmentSeqLast = segmentSeq
	if builder.tsMsBase == 0 {
		builder.tsMsBase = tsMs
	}
	builder.tsMsLast = tsMs

	var names, values, pairs []flatbuffers.UOffsetT
	for k, v := range properties {
		names = append(names, builder.b.CreateString(k))
		values = append(values, builder.b.CreateByteVector(v))
	}
	for i, _ := range names {
		message_fbs.NameValueStart(builder.b)
		message_fbs.NameValueAddName(builder.b, names[i])
		message_fbs.NameValueAddValue(builder.b, values[i])
		pair := message_fbs.NameValueEnd(builder.b)
		pairs = append(pairs, pair)
	}

	message_fbs.MessageStartPropertiesVector(builder.b, len(properties))
	for i := len(pairs) - 1; i >= 0; i-- {
		builder.b.PrependUOffsetT(pairs[i])
	}
	propOffset := builder.b.EndVector(len(properties))

	keyOffset := builder.b.CreateByteVector(key)
	valueOffset := builder.b.CreateByteVector(value)

	message_fbs.MessageStart(builder.b)
	message_fbs.MessageAddSeqDelta(builder.b, int32(segmentSeq-builder.segmentSeqBase))
	message_fbs.MessageAddTsMsDelta(builder.b, int32(tsMs-builder.tsMsBase))

	message_fbs.MessageAddProperties(builder.b, propOffset)
	message_fbs.MessageAddKey(builder.b, keyOffset)
	message_fbs.MessageAddData(builder.b, valueOffset)
	messageOffset := message_fbs.MessageEnd(builder.b)

	builder.messageOffsets = append(builder.messageOffsets, messageOffset)

}

func (builder *MessageBatchBuilder) BuildMessageBatch() {
	message_fbs.MessageBatchStartMessagesVector(builder.b, len(builder.messageOffsets))
	for i := len(builder.messageOffsets) - 1; i >= 0; i-- {
		builder.b.PrependUOffsetT(builder.messageOffsets[i])
	}
	messagesOffset := builder.b.EndVector(len(builder.messageOffsets))

	message_fbs.MessageBatchStart(builder.b)
	message_fbs.MessageBatchAddProducerId(builder.b, builder.producerId)
	message_fbs.MessageBatchAddProducerEpoch(builder.b, builder.producerEpoch)
	message_fbs.MessageBatchAddSegmentId(builder.b, builder.segmentId)
	message_fbs.MessageBatchAddFlags(builder.b, builder.flags)
	message_fbs.MessageBatchAddSegmentSeqBase(builder.b, builder.segmentSeqBase)
	message_fbs.MessageBatchAddSegmentSeqMaxDelta(builder.b, int32(builder.segmentSeqLast-builder.segmentSeqBase))
	message_fbs.MessageBatchAddTsMsBase(builder.b, builder.tsMsBase)
	message_fbs.MessageBatchAddTsMsMaxDelta(builder.b, int32(builder.tsMsLast-builder.tsMsBase))

	message_fbs.MessageBatchAddMessages(builder.b, messagesOffset)

	messageBatch := message_fbs.MessageBatchEnd(builder.b)

	builder.b.Finish(messageBatch)
}

func (builder *MessageBatchBuilder) GetBytes() []byte {
	return builder.b.FinishedBytes()
}
