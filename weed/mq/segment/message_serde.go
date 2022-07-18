package segment

import (
	"github.com/chrislusf/seaweedfs/weed/pb/message_fbs"
	flatbuffers "github.com/google/flatbuffers/go"
)

func CreateMessage(b *flatbuffers.Builder, producerId int32, producerSeq int64, segmentId int32, segmentSeq int64,
	eventTsNs int64, recvTsNs int64, properties map[string]string, key []byte, value []byte) {
	b.Reset()

	var names, values, pairs []flatbuffers.UOffsetT
	for k, v := range properties {
		names = append(names, b.CreateString(k))
		values = append(values, b.CreateString(v))
	}

	for i, _ := range names {
		message_fbs.NameValueStart(b)
		message_fbs.NameValueAddName(b, names[i])
		message_fbs.NameValueAddValue(b, values[i])
		pair := message_fbs.NameValueEnd(b)
		pairs = append(pairs, pair)
	}
	message_fbs.MessageStartPropertiesVector(b, len(properties))
	for i := len(pairs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(pairs[i])
	}
	prop := b.EndVector(len(properties))

	k := b.CreateByteVector(key)
	v := b.CreateByteVector(value)

	message_fbs.MessageStart(b)
	message_fbs.MessageAddProducerId(b, producerId)
	message_fbs.MessageAddProducerSeq(b, producerSeq)
	message_fbs.MessageAddSegmentId(b, segmentId)
	message_fbs.MessageAddSegmentSeq(b, segmentSeq)
	message_fbs.MessageAddEventTsNs(b, eventTsNs)
	message_fbs.MessageAddRecvTsNs(b, recvTsNs)

	message_fbs.MessageAddProperties(b, prop)
	message_fbs.MessageAddKey(b, k)
	message_fbs.MessageAddData(b, v)
	message := message_fbs.MessageEnd(b)

	b.Finish(message)
}
