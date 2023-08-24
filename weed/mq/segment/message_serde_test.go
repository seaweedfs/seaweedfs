package segment

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/seaweedfs/seaweedfs/weed/pb/message_fbs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageSerde(t *testing.T) {
	b := flatbuffers.NewBuilder(1024)

	prop := make(map[string][]byte)
	prop["n1"] = []byte("v1")
	prop["n2"] = []byte("v2")

	bb := NewMessageBatchBuilder(b, 1, 2, 3, 4)

	bb.AddMessage(5, 6, prop, []byte("the primary key"), []byte("body is here"))
	bb.AddMessage(5, 7, prop, []byte("the primary 2"), []byte("body is 2"))

	bb.BuildMessageBatch()

	buf := bb.GetBytes()

	println("serialized size", len(buf))

	mb := message_fbs.GetRootAsMessageBatch(buf, 0)

	assert.Equal(t, int32(1), mb.ProducerId())
	assert.Equal(t, int32(2), mb.ProducerEpoch())
	assert.Equal(t, int32(3), mb.SegmentId())
	assert.Equal(t, int32(4), mb.Flags())
	assert.Equal(t, int64(5), mb.SegmentSeqBase())
	assert.Equal(t, int32(0), mb.SegmentSeqMaxDelta())
	assert.Equal(t, int64(6), mb.TsMsBase())
	assert.Equal(t, int32(1), mb.TsMsMaxDelta())

	assert.Equal(t, 2, mb.MessagesLength())

	m := &message_fbs.Message{}
	mb.Messages(m, 0)

	/*
		// the vector seems not consistent
		nv := &message_fbs.NameValue{}
		m.Properties(nv, 0)
		assert.Equal(t, "n1", string(nv.Name()))
		assert.Equal(t, "v1", string(nv.Value()))
		m.Properties(nv, 1)
		assert.Equal(t, "n2", string(nv.Name()))
		assert.Equal(t, "v2", string(nv.Value()))
	*/
	assert.Equal(t, []byte("the primary key"), m.Key())
	assert.Equal(t, []byte("body is here"), m.Data())

	assert.Equal(t, int32(0), m.SeqDelta())
	assert.Equal(t, int32(0), m.TsMsDelta())

}
