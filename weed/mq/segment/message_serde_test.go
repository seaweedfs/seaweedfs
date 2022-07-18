package segment

import (
	"github.com/chrislusf/seaweedfs/weed/pb/message_fbs"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageSerde(t *testing.T) {
	b := flatbuffers.NewBuilder(1024)

	prop := make(map[string]string)
	prop["n1"] = "v1"
	prop["n2"] = "v2"

	CreateMessage(b, 1, 2, 3, 4, 5, 6, prop,
		[]byte("the primary key"), []byte("body is here"))

	buf := b.FinishedBytes()

	println("serialized size", len(buf))

	m := message_fbs.GetRootAsMessage(buf, 0)

	assert.Equal(t, int32(1), m.ProducerId())
	assert.Equal(t, int64(2), m.ProducerSeq())
	assert.Equal(t, int32(3), m.SegmentId())
	assert.Equal(t, int64(4), m.SegmentSeq())
	assert.Equal(t, int64(5), m.EventTsNs())
	assert.Equal(t, int64(6), m.RecvTsNs())

	assert.Equal(t, 2, m.PropertiesLength())
	nv := &message_fbs.NameValue{}
	m.Properties(nv, 0)
	assert.Equal(t, "n1", string(nv.Name()))
	assert.Equal(t, "v1", string(nv.Value()))
	m.Properties(nv, 1)
	assert.Equal(t, "n2", string(nv.Name()))
	assert.Equal(t, "v2", string(nv.Value()))
	assert.Equal(t, []byte("the primary key"), m.Key())
	assert.Equal(t, []byte("body is here"), m.Data())

	m.MutateSegmentSeq(123)
	assert.Equal(t, int64(123), m.SegmentSeq())

}
