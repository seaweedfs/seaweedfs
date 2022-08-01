package sequence

import (
	"encoding/hex"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSequencer(t *testing.T) {
	seq, err := NewSnowflakeSequencer("for_test", 1)
	assert.Equal(t, nil, err)
	last := uint64(0)
	bytes := make([]byte, types.NeedleIdSize)
	for i := 0; i < 100; i++ {
		next := seq.NextFileId(1)
		types.NeedleIdToBytes(bytes, types.NeedleId(next))
		println(hex.EncodeToString(bytes))
		if last == next {
			t.Errorf("last %d next %d", last, next)
		}
		last = next
	}

}
