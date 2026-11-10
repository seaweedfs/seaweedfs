package erasure_coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// FindAllEcVolumes returns runtimes whose last shard can be unmounted before
// the merge reads them.
func TestMergeEcRuntimesWithEmptiedRuntime(t *testing.T) {
	newRuntime := func(shardIds ...ShardId) *EcVolume {
		ev := &EcVolume{
			VolumeId:   needle.VolumeId(7),
			ECContext:  NewDefaultECContext("", 7),
			EncodeTsNs: 100,
		}
		for _, id := range shardIds {
			ev.Shards = append(ev.Shards, &EcVolumeShard{VolumeId: 7, ShardId: id})
		}
		return ev
	}
	emptied := newRuntime()
	holder := newRuntime(2, 5)

	merged := MergeEcRuntimes([]*EcVolume{emptied, holder})
	require.NotNil(t, merged)
	assert.Same(t, holder, merged.Anchor)
	assert.Len(t, merged.Merged, 2)
	assert.Same(t, holder.Shards[0], merged.Slots[2])
	assert.Same(t, holder.Shards[1], merged.Slots[5])
}
