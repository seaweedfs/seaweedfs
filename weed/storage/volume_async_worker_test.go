package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/stretchr/testify/require"
)

func newWorkerTestVolume(t *testing.T) *Volume {
	t.Helper()
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	return v
}

// A server holding millions of volumes pays for whatever every mount costs, so
// the batch worker and its channel may not exist until a durable write needs
// them.
func TestMountingAVolumeStartsNoBatchWorker(t *testing.T) {
	v := newWorkerTestVolume(t)
	require.Nil(t, v.asyncRequestsChan, "mounting a volume started a batch worker nothing had asked for")

	_, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, false, false)
	require.NoError(t, err)
	require.Nil(t, v.asyncRequestsChan, "a write that did not ask for fsync started a batch worker")
}

func TestDurableWriteStartsTheBatchWorkerOnce(t *testing.T) {
	v := newWorkerTestVolume(t)

	_, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, true, false)
	require.NoError(t, err)
	started := v.asyncRequestsChan
	require.NotNil(t, started, "a durable write did not start the batch worker")

	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, true, false)
	require.NoError(t, err)
	require.Equal(t, started, v.asyncRequestsChan, "a second durable write replaced the worker's channel")
}

// Destroy closes the channel. A write arriving after that has to fall back to
// the inline path rather than queue onto a worker that has gone.
func TestDurableWriteAfterDestroyWritesInline(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, true, false)
	require.NoError(t, err)

	require.NoError(t, v.Destroy(false, false))
	require.Nil(t, v.asyncRequestsChan)
	require.False(t, v.asyncRequestAppend(needle.NewAsyncRequest(newRandomNeedle(2), true)))
}
