package weed_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeStatusErrorDistinguishesAbsentFromTransportFailure(t *testing.T) {
	notFound := volumeStatusError(fmt.Errorf("delete volume 17 not found on disk: %w", storage.ErrVolumeNotFound))
	assert.Equal(t, codes.NotFound, status.Code(notFound))
	assert.Contains(t, notFound.Error(), "not found", "the store message must survive for callers that match on it")

	transport := errors.New("connection reset")
	require.ErrorIs(t, volumeStatusError(transport), transport)
	assert.NotEqual(t, codes.NotFound, status.Code(transport))

	notEmpty := volumeStatusError(storage.ErrVolumeNotEmpty)
	assert.Equal(t, codes.FailedPrecondition, status.Code(notEmpty))
	assert.Contains(t, notEmpty.Error(), "volume not empty")

	noSpace := volumeStatusError(fmt.Errorf("compact volume 17: %w", storage.ErrInsufficientSpace))
	assert.Equal(t, codes.ResourceExhausted, status.Code(noSpace))
	assert.Contains(t, noSpace.Error(), "insufficient free space")
}

func TestVolumeDeleteMapsAbsentStoreVolumeToNotFound(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}

	_, err := vs.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{VolumeId: 17})
	assert.Equal(t, codes.NotFound, status.Code(err), err)
}

func TestVolumeDeleteSkipsCancelledRPC(t *testing.T) {
	vid := needle.VolumeId(1)
	vs := newVacuumCommitTestServer(t, 0, vid)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := vs.VolumeDelete(cancelled, &volume_server_pb.VolumeDeleteRequest{VolumeId: uint32(vid)})
	assert.Equal(t, codes.Canceled, status.Code(err), err)
	require.NotNil(t, vs.store.GetVolume(vid), "a cancelled delete must leave the volume")

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	_, err = vs.VolumeDelete(expired, &volume_server_pb.VolumeDeleteRequest{VolumeId: uint32(vid)})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), err)
	require.NotNil(t, vs.store.GetVolume(vid), "an expired delete must leave the volume")

	_, err = vs.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{VolumeId: uint32(vid)})
	require.NoError(t, err)
	assert.Nil(t, vs.store.GetVolume(vid))
}

func TestVolumeDeleteCancelledWhileQueuedBehindGuard(t *testing.T) {
	vid := needle.VolumeId(1)
	vs := newVacuumCommitTestServer(t, 0, vid)

	guard := vs.volumeCopyGuard(uint32(vid))
	guard.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := vs.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{VolumeId: uint32(vid)})
		done <- err
	}()
	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		for _, g := range bytes.Split(buf[:runtime.Stack(buf, true)], []byte("\n\n")) {
			if bytes.Contains(g, []byte("(*VolumeServer).VolumeDelete")) && bytes.Contains(g, []byte("sync.(*Mutex).Lock")) {
				return true
			}
		}
		return false
	}, 10*time.Second, time.Millisecond)
	cancel()
	guard.Unlock()

	select {
	case err := <-done:
		assert.Equal(t, codes.Canceled, status.Code(err), err)
	case <-time.After(10 * time.Second):
		t.Fatal("VolumeDelete did not return")
	}
	assert.NotNil(t, vs.store.GetVolume(vid), "a delete cancelled while queued must leave the volume")
}
