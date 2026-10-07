package weed_server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeVacuumCompactStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeVacuumCompactStream) Context() context.Context { return s.ctx }

func (s *fakeVacuumCompactStream) Send(*volume_server_pb.VacuumVolumeCompactResponse) error {
	return nil
}

func newVacuumCommitTestServer(t *testing.T, maxParallelCommits int, vids ...needle.VolumeId) *VolumeServer {
	t.Helper()
	store := newTraversalTestStore(t.TempDir())
	t.Cleanup(store.Close)
	for _, vid := range vids {
		require.NoError(t, store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))
		for i := 1; i <= 3; i++ {
			n := new(needle.Needle)
			n.Id = types.Uint64ToNeedleId(uint64(i))
			n.Cookie = 0x1234
			n.Data = []byte(fmt.Sprintf("volume-%d-needle-%d", vid, i))
			n.Checksum = needle.NewCRC(n.Data)
			_, err := store.WriteVolumeNeedle(vid, n, true, false)
			require.NoError(t, err)
		}
	}
	vs := &VolumeServer{store: store}
	if maxParallelCommits > 0 {
		vs.vacuumCommitSlots = make(chan struct{}, maxParallelCommits)
	}
	return vs
}

func compactForTest(vs *VolumeServer, vid needle.VolumeId) error {
	stream := &fakeVacuumCompactStream{ctx: context.Background()}
	return vs.VacuumVolumeCompact(&volume_server_pb.VacuumVolumeCompactRequest{VolumeId: uint32(vid)}, stream)
}

func maxConcurrentHolders(vs *VolumeServer, workers int) int32 {
	var current, peak int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := vs.acquireVacuumCommitSlot(context.Background())
			if err != nil {
				return
			}
			n := atomic.AddInt32(&current, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&current, -1)
			release()
		}()
	}
	wg.Wait()
	return atomic.LoadInt32(&peak)
}

func TestVacuumCommitSlotsBoundConcurrentCommits(t *testing.T) {
	assert.Equal(t, int32(1), maxConcurrentHolders(&VolumeServer{vacuumCommitSlots: make(chan struct{}, 1)}, 8))
	assert.LessOrEqual(t, maxConcurrentHolders(&VolumeServer{vacuumCommitSlots: make(chan struct{}, 3)}, 8), int32(3))
}

func TestVacuumCommitSlotsUnlimitedByDefault(t *testing.T) {
	vs := &VolumeServer{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 8; i++ {
		_, err := vs.acquireVacuumCommitSlot(ctx)
		require.NoError(t, err, "acquisition %d must not wait when no limit is configured", i)
	}
}

func TestVacuumCommitSlotNotGrantedToExpiredContext(t *testing.T) {
	vs := &VolumeServer{vacuumCommitSlots: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 100; i++ {
		release, err := vs.acquireVacuumCommitSlot(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, release)
		require.Empty(t, vs.vacuumCommitSlots, "a refused acquisition must not keep the slot")
	}
}

func TestVacuumCommitLimitLeavesCompactionAndOtherServersAlone(t *testing.T) {
	vids := []needle.VolumeId{1, 2, 3, 4}
	vs := newVacuumCommitTestServer(t, 1, vids...)
	other := newVacuumCommitTestServer(t, 1, 9)
	vs.vacuumCommitSlots <- struct{}{}

	errs := make(chan error, len(vids))
	for _, vid := range vids {
		go func(vid needle.VolumeId) {
			errs <- compactForTest(vs, vid)
		}(vid)
	}
	for range vids {
		require.NoError(t, <-errs)
	}

	require.NoError(t, compactForTest(other, 9))
	_, err := other.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{VolumeId: 9})
	require.NoError(t, err, "a busy commit slot on one server must not block another server")
}

func TestQueuedVacuumCommitHoldsNoVolumeLockAndHonoursDeadline(t *testing.T) {
	vid := needle.VolumeId(1)
	vs := newVacuumCommitTestServer(t, 1, vid)
	require.NoError(t, compactForTest(vs, vid))
	vs.vacuumCommitSlots <- struct{}{}

	committed := make(chan error, 1)
	go func() {
		_, err := vs.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{VolumeId: uint32(vid)})
		committed <- err
	}()

	readDone := make(chan error, 1)
	go func() {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(1), Cookie: 0x1234}
		_, err := vs.store.ReadVolumeNeedle(vid, n, nil, nil)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("read blocked behind a queued commit")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := vs.VacuumVolumeCommit(ctx, &volume_server_pb.VacuumVolumeCommitRequest{VolumeId: uint32(vid)})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))

	select {
	case err := <-committed:
		t.Fatalf("commit ran while the only slot was taken: %v", err)
	default:
	}

	<-vs.vacuumCommitSlots
	select {
	case err := <-committed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("queued commit did not run after the slot freed")
	}
	assert.Empty(t, vs.vacuumCommitSlots, "the commit must return its slot")
}

func TestVacuumCommitReleasesSlotOnError(t *testing.T) {
	vs := newVacuumCommitTestServer(t, 1)
	_, err := vs.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{VolumeId: 42})
	require.Error(t, err)
	assert.Empty(t, vs.vacuumCommitSlots, "a failed commit must return its slot")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := vs.acquireVacuumCommitSlot(ctx)
	require.NoError(t, err)
	release()
}

func TestQueuedVacuumCommitDoesNotStartInMaintenanceMode(t *testing.T) {
	vid := needle.VolumeId(1)
	vs := newVacuumCommitTestServer(t, 1, vid)
	require.NoError(t, compactForTest(vs, vid))
	revisionBefore := vs.store.GetVolume(vid).SuperBlock.CompactionRevision
	vs.vacuumCommitSlots <- struct{}{}

	committed := make(chan error, 1)
	go func() {
		_, err := vs.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{VolumeId: uint32(vid)})
		committed <- err
	}()
	require.Eventually(t, func() bool { return vs.vacuumCommitsWaiting.Load() == 1 }, 5*time.Second, time.Millisecond,
		"the commit must pass the entry maintenance check and wait for a slot")

	require.NoError(t, vs.store.State.Update(&volume_server_pb.VolumeServerState{Maintenance: true}))
	<-vs.vacuumCommitSlots

	select {
	case err := <-committed:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "maintenance mode")
	case <-time.After(5 * time.Second):
		t.Fatal("queued commit did not return after the slot freed")
	}
	assert.Equal(t, revisionBefore, vs.store.GetVolume(vid).SuperBlock.CompactionRevision, "the volume must not be committed")
	assert.Empty(t, vs.vacuumCommitSlots, "the refused commit must return its slot")
}
