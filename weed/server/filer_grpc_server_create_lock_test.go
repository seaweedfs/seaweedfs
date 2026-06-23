package weed_server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Concurrent OExcl creates for the same path must yield exactly one winner. The
// filer's CreateEntry is a FindEntry-then-Insert; without the per-path lock both
// racers observe "not found" and both insert. The exclusive entry lock makes the
// check-then-act atomic so the losers see ErrEntryAlreadyExists.
func TestCreateEntryOExclSerialized(t *testing.T) {
	store := newRenameTestStore()
	store.findDelay = 5 * time.Millisecond
	f := newRenameTestFiler(store)
	f.DirBucketsPath = "/buckets"

	fs := &FilerServer{
		filer:          f,
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}

	const racers = 8
	var success, alreadyExists, unexpected int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := fs.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
				Directory:                "/test",
				OExcl:                    true,
				SkipCheckParentDirectory: true,
				Entry: &filer_pb.Entry{
					Name:       "obj",
					Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644, Inode: 1},
				},
			})
			switch {
			case err != nil:
				atomic.AddInt32(&unexpected, 1)
			case resp.Error == "":
				atomic.AddInt32(&success, 1)
			case resp.ErrorCode == filer_pb.FilerError_ENTRY_ALREADY_EXISTS:
				atomic.AddInt32(&alreadyExists, 1)
			default:
				atomic.AddInt32(&unexpected, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	// Exactly one winner; every loser fails with ENTRY_ALREADY_EXISTS and nothing
	// else, so an unrelated failure can't masquerade as a passing test.
	if success != 1 || alreadyExists != racers-1 || unexpected != 0 {
		t.Fatalf("winners=%d already_exists=%d unexpected=%d (racers=%d)", success, alreadyExists, unexpected, racers)
	}
}
