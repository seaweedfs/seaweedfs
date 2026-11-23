package weed_server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// The expired target is logically absent but still owns data. Both rename
// RPCs must capture it before overwriting its metadata, and publish its
// deletion and release its local/remote data only after a successful commit.
func TestRenameOverExpiredS3TargetDefersCleanupUntilCommit(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, fail := range []bool{false, true} {
				name := map[bool]string{false: "local", true: "remote"}[remote] + "/" +
					map[bool]string{false: "atomic", true: "stream"}[stream] + "/" +
					map[bool]string{false: "commit", true: "rollback"}[fail]
				t.Run(name, func(t *testing.T) {
					client := &renameRemoteClient{}
					server, base := newRenameRemoteTestServer(t, client)
					store := &txnRenameStore{renameTestStore: base}
					server.filer.Store = filer.NewFilerStoreWrapper(store)
					queue := &captureQueue{}
					swapNotificationQueue(t, queue)
					const source, target = "/buckets/b/src/f", "/buckets/b/dst/f"
					store.entries[source] = fileEntryWithChunk(source, 101, updatedChunk)
					expired := fileEntryWithChunk(target, 202, storedChunk)
					expired.TtlSec = 1
					expired.Mtime = time.Now().Add(-time.Hour)
					expired.Extended = map[string][]byte{s3_constants.SeaweedFSExpiresS3: []byte("true")}
					if remote {
						expired.Remote = remoteOnlyRenameEntry(target, 202).Remote
					}
					store.entries[target] = expired
					commitErr := errors.New("commit refused")
					store.refuse = func(int) error {
						if got := len(queue.snapshot()); got != 0 {
							t.Fatalf("events before commit = %d, want none", got)
						}
						if got := client.recorded(); len(got) != 0 {
							t.Fatalf("remote effects before commit = %v, want none", got)
						}
						checkDeletions(t, server.filer)
						if fail {
							return commitErr
						}
						return nil
					}

					var err error
					if stream {
						err = server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
							OldDirectory: "/buckets/b/src", OldName: "f", NewDirectory: "/buckets/b/dst", NewName: "f",
						}, &inPlaceRenameStream{})
					} else {
						err = renameFile(server, source, target)
					}
					if fail {
						if err == nil {
							t.Fatal("rename succeeded despite refused commit")
						}
						checkChunk(t, store, source, updatedChunk)
						checkChunk(t, store, target, storedChunk)
						checkDeletions(t, server.filer)
						if len(queue.snapshot()) != 0 || len(client.recorded()) != 0 {
							t.Fatal("rolled-back rename published a delete or removed a remote object")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					checkChunk(t, store, target, updatedChunk)
					if _, err := store.renameTestStore.FindEntry(context.Background(), source); !errors.Is(err, filer_pb.ErrNotFound) {
						t.Fatalf("source after rename: %v, want NotFound", err)
					}
					checkDeletions(t, server.filer, storedChunk)
					events := queue.snapshot()
					if len(events) != 2 || events[0].key != target || events[0].notification.NewEntry != nil ||
						!events[0].notification.DeleteChunks || events[1].key != source {
						t.Fatalf("events = %+v, want target delete then rename", events)
					}
					calls := client.recorded()
					if remote {
						if len(calls) != 1 || calls[0] != "delete origin/dst/f" {
							t.Fatalf("remote effects = %v, want expired target deleted once", calls)
						}
					} else if len(calls) != 0 {
						t.Fatalf("remote effects = %v, want none", calls)
					}
				})
			}
		}
	}
}
