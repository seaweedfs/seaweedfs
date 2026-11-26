package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestScopedUpdateRechecksExpiryAfterPreparation(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain store", true: "transactional store"}[transactional], func(t *testing.T) {
			store := newRenameTestStore()
			fs, interceptor := newScopeRetentionServer(t, store)
			if transactional {
				fs.filer = newRenameTestFiler(t, conflictRefusingStore{&txnRenameStore{renameTestStore: store}})
			}
			queue := &captureQueue{}
			swapNotificationQueue(t, queue)
			path := util.NewFullPath(aliceWS, "expiry")
			stored := newFileEntry(string(path), 1)
			stored.Chunks = []*filer_pb.FileChunk{written(storedChunk, 1)}
			store.entries[string(path)] = stored
			before := proto.Clone(stored.ToProtoEntry()).(*filer_pb.Entry)
			replacement := proto.Clone(before).(*filer_pb.Entry)
			replacement.Attributes.Mtime++
			const manifestID = "4,0d04"
			preparations := 0
			err := scopedWormUnary(t, interceptor, "UpdateEntry", &filer_pb.UpdateEntryRequest{
				Directory: aliceWS, Entry: replacement,
			}, func(ctx context.Context, request any) (any, error) {
				return fs.updateEntry(ctx, request.(*filer_pb.UpdateEntryRequest), string(path),
					func(ctx context.Context, _ string, _ *filer.Entry, _ *filer_pb.Entry) ([]*filer_pb.FileChunk, []*filer_pb.FileChunk, error) {
						preparations++
						authority := ctx.Value(scopeWriteAuthorityKey{}).(*scopeWriteAuthority)
						authority.sc.exp = time.Now().Add(-authority.sc.leeway - time.Second)
						manifest := written(manifestID, 2)
						manifest.IsChunkManifest = true
						return []*filer_pb.FileChunk{manifest}, nil, nil
					})
			})
			if status.Code(err) != codes.Unauthenticated || preparations != 1 {
				t.Fatalf("update = %v, preparations = %d, want Unauthenticated after one preparation", err, preparations)
			}
			got, err := store.FindEntry(context.Background(), path)
			if err != nil || !proto.Equal(got.ToProtoEntry(), before) {
				t.Fatalf("expired update changed stored entry: got=%v err=%v", got, err)
			}
			if events := queue.snapshot(); len(events) != 0 {
				t.Fatalf("expired update emitted %d events", len(events))
			}
			checkDeletions(t, fs.filer, manifestID)
		})
	}
}
