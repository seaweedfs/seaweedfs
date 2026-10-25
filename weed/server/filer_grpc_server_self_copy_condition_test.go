package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Exercise the native condition and UpdateEntry commit, independently of the
// S3 handler's fake filer: same-content changes must invalidate a whole snapshot.
func TestSelfCopyEntryCondition(t *testing.T) {
	tests := []struct {
		name   string
		change func(*filer_pb.Entry)
	}{
		{name: "unchanged control"},
		{name: "mode", change: func(e *filer_pb.Entry) { e.Attributes.FileMode = 0644 }},
		{name: "mime", change: func(e *filer_pb.Entry) { e.Attributes.Mime = "image/png" }},
		{name: "legal hold", change: func(e *filer_pb.Entry) { e.Extended[s3_constants.ExtLegalHoldKey] = []byte("ON") }},
		{name: "content", change: func(e *filer_pb.Entry) {
			e.Content = []byte("next")
			e.Extended[s3_constants.ExtETagKey] = []byte("next-etag")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store := newTxnTestServer(t, map[string]*filer.Entry{
				"/buckets/b/obj": {
					Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000000, 0), Crtime: time.Unix(1700000000, 0), Ctime: time.Unix(1700000000, 0), Atime: time.Unix(1700000000, 0), Mode: 0660, Mime: "text/plain", FileSize: 4},
					Content:  []byte("data"),
					Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("same-etag"), s3_constants.ExtAmzAclKey: []byte("original grants")},
				},
			})
			expected := store.entries["/buckets/b/obj"].ToProtoEntry()
			updated := proto.Clone(expected).(*filer_pb.Entry)
			updated.Extended[s3_constants.ExtAmzAclKey] = []byte("private grants")
			req := &filer_pb.UpdateEntryRequest{
				Directory: "/buckets/b", Entry: updated,
				Condition: one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: expected}),
			}
			if tt.change != nil {
				concurrent := proto.Clone(expected).(*filer_pb.Entry)
				tt.change(concurrent)
				if _, err := fs.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{Directory: "/buckets/b", Entry: concurrent}); err != nil {
					t.Fatal(err)
				}
			}
			_, err := fs.UpdateEntry(context.Background(), req)
			if tt.change != nil {
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("stale self-copy: want FailedPrecondition, got %v", err)
				}
				current := store.entries["/buckets/b/obj"].ToProtoEntry()
				if string(current.Extended[s3_constants.ExtAmzAclKey]) != "original grants" {
					t.Fatal("failed condition changed grants")
				}
				// A fresh retry preserves the intervening content and unmanaged keys
				// while setting the desired mode and grants in the same update.
				req.Condition.Clauses[0].ExpectedEntry = current
				req.Entry = proto.Clone(current).(*filer_pb.Entry)
				req.Entry.Attributes.FileMode = 0660
				req.Entry.Extended[s3_constants.ExtAmzAclKey] = []byte("private grants")
				_, err = fs.UpdateEntry(context.Background(), req)
			}
			if err != nil {
				t.Fatalf("fresh self-copy: %v", err)
			}
			stored := store.entries["/buckets/b/obj"].ToProtoEntry()
			if stored.Attributes.FileMode != 0660 || string(stored.Extended[s3_constants.ExtAmzAclKey]) != "private grants" {
				t.Fatal("mode and grants did not commit together")
			}
			if tt.name == "legal hold" && string(stored.Extended[s3_constants.ExtLegalHoldKey]) != "ON" {
				t.Fatal("retry lost legal hold")
			}
			if tt.name == "content" && string(stored.Content) != "next" {
				t.Fatal("retry resurrected stale content")
			}
		})
	}
}
