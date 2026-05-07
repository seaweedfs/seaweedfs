package reader

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func TestExtractBucketKeyCreate(t *testing.T) {
	r := &Reader{BucketsPath: "/buckets"}
	resp := &filer_pb.SubscribeMetadataResponse{
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/mybucket/path/to",
			NewEntry:      &filer_pb.Entry{Name: "obj.txt"},
		},
	}
	b, k, ok := r.extractBucketKey(resp)
	if !ok || b != "mybucket" || k != "path/to/obj.txt" {
		t.Fatalf("got (%q,%q,%v), want (mybucket,path/to/obj.txt,true)", b, k, ok)
	}
}

func TestExtractBucketKeyTopLevelObject(t *testing.T) {
	r := &Reader{BucketsPath: "/buckets"}
	resp := &filer_pb.SubscribeMetadataResponse{
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/mybucket",
			NewEntry:      &filer_pb.Entry{Name: "a.txt"},
		},
	}
	b, k, ok := r.extractBucketKey(resp)
	if !ok || b != "mybucket" || k != "a.txt" {
		t.Fatalf("got (%q,%q,%v), want (mybucket,a.txt,true)", b, k, ok)
	}
}

func TestExtractBucketKeyDeleteUsesOldEntry(t *testing.T) {
	r := &Reader{BucketsPath: "/buckets"}
	resp := &filer_pb.SubscribeMetadataResponse{
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/b/dir",
			OldEntry:      &filer_pb.Entry{Name: "gone.txt"},
		},
	}
	b, k, ok := r.extractBucketKey(resp)
	if !ok || b != "b" || k != "dir/gone.txt" {
		t.Fatalf("got (%q,%q,%v), want (b,dir/gone.txt,true)", b, k, ok)
	}
}

func TestExtractBucketKeyDeleteWithEmptyParent(t *testing.T) {
	// Some delete events carry the directory in resp.Directory rather than
	// EventNotification.NewParentPath.
	r := &Reader{BucketsPath: "/buckets"}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "gone.txt"},
		},
	}
	b, k, ok := r.extractBucketKey(resp)
	if !ok || b != "b" || k != "dir/gone.txt" {
		t.Fatalf("got (%q,%q,%v), want (b,dir/gone.txt,true)", b, k, ok)
	}
}

func TestExtractBucketKeyOutsideBucketsSkipped(t *testing.T) {
	r := &Reader{BucketsPath: "/buckets"}
	resp := &filer_pb.SubscribeMetadataResponse{
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/etc/something",
			NewEntry:      &filer_pb.Entry{Name: "config"},
		},
	}
	if _, _, ok := r.extractBucketKey(resp); ok {
		t.Fatal("expected skip for non-bucket path")
	}
}

func TestExtractBucketKeyBucketCreateAtRoot(t *testing.T) {
	// Creating a new bucket emits an event at /buckets/ with the bucket as
	// the new entry's Name.
	r := &Reader{BucketsPath: "/buckets"}
	resp := &filer_pb.SubscribeMetadataResponse{
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/",
			NewEntry:      &filer_pb.Entry{Name: "newbucket"},
		},
	}
	b, k, ok := r.extractBucketKey(resp)
	if !ok || b != "newbucket" || k != "" {
		t.Fatalf("got (%q,%q,%v), want (newbucket,,true)", b, k, ok)
	}
}

func TestDispatchOneFiltersByShard(t *testing.T) {
	// Pick a bucket+key whose shard is known, set Reader to a different
	// shard, expect skip; same shard, expect emit.
	bucket, key := "mybucket", "thing.txt"
	target := s3lifecycle.ShardID(bucket, key)
	other := (target + 1) % s3lifecycle.ShardCount

	resp := &filer_pb.SubscribeMetadataResponse{
		TsNs: 12345,
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/" + bucket,
			NewEntry:      &filer_pb.Entry{Name: key},
		},
	}

	out := make(chan *Event, 1)
	r := &Reader{
		ShardID:     other,
		BucketsPath: "/buckets",
		Cursor:      NewCursor(),
		Events:      out,
	}
	processed := 0
	if err := r.dispatchOne(context.Background(), resp, &processed); err != nil {
		t.Fatalf("dispatchOne err: %v", err)
	}
	if processed != 0 || len(out) != 0 {
		t.Fatalf("wrong-shard event should be skipped: processed=%d emitted=%d", processed, len(out))
	}

	r.ShardID = target
	if err := r.dispatchOne(context.Background(), resp, &processed); err != nil {
		t.Fatalf("dispatchOne err: %v", err)
	}
	if processed != 1 || len(out) != 1 {
		t.Fatalf("matching-shard event should be emitted: processed=%d emitted=%d", processed, len(out))
	}
	ev := <-out
	if ev.Bucket != bucket || ev.Key != key || ev.TsNs != 12345 {
		t.Fatalf("emitted event mismatch: %+v", ev)
	}
}

func TestDispatchOneRespectsCtxCancel(t *testing.T) {
	// Buffered channel of size 0 (unbuffered) blocks the send; ctx cancel
	// should unblock and return ctx.Err().
	bucket, key := "mybucket", "thing.txt"
	target := s3lifecycle.ShardID(bucket, key)
	resp := &filer_pb.SubscribeMetadataResponse{
		TsNs: 1,
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/" + bucket,
			NewEntry:      &filer_pb.Entry{Name: key},
		},
	}
	out := make(chan *Event)
	r := &Reader{
		ShardID:     target,
		BucketsPath: "/buckets",
		Cursor:      NewCursor(),
		Events:      out,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	processed := 0
	err := r.dispatchOne(ctx, resp, &processed)
	if err != context.Canceled {
		t.Fatalf("expected ctx.Canceled, got %v", err)
	}
}
