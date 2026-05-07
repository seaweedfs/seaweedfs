package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// Event is one in-shard meta-log event delivered to the router.
type Event struct {
	TsNs      int64
	Bucket    string
	Key       string
	OldEntry  *filer_pb.Entry
	NewEntry  *filer_pb.Entry
	NewParent string
}

// IsDelete reports whether this event removes an entry.
func (e *Event) IsDelete() bool {
	return e.NewEntry == nil && e.OldEntry != nil
}

// IsCreate reports whether this event creates an entry.
func (e *Event) IsCreate() bool {
	return e.OldEntry == nil && e.NewEntry != nil
}

// Reader subscribes to the filer meta-log for one shard. It owns a Cursor
// (per-ActionKey position within the shard) and emits in-shard Events to a
// channel; the downstream router consumes events and ack-advances the cursor
// for matched ActionKeys when their actions complete.
type Reader struct {
	ShardID     int    // [0, s3lifecycle.ShardCount)
	BucketsPath string // e.g. "/buckets"
	Cursor      *Cursor
	Events      chan<- *Event

	// EventBudget caps how many events Run processes before returning nil.
	// Zero = unbounded; the run continues until ctx cancellation or stream
	// error. Used by the worker scheduler to bound a single READ task.
	EventBudget int

	// bucketsPathSlash is BucketsPath with a guaranteed trailing slash,
	// computed once on Run and reused per event to avoid recomputing the
	// normalized prefix in extractBucketKey.
	bucketsPathSlash string
}

// Run subscribes via SubscribeMetadata starting at Cursor.MinTsNs(), filters
// to this shard, and emits Events. Returns on ctx.Done(), io.EOF, or
// stream error. Caller is responsible for closing Events if it owns it.
func (r *Reader) Run(ctx context.Context, client filer_pb.SeaweedFilerClient, clientName string, clientID int32) error {
	if r.ShardID < 0 || r.ShardID >= s3lifecycle.ShardCount {
		return fmt.Errorf("reader: shard_id %d out of range", r.ShardID)
	}
	if r.Cursor == nil {
		return errors.New("reader: nil Cursor")
	}
	if r.Events == nil {
		return errors.New("reader: nil Events channel")
	}
	if r.BucketsPath == "" {
		return errors.New("reader: empty BucketsPath")
	}
	r.bucketsPathSlash = r.BucketsPath
	if !strings.HasSuffix(r.bucketsPathSlash, "/") {
		r.bucketsPathSlash += "/"
	}

	stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
		ClientName:             clientName,
		PathPrefix:             r.BucketsPath,
		SinceNs:                r.Cursor.MinTsNs(),
		ClientId:               clientID,
		ClientSupportsBatching: true,
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	processed := 0
	for {
		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}

		// First event in resp is the primary; resp.Events is the batched tail.
		if err := r.dispatchOne(ctx, resp, &processed); err != nil {
			return err
		}
		for _, ev := range resp.Events {
			if err := r.dispatchOne(ctx, ev, &processed); err != nil {
				return err
			}
		}
		if r.EventBudget > 0 && processed >= r.EventBudget {
			return nil
		}
	}
}

func (r *Reader) dispatchOne(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, processed *int) error {
	if resp == nil || resp.EventNotification == nil {
		return nil
	}
	bucket, key, ok := r.extractBucketKey(resp)
	if !ok {
		return nil
	}
	if s3lifecycle.ShardID(bucket, key) != r.ShardID {
		return nil
	}

	ev := &Event{
		TsNs:      resp.TsNs,
		Bucket:    bucket,
		Key:       key,
		OldEntry:  resp.EventNotification.OldEntry,
		NewEntry:  resp.EventNotification.NewEntry,
		NewParent: resp.EventNotification.NewParentPath,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.Events <- ev:
		*processed++
		return nil
	}
}

// extractBucketKey turns a meta-log event's path into (bucket, key) when the
// event lies under BucketsPath. Returns ok=false for events outside that
// subtree (cluster admin entries, system files, etc.) so the reader can skip
// them without engaging the routing index.
//
// The path is reconstructed as DirectoryPath/Name, where DirectoryPath comes
// from the entry context and Name from old_entry/new_entry. We prefer
// new_entry on creates/updates and old_entry on deletes; both carry the same
// Name on renames where new_parent_path differs.
func (r *Reader) extractBucketKey(resp *filer_pb.SubscribeMetadataResponse) (string, string, bool) {
	notif := resp.EventNotification
	dir := notif.NewParentPath
	if dir == "" {
		// On deletes, NewParentPath may be empty; the directory is encoded
		// in resp.Directory.
		dir = resp.Directory
	}
	var name string
	switch {
	case notif.NewEntry != nil:
		name = notif.NewEntry.Name
	case notif.OldEntry != nil:
		name = notif.OldEntry.Name
	default:
		return "", "", false
	}

	// Pre-normalized prefix (BucketsPath with trailing slash) is computed
	// once in Run; bucket-root events arrive as either "/buckets" or
	// "/buckets/", so accept both. The fallback path mirrors Run's
	// normalization for tests that call extractBucketKey directly.
	prefix := r.bucketsPathSlash
	if prefix == "" {
		prefix = r.BucketsPath
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}
	bare := strings.TrimSuffix(prefix, "/")
	var rest string
	switch {
	case dir == bare || dir == prefix:
		// Bucket create/delete at /buckets root: bucket name is the entry name.
		if name == "" {
			return "", "", false
		}
		return name, "", true
	case strings.HasPrefix(dir, prefix):
		rest = dir[len(prefix):]
	default:
		return "", "", false
	}
	// rest = "<bucket>" or "<bucket>/<sub>/<sub>..."
	slash := strings.IndexByte(rest, '/')
	var bucket, parentInBucket string
	if slash < 0 {
		bucket = rest
	} else {
		bucket = rest[:slash]
		parentInBucket = rest[slash+1:]
	}
	if bucket == "" {
		return "", "", false
	}
	if parentInBucket != "" {
		return bucket, parentInBucket + "/" + name, true
	}
	return bucket, name, true
}

// LogStartup is a small helper for callers that want a one-line readable
// description of where the reader is starting.
func (r *Reader) LogStartup() {
	glog.V(1).Infof("lifecycle reader: shard=%d sinceNs=%d budget=%d",
		r.ShardID, r.Cursor.MinTsNs(), r.EventBudget)
}
