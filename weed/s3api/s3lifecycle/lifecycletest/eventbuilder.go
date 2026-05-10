package lifecycletest

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

// EventOption mutates a reader.Event during construction. Options compose
// in order — later options override earlier ones if they touch the same
// field, which lets a default+override pattern work without surprises.
type EventOption func(*reader.Event)

// WithSize sets the Size on whichever entry the event populates (NewEntry
// for Create / Update, OldEntry for Delete). Constructors that build
// both entries apply the size to NewEntry.
func WithSize(bytes int64) EventOption {
	return func(e *reader.Event) {
		if e.NewEntry != nil && e.NewEntry.Attributes != nil {
			e.NewEntry.Attributes.FileSize = uint64(bytes)
		} else if e.OldEntry != nil && e.OldEntry.Attributes != nil {
			e.OldEntry.Attributes.FileSize = uint64(bytes)
		}
	}
}

// WithModTime sets Mtime/MtimeNs on the populated entry.
func WithModTime(t time.Time) EventOption {
	return func(e *reader.Event) {
		secs, nanos := t.Unix(), int32(t.Nanosecond())
		if e.NewEntry != nil && e.NewEntry.Attributes != nil {
			e.NewEntry.Attributes.Mtime = secs
			e.NewEntry.Attributes.MtimeNs = nanos
		} else if e.OldEntry != nil && e.OldEntry.Attributes != nil {
			e.OldEntry.Attributes.Mtime = secs
			e.OldEntry.Attributes.MtimeNs = nanos
		}
	}
}

// WithTtlSec sets the TTL on the populated entry. Used by tests that
// exercise the lifecycle metadata-only delete path (TtlSec > 0 means
// the volume reclaims chunks naturally).
func WithTtlSec(ttl int32) EventOption {
	return func(e *reader.Event) {
		if e.NewEntry != nil && e.NewEntry.Attributes != nil {
			e.NewEntry.Attributes.TtlSec = ttl
		} else if e.OldEntry != nil && e.OldEntry.Attributes != nil {
			e.OldEntry.Attributes.TtlSec = ttl
		}
	}
}

// WithVersionID stamps Seaweed-X-Amz-Version-Id on the populated entry's
// Extended map. Used by versioning-aware router and dispatcher tests.
func WithVersionID(versionID string) EventOption {
	return func(e *reader.Event) {
		var entry *filer_pb.Entry
		if e.NewEntry != nil {
			entry = e.NewEntry
		} else if e.OldEntry != nil {
			entry = e.OldEntry
		}
		if entry == nil {
			return
		}
		if entry.Extended == nil {
			entry.Extended = map[string][]byte{}
		}
		entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionID)
	}
}

// WithExtended sets an arbitrary Extended key/value on the populated entry.
func WithExtended(key string, value []byte) EventOption {
	return func(e *reader.Event) {
		var entry *filer_pb.Entry
		if e.NewEntry != nil {
			entry = e.NewEntry
		} else if e.OldEntry != nil {
			entry = e.OldEntry
		}
		if entry == nil {
			return
		}
		if entry.Extended == nil {
			entry.Extended = map[string][]byte{}
		}
		entry.Extended[key] = value
	}
}

// WithChunks attaches FileChunks to the populated entry. Identity-CAS in
// LifecycleDelete uses the head chunk's FID, so tests that exercise CAS
// drift need at least one chunk. For Update events the chunks land on
// NewEntry; use WithOldChunks to target the pre-update state.
func WithChunks(chunks ...*filer_pb.FileChunk) EventOption {
	return func(e *reader.Event) {
		if e.NewEntry != nil {
			e.NewEntry.Chunks = append(e.NewEntry.Chunks, chunks...)
		} else if e.OldEntry != nil {
			e.OldEntry.Chunks = append(e.OldEntry.Chunks, chunks...)
		}
	}
}

// WithOldSize sets FileSize on OldEntry specifically. Use on Update
// events to configure the pre-update size (the WithSize default lands
// on NewEntry when both are populated).
func WithOldSize(bytes int64) EventOption {
	return func(e *reader.Event) {
		if e.OldEntry != nil && e.OldEntry.Attributes != nil {
			e.OldEntry.Attributes.FileSize = uint64(bytes)
		}
	}
}

// WithOldChunks attaches FileChunks to OldEntry specifically. Use on
// Update events to configure the pre-update chunk list (WithChunks
// targets NewEntry when both are populated).
func WithOldChunks(chunks ...*filer_pb.FileChunk) EventOption {
	return func(e *reader.Event) {
		if e.OldEntry != nil {
			e.OldEntry.Chunks = append(e.OldEntry.Chunks, chunks...)
		}
	}
}

// WithOldModTime sets Mtime/MtimeNs on OldEntry specifically. Update
// events whose pre-update mtime should differ from the event timestamp
// use this to override the default.
func WithOldModTime(t time.Time) EventOption {
	return func(e *reader.Event) {
		if e.OldEntry != nil && e.OldEntry.Attributes != nil {
			e.OldEntry.Attributes.Mtime = t.Unix()
			e.OldEntry.Attributes.MtimeNs = int32(t.Nanosecond())
		}
	}
}

// WithBootstrapVersion attaches a BootstrapVersion to the event.
// Bootstrap-walk events use this to carry per-version state the live
// meta-log doesn't see.
func WithBootstrapVersion(bv *reader.BootstrapVersion) EventOption {
	return func(e *reader.Event) { e.BootstrapVersion = bv }
}

// WithShardID overrides the computed ShardID. Most tests should leave
// it as the s3lifecycle.ShardID default to mirror production routing.
func WithShardID(shard int) EventOption {
	return func(e *reader.Event) { e.ShardID = shard }
}

// NewCreate builds a Create event (NewEntry populated, OldEntry nil) at
// the given timestamp. Bucket and key are required; everything else is
// derived (Mtime defaults to ts, FileSize defaults to 0). Apply options
// to override.
func NewCreate(bucket, key string, ts time.Time, opts ...EventOption) *reader.Event {
	e := &reader.Event{
		TsNs:    ts.UnixNano(),
		Bucket:  bucket,
		Key:     key,
		ShardID: s3lifecycle.ShardID(bucket, key),
		NewEntry: &filer_pb.Entry{
			Name: leafOf(key),
			Attributes: &filer_pb.FuseAttributes{
				Mtime:   ts.Unix(),
				MtimeNs: int32(ts.Nanosecond()),
			},
		},
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// NewDelete builds a Delete event (OldEntry populated, NewEntry nil).
// Tests usually pass the entry the prior Create produced as a snapshot
// of pre-delete state; this helper builds a minimal stand-in.
func NewDelete(bucket, key string, ts time.Time, opts ...EventOption) *reader.Event {
	e := &reader.Event{
		TsNs:    ts.UnixNano(),
		Bucket:  bucket,
		Key:     key,
		ShardID: s3lifecycle.ShardID(bucket, key),
		OldEntry: &filer_pb.Entry{
			Name: leafOf(key),
			Attributes: &filer_pb.FuseAttributes{
				Mtime:   ts.Unix(),
				MtimeNs: int32(ts.Nanosecond()),
			},
		},
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// NewUpdate builds an Update event (both OldEntry and NewEntry populated).
// Same defaults as NewCreate; options apply to NewEntry per the rules in
// each option's doc.
func NewUpdate(bucket, key string, ts time.Time, opts ...EventOption) *reader.Event {
	e := &reader.Event{
		TsNs:    ts.UnixNano(),
		Bucket:  bucket,
		Key:     key,
		ShardID: s3lifecycle.ShardID(bucket, key),
		OldEntry: &filer_pb.Entry{
			Name: leafOf(key),
			Attributes: &filer_pb.FuseAttributes{
				Mtime:   ts.Unix(),
				MtimeNs: int32(ts.Nanosecond()),
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: leafOf(key),
			Attributes: &filer_pb.FuseAttributes{
				Mtime:   ts.Unix(),
				MtimeNs: int32(ts.Nanosecond()),
			},
		},
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// MetaLogClock produces monotonically increasing timestamps for fixture
// generation. Each call to Next advances by Step (default 1s) so tests
// don't have to thread a counter through every helper invocation. Safe
// for concurrent use.
type MetaLogClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

// NewMetaLogClock returns a clock that ticks forward Step on every Next
// call. Step defaults to 1s when zero.
func NewMetaLogClock(start time.Time, step time.Duration) *MetaLogClock {
	if step <= 0 {
		step = time.Second
	}
	return &MetaLogClock{now: start, step: step}
}

// Next returns the current timestamp and advances by Step.
func (c *MetaLogClock) Next() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

// Peek returns what Next() would return without advancing.
func (c *MetaLogClock) Peek() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// leafOf returns the basename of a slash-separated key. Filer entries
// store only the leaf name; tests that mirror production layout need
// the same shape. Trailing slashes are stripped first so directory-key
// fixtures (e.g. "folder/") get the slashless leaf "folder" — the
// production directory-marker write path stores the name without the
// trailing slash.
func leafOf(key string) string {
	for len(key) > 0 && key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[i+1:]
		}
	}
	return key
}
