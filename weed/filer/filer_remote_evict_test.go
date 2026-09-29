package filer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func remoteCachedEntry(path string, syncTsNs int64, mtime time.Time, chunks int) *Entry {
	entry := &Entry{
		FullPath: util.FullPath(path),
		Attr: Attr{
			Mtime:    mtime,
			Crtime:   mtime,
			Mode:     0644,
			Uid:      1,
			Gid:      1,
			Mime:     "application/octet-stream",
			Md5:      nil,
			FileSize: 0,
		},
		Remote: &filer_pb.RemoteEntry{
			RemoteMtime:       mtime.Unix(),
			LastLocalSyncTsNs: syncTsNs,
			RemoteETag:        "etag",
			RemoteSize:        100,
		},
	}
	for i := 0; i < chunks; i++ {
		entry.Chunks = append(entry.Chunks, &filer_pb.FileChunk{FileId: "1,01637037d6", Size: 100})
	}
	return entry
}

func TestIsEvictableRemoteEntry(t *testing.T) {
	now := time.Now()
	synced := now.Add(-time.Hour).UnixNano()
	mtime := now.Add(-time.Hour)

	tests := []struct {
		name     string
		entry    *Entry
		expected bool
	}{
		{"directory", &Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir | 0755}}, false},
		{"local only, no remote", &Entry{FullPath: "/f", Attr: Attr{Mtime: mtime, Mode: 0644}}, false},
		{"remote only, never cached", remoteCachedEntry("/f", 0, mtime, 0), false},
		{"remote, chunks cleared", remoteCachedEntry("/f", synced, mtime, 0), false},
		{"dirty, newer than remote", remoteCachedEntry("/f", synced, now, 1), false},
		{"same-second write after sync", remoteCachedEntry("/f",
			now.Truncate(time.Second).Add(100*time.Millisecond).UnixNano(),
			now.Truncate(time.Second).Add(900*time.Millisecond), 1), false},
		{"same-second sync after write", remoteCachedEntry("/f",
			now.Truncate(time.Second).Add(900*time.Millisecond).UnixNano(),
			now.Truncate(time.Second).Add(100*time.Millisecond), 1), true},
		{"synced and cached", remoteCachedEntry("/f", synced, mtime, 1), true},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, IsEvictableRemoteEntry(tt.entry), tt.name)
	}
}

func TestListEvictableRemoteEntries(t *testing.T) {
	now := time.Now()
	mtime := now.Add(-2 * time.Hour)
	oldSync := now.Add(-time.Hour).UnixNano()
	newSync := now.Add(-time.Second).UnixNano()

	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	mounts := []util.FullPath{"/buckets/mybucket"}

	seed := func(e *Entry) {
		require.NoError(t, f.CreateEntry(context.Background(), e, nil, false, false, nil, false, 255))
	}
	seed(remoteCachedEntry("/buckets/mybucket/old.bin", oldSync, mtime, 1))
	seed(remoteCachedEntry("/buckets/mybucket/fresh.bin", newSync, mtime, 1))
	seed(remoteCachedEntry("/buckets/mybucket/remoteonly.bin", 0, mtime, 0))
	seed(remoteCachedEntry("/buckets/mybucket/dirty.bin", oldSync, now, 1))
	seed(&Entry{FullPath: "/buckets/mybucket/plain.bin", Attr: Attr{Mtime: mtime, Mode: 0644}})

	store.entries["/buckets/mybucket/sub"] = &Entry{
		FullPath: "/buckets/mybucket/sub",
		Attr:     Attr{Mode: os.ModeDir | 0755, Mtime: mtime},
	}
	seed(remoteCachedEntry("/buckets/mybucket/sub/nested.bin", oldSync-1000, mtime, 1))

	got := f.ListEvictableRemoteEntries(context.Background(), mounts, 0)
	var paths []string
	for _, e := range got {
		paths = append(paths, string(e.FullPath))
	}
	assert.Equal(t, []string{
		"/buckets/mybucket/sub/nested.bin",
		"/buckets/mybucket/old.bin",
		"/buckets/mybucket/fresh.bin",
	}, paths, "oldest LastLocalSyncTsNs first; remote-only, dirty, local entries skipped")

	got = f.ListEvictableRemoteEntries(context.Background(), mounts, 30*time.Second)
	paths = paths[:0]
	for _, e := range got {
		paths = append(paths, string(e.FullPath))
	}
	assert.Equal(t, []string{
		"/buckets/mybucket/sub/nested.bin",
		"/buckets/mybucket/old.bin",
	}, paths, "minCacheAge excludes freshly cached entries")
}
