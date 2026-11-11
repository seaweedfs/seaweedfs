package etcd

import (
	"context"
	"errors"
	"os"
	"sort"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type fakeVersion struct {
	rev   int64
	value []byte // nil marks a delete
}

// fakeMVCC keeps every revision of every key, as etcd does until compaction.
type fakeMVCC struct {
	clientv3.KV
	rev       int64
	compacted int64
	keys      map[string][]fakeVersion
	gets      int
	beforeGet func(gets int)
}

func newFakeMVCC() *fakeMVCC {
	return &fakeMVCC{keys: map[string][]fakeVersion{}}
}

func (m *fakeMVCC) put(key string, value []byte) {
	m.rev++
	m.keys[key] = append(m.keys[key], fakeVersion{m.rev, value})
}

func (m *fakeMVCC) del(key string) {
	m.rev++
	m.keys[key] = append(m.keys[key], fakeVersion{m.rev, nil})
}

func (m *fakeMVCC) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	m.gets++
	if m.beforeGet != nil {
		m.beforeGet(m.gets)
	}
	op := clientv3.OpGet(key, opts...)
	rev := op.Rev()
	if rev == 0 {
		rev = m.rev
	}
	if rev < m.compacted {
		return nil, rpctypes.ErrCompacted
	}
	end := string(op.RangeBytes())
	var names []string
	for k := range m.keys {
		if k == key || (end != "" && k > key && k < end) {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	resp := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: rev}}
	for _, k := range names {
		var value []byte
		for _, v := range m.keys[k] {
			if v.rev <= rev {
				value = v.value
			}
		}
		if value == nil {
			continue
		}
		if limit := op.Limit(); limit > 0 && int64(len(resp.Kvs)) == limit {
			resp.More = true
			break
		}
		resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(k), Value: value})
	}
	return resp, nil
}

const testKeyPrefix = "pfx"

func (m *fakeMVCC) putEntry(t *testing.T, p util.FullPath, isDir bool) {
	t.Helper()
	entry := &filer.Entry{FullPath: p}
	if isDir {
		entry.Attr.Mode = os.ModeDir | 0755
	} else {
		entry.Chunks = []*filer_pb.FileChunk{{FileId: "3,01" + p.Name(), Size: 1}}
	}
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		t.Fatal(err)
	}
	m.put(entryKey(p), meta)
}

func entryKey(p util.FullPath) string {
	return testKeyPrefix + string(genKey(p.DirAndName()))
}

func scanPaths(t *testing.T, m *fakeMVCC, dirPath util.FullPath) map[util.FullPath]int {
	t.Helper()
	seen := map[util.FullPath]int{}
	if err := scanEntries(context.Background(), m, testKeyPrefix, dirPath, func(_ context.Context, entry *filer.Entry) error {
		seen[entry.FullPath]++
		return nil
	}); err != nil {
		t.Fatalf("scan %s: %v", dirPath, err)
	}
	return seen
}

func withScanPageSize(t *testing.T, n int64) {
	prev := scanPageSize
	scanPageSize = n
	t.Cleanup(func() { scanPageSize = prev })
}

func TestScanEntriesSeesFileMovedDuringScan(t *testing.T) {
	withScanPageSize(t, 2)
	m := newFakeMVCC()
	for _, d := range []util.FullPath{"/buckets", "/buckets/b", "/buckets/b/aaa", "/buckets/b/pad", "/buckets/b/zzz", "/buckets/b/zzz/late", "/buckets/bc"} {
		m.putEntry(t, d, true)
	}
	for _, f := range []util.FullPath{"/buckets/b/pad/p0", "/buckets/b/pad/p1", "/buckets/b/pad/p2", "/buckets/b/zzz/late/f1", "/buckets/b/zzz/late/f2", "/buckets/bc/x"} {
		m.putEntry(t, f, false)
	}
	m.put(testKeyPrefix+"/buckets/b/hardlink-without-separator", []byte("not an entry"))

	// Pages run root, children of /buckets/b, then deeper paths in key order:
	// the move lands after aaa's subtree was read and before late's.
	m.beforeGet = func(gets int) {
		if gets == 5 {
			from, to := util.FullPath("/buckets/b/zzz/late/f1"), util.FullPath("/buckets/b/aaa/f1")
			value := m.keys[entryKey(from)][len(m.keys[entryKey(from)])-1].value
			m.del(entryKey(from))
			m.put(entryKey(to), value)
		}
	}

	seen := scanPaths(t, m, "/buckets/b")
	want := []util.FullPath{"/buckets/b", "/buckets/b/aaa", "/buckets/b/pad", "/buckets/b/zzz", "/buckets/b/zzz/late",
		"/buckets/b/pad/p0", "/buckets/b/pad/p1", "/buckets/b/pad/p2", "/buckets/b/zzz/late/f1", "/buckets/b/zzz/late/f2"}
	for _, p := range want {
		if seen[p] != 1 {
			t.Errorf("%s seen %d times, want once", p, seen[p])
		}
	}
	if len(seen) != len(want) {
		t.Errorf("seen %v, want exactly %v", seen, want)
	}
	if m.gets < 5 {
		t.Fatalf("the move never ran: %d reads", m.gets)
	}
	if _, found := m.keys[entryKey("/buckets/b/aaa/f1")]; !found {
		t.Fatal("the move did not land")
	}
}

func TestScanEntriesFromRoot(t *testing.T) {
	withScanPageSize(t, 3)
	m := newFakeMVCC()
	m.putEntry(t, "/a", true)
	m.putEntry(t, "/a/x", false)
	m.putEntry(t, "/top", false)
	m.put(testKeyPrefix+"filer.store.id", []byte{1, 2, 3, 4})
	m.put(testKeyPrefix+"/\x00", []byte("root marker"))
	for _, id := range []string{"/\x00\xff\xfe\x80abcdefghijk\x01", "/\x00abcdefghijklmn\x01"} {
		link, err := (&filer.Entry{FullPath: "/top", HardLinkId: filer.HardLinkId(id), HardLinkCounter: 2}).EncodeAttributesAndChunks()
		if err != nil {
			t.Fatal(err)
		}
		m.put(testKeyPrefix+id, link)
	}
	m.putEntry(t, "/\xc3\x28xyz/name", false)

	seen := scanPaths(t, m, "/")
	if len(seen) != 4 || seen["/a"] != 1 || seen["/a/x"] != 1 || seen["/top"] != 1 || seen["/_(xyz/name"] != 1 {
		t.Fatalf("seen %v", seen)
	}
}

func TestScanEntriesReadsHardLinkAtScanRevision(t *testing.T) {
	m := newFakeMVCC()
	m.putEntry(t, "/d", true)
	m.putEntry(t, "/d/f", false)
	m.put(testKeyPrefix+"link", []byte("v1"))
	m.beforeGet = func(gets int) {
		if gets == 2 {
			m.put(testKeyPrefix+"link", []byte("v2"))
		}
	}
	var got []string
	err := scanEntries(context.Background(), m, testKeyPrefix, "/d", func(ctx context.Context, entry *filer.Entry) error {
		resp, err := m.Get(ctx, testKeyPrefix+"link", atScanRevision(ctx)...)
		if err != nil {
			return err
		}
		got = append(got, string(resp.Kvs[0].Value))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range got {
		if v != "v1" {
			t.Fatalf("hard link read %q inside the scan, want the scan revision's v1", got)
		}
	}
	if len(got) != 2 {
		t.Fatalf("visited %d entries, want 2", len(got))
	}
}

func TestScanEntriesFailsWhenRevisionCompacted(t *testing.T) {
	withScanPageSize(t, 1)
	m := newFakeMVCC()
	m.putEntry(t, "/d", true)
	m.putEntry(t, "/d/a", false)
	m.putEntry(t, "/d/b", false)
	m.beforeGet = func(gets int) {
		if gets == 3 {
			m.putEntry(t, "/d/c", false)
			m.compacted = m.rev
		}
	}
	err := scanEntries(context.Background(), m, testKeyPrefix, "/d", func(context.Context, *filer.Entry) error { return nil })
	if !errors.Is(err, rpctypes.ErrCompacted) {
		t.Fatalf("got %v, want a compaction error", err)
	}
	if errors.Is(err, filer.ErrUnsupportedEntryScan) {
		t.Fatalf("a compacted scan must fail, not fall back to the walk: %v", err)
	}
}

func TestScanEntriesRejectsSanitizedPathOfAnotherEntry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		paths   []util.FullPath
		lateDup bool
		wantErr bool
	}{
		{name: "free", paths: []util.FullPath{"/\xc3\x28xyz/name"}},
		{name: "stored entry", paths: []util.FullPath{"/\xc3\x28xyz/name", "/_(xyz/name"}, wantErr: true},
		{name: "two invalid directories", paths: []util.FullPath{"/a\xff/b", "/a\xfe/b"}, wantErr: true},
		{name: "created after the scan revision", paths: []util.FullPath{"/\xc3\x28xyz/name"}, lateDup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withScanPageSize(t, 1)
			m := newFakeMVCC()
			for _, p := range tc.paths {
				m.putEntry(t, p, false)
			}
			if tc.lateDup {
				m.beforeGet = func(gets int) {
					if gets == 2 {
						m.putEntry(t, "/_(xyz/name", false)
					}
				}
			}
			var seen []util.FullPath
			err := scanEntries(context.Background(), m, testKeyPrefix, "/", func(_ context.Context, entry *filer.Entry) error {
				seen = append(seen, entry.FullPath)
				return nil
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("scan succeeded with %v, want a collision error", seen)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(seen) != 1 || seen[0] != "/_(xyz/name" {
				t.Fatalf("seen %v", seen)
			}
		})
	}
}
