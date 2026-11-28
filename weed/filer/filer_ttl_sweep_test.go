package filer

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func putSweepTestEntry(t *testing.T, store *stubFilerStore, path string, isDir bool, ttlSec int32, crtime time.Time) {
	t.Helper()
	mode := os.FileMode(0o644)
	if isDir {
		mode = os.ModeDir | 0o755
	}
	entry := &Entry{FullPath: util.FullPath(path), Attr: Attr{Mode: mode, Crtime: crtime, Mtime: crtime, TtlSec: ttlSec}}
	if err := store.InsertEntry(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
}

func TestSweepExpiredEntriesRemovesRowsNobodyVisits(t *testing.T) {
	f, store := newTestFilerWithStubStore(t)
	f.DirBucketsPath = "/buckets"
	f.FilerConf = NewFilerConf()
	for _, rule := range []*filer_pb.FilerConf_PathConf{
		{LocationPrefix: "/buckets/ttl-demo/", Ttl: "2m"},
		{LocationPrefix: "/ttl-http/logs", Ttl: "3d"},
	} {
		if err := f.FilerConf.SetLocationConf(rule); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	old := now.Add(-time.Hour)
	for _, dir := range []string{"/buckets", "/buckets/ttl-demo", "/buckets/other", "/ttl-http"} {
		putSweepTestEntry(t, store, dir, true, 0, old)
	}
	for _, dir := range []string{"day01", "day02", "day03"} {
		putSweepTestEntry(t, store, "/buckets/ttl-demo/"+dir, true, 0, old)
	}
	for i := 0; i < expiredEntrySweepPageSize+300; i++ {
		putSweepTestEntry(t, store, fmt.Sprintf("/buckets/ttl-demo/day01/f%05d", i), false, 120, old)
	}
	putSweepTestEntry(t, store, "/buckets/ttl-demo/day02/f1", false, 120, old)
	putSweepTestEntry(t, store, "/buckets/ttl-demo/day03/old", false, 120, old)
	putSweepTestEntry(t, store, "/buckets/ttl-demo/day03/fresh", false, 120, now)
	putSweepTestEntry(t, store, "/buckets/other/old", false, 120, old)
	putSweepTestEntry(t, store, "/ttl-http/logs1", false, 120, old)
	putSweepTestEntry(t, store, "/ttl-http/other", false, 120, old)

	lastSwept := make(map[string]time.Time)
	f.sweepExpiredEntries(context.Background(), "", lastSwept, now)

	gone := []string{
		"/buckets/ttl-demo/day01", "/buckets/ttl-demo/day01/f00000", "/buckets/ttl-demo/day01/f01300",
		"/buckets/ttl-demo/day02", "/buckets/ttl-demo/day02/f1",
		"/buckets/ttl-demo/day03/old", "/ttl-http/logs1",
	}
	for _, p := range gone {
		if store.getEntry(p) != nil {
			t.Errorf("%s still stored after the sweep", p)
		}
	}
	kept := []string{
		"/buckets/ttl-demo", "/buckets/ttl-demo/day03", "/buckets/ttl-demo/day03/fresh",
		"/buckets/other/old", "/ttl-http/other",
	}
	for _, p := range kept {
		if store.getEntry(p) == nil {
			t.Errorf("%s removed by the sweep", p)
		}
	}
	if !lastSwept["/buckets/ttl-demo/"].Equal(now) || !lastSwept["/ttl-http/logs"].Equal(now) {
		t.Fatalf("sweep times not recorded: %v", lastSwept)
	}

	putSweepTestEntry(t, store, "/buckets/ttl-demo/day03/later", false, 120, old)
	f.sweepExpiredEntries(context.Background(), "", lastSwept, now.Add(30*time.Minute))
	if store.getEntry("/buckets/ttl-demo/day03/later") == nil {
		t.Fatal("swept again before its interval passed")
	}
	f.sweepExpiredEntries(context.Background(), "", lastSwept, now.Add(expiredEntrySweepMinInterval))
	if store.getEntry("/buckets/ttl-demo/day03/later") != nil {
		t.Fatal("not swept once its interval passed")
	}
}

func TestExpiredEntrySweepPrefixes(t *testing.T) {
	fc := NewFilerConf()
	for _, rule := range []*filer_pb.FilerConf_PathConf{
		{LocationPrefix: "/a/", Ttl: "3d"},
		{LocationPrefix: "/a/b/", Ttl: "2m"},
		{LocationPrefix: "/a0/", Ttl: "1w"},
		{LocationPrefix: "/c/", Collection: "c"},
		{LocationPrefix: "/d/", Ttl: "5h"},
	} {
		if err := fc.SetLocationConf(rule); err != nil {
			t.Fatal(err)
		}
	}
	got := expiredEntrySweepPrefixes(fc)
	want := map[string]time.Duration{
		"/a/":  expiredEntrySweepMinInterval,
		"/a0/": expiredEntrySweepMaxInterval,
		"/d/":  5 * time.Hour,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for prefix, interval := range want {
		if got[prefix] != interval {
			t.Errorf("%s: got %v, want %v", prefix, got[prefix], interval)
		}
	}
}

func TestSweepsExpiredEntriesOnlyOnceForASharedStore(t *testing.T) {
	f, _ := newTestFilerWithStubStore(t)
	self, peer := pb.ServerAddress("filer1:8888"), pb.ServerAddress("filer2:8888")
	f.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{self, peer}, 1)
	f.MetaAggregator = &MetaAggregator{peerSharesStore: map[pb.ServerAddress]bool{peer: true}}

	var ownedBySelf, ownedByPeer string
	for i := 0; ownedBySelf == "" || ownedByPeer == ""; i++ {
		prefix := fmt.Sprintf("/buckets/b%d/", i)
		if f.Dlm.LockRing.GetPrimary(prefix) == self {
			ownedBySelf = prefix
		} else {
			ownedByPeer = prefix
		}
	}
	if !f.sweepsExpiredEntries(ownedBySelf, self) {
		t.Errorf("%s: owner does not sweep", ownedBySelf)
	}
	if f.sweepsExpiredEntries(ownedByPeer, self) {
		t.Errorf("%s: swept here although its owner shares this store", ownedByPeer)
	}
	f.MetaAggregator.peerSharesStore[peer] = false
	if !f.sweepsExpiredEntries(ownedByPeer, self) {
		t.Errorf("%s: not swept here although its owner has a store of its own", ownedByPeer)
	}
}

func TestSweepExpiredEntriesKeepsADirectoryKeyObject(t *testing.T) {
	f, store := newTestFilerWithStubStore(t)
	f.DirBucketsPath = "/buckets"
	f.FilerConf = NewFilerConf()
	if err := f.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/", Ttl: "2m"}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	putSweepTestEntry(t, store, "/buckets", true, 0, old)
	putSweepTestEntry(t, store, "/buckets/b", true, 0, old)
	reports := &Entry{FullPath: "/buckets/b/reports", Attr: Attr{Mode: os.ModeDir | 0o755, Crtime: old, Mtime: old, Mime: "text/plain"},
		Extended: map[string][]byte{s3_constants.SeaweedFSPrefixObject: []byte("true")}}
	if err := store.InsertEntry(context.Background(), reports); err != nil {
		t.Fatal(err)
	}
	putSweepTestEntry(t, store, "/buckets/b/reports/day1", false, 120, old)

	f.sweepExpiredEntries(context.Background(), "", make(map[string]time.Time), time.Now())

	if store.getEntry("/buckets/b/reports/day1") != nil {
		t.Fatal("expired child not removed")
	}
	if store.getEntry("/buckets/b/reports") == nil {
		t.Fatal("directory holding an object removed with its last child")
	}
}

func TestDeleteExpiredEntryKeepsAReplacement(t *testing.T) {
	f, store := newTestFilerWithStubStore(t)
	ctx := context.Background()
	p := util.FullPath("/logs/current")

	putSweepTestEntry(t, store, string(p), false, 120, time.Now())
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if store.getEntry(string(p)) == nil {
		t.Fatal("live replacement removed")
	}

	putSweepTestEntry(t, store, string(p), false, 120, time.Now().Add(-time.Hour))
	lock := f.EntryLockTable.AcquireLock("test", p, util.ExclusiveLock)
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	f.EntryLockTable.ReleaseLock(p, lock)
	if store.getEntry(string(p)) == nil {
		t.Fatal("removed while a writer held the path")
	}

	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if store.getEntry(string(p)) != nil {
		t.Fatal("expired entry kept")
	}
}
