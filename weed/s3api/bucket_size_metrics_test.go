package s3api

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/bucketsize"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TestCollectionInfoLogicalSize verifies logical size excludes un-vacuumed
// garbage and never goes negative. Quota enforcement runs on this value so a
// bucket full of tombstones is not flipped read-only while its live data is
// under quota.
func TestCollectionInfoLogicalSize(t *testing.T) {
	cases := []struct {
		name     string
		size     float64
		deleted  float64
		expected float64
	}{
		{"no garbage", 1000, 0, 1000},
		{"some garbage", 1000, 300, 700},
		{"all garbage", 1000, 1000, 0},
		{"deleted exceeds size clamps to zero", 300, 1000, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info := &CollectionInfo{Size: tc.size, DeletedByteCount: tc.deleted}
			if got := info.LogicalSize(); got != tc.expected {
				t.Errorf("LogicalSize(): got %.0f, want %.0f", got, tc.expected)
			}
		})
	}
}

// quotaTree serves a filer directory tree for bucket sizing and counts the
// directories listed.
type quotaTree struct {
	filer_pb.SeaweedFilerClient
	tree    map[string][]*filer_pb.Entry
	errDirs map[string]error

	mu     sync.Mutex
	listed map[string]int
}

func (f *quotaTree) listings(dir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listed[dir]
}

func (f *quotaTree) totalListings() (n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.listed {
		n += c
	}
	return n
}

func (f *quotaTree) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(f)
}
func (f *quotaTree) AdjustedUrl(*filer_pb.Location) string { return "" }
func (f *quotaTree) GetDataCenter() string                 { return "" }

func (f *quotaTree) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	f.mu.Lock()
	if f.listed == nil {
		f.listed = map[string]int{}
	}
	f.listed[req.Directory]++
	f.mu.Unlock()
	if err := f.errDirs[req.Directory]; err != nil {
		return nil, err
	}
	if req.StartFromFileName != "" {
		return &quotaTreeStream{}, nil
	}
	return &quotaTreeStream{entries: f.tree[req.Directory]}, nil
}

type quotaTreeStream struct {
	grpc.ClientStream
	entries []*filer_pb.Entry
	idx     int
}

func (s *quotaTreeStream) Trailer() metadata.MD { return nil }
func (s *quotaTreeStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.idx >= len(s.entries) {
		return nil, io.EOF
	}
	s.idx++
	return &filer_pb.ListEntriesResponse{Entry: s.entries[s.idx-1]}, nil
}

// remoteOnlyBucket holds 3000 bytes, all remote-only (no chunks, as after an
// uncache or eviction), under one Iceberg table.
func remoteOnlyBucket(bucket string) map[string][]*filer_pb.Entry {
	b := "/buckets/" + bucket
	remote := func(name string, size int64) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{FileSize: uint64(size)},
			RemoteEntry: &filer_pb.RemoteEntry{StorageName: "bunny", RemoteSize: size}}
	}
	d := func(name string) *filer_pb.Entry { return &filer_pb.Entry{Name: name, IsDirectory: true} }
	return map[string][]*filer_pb.Entry{
		b:                    {d("ns")},
		b + "/ns":            {d("t")},
		b + "/ns/t":          {d("data"), d("metadata")},
		b + "/ns/t/data":     {remote("a.parquet", 2500)},
		b + "/ns/t/metadata": {remote("00001.metadata.json", 500)},
	}
}

// quotaServer is a gateway whose background sizer walks f with bucketsize.Size.
func quotaServer(f *quotaTree, interval time.Duration) *S3ApiServer {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets", QuotaFilerSizeInterval: interval}}
	s3a.quotaSizer = newFilerQuotaSizer(f, bucketsize.Size, interval)
	return s3a
}

func quotaBucket(name string, quota int64) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, IsDirectory: true, Quota: quota}
}

func mountedAt(dirs ...string) *remote_pb.RemoteStorageMapping {
	m := &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{}}
	for _, d := range dirs {
		m.Mappings[d] = &remote_pb.RemoteStorageLocation{Name: "bunny"}
	}
	return m
}

func quotaReadOnly(fc *filer.FilerConf, bucket string) bool {
	return fc.MatchStorageRule("/buckets/" + bucket + "/").ReadOnly
}

// the collection holds only the local cache: nothing, for remote-only bytes
var emptyCache = map[string]*CollectionInfo{"m": {Size: 0}}

// quotaPass runs one enforcement pass's mount refresh and apply against fc,
// with the bucket listing unchanged in between.
func quotaPass(s3a *S3ApiServer, fc *filer.FilerConf, buckets []*filer_pb.Entry, mounts *remote_pb.RemoteStorageMapping, collectionInfos map[string]*CollectionInfo) bool {
	mounted, ok := s3a.refreshQuotaMounts(buckets, mounts, nil)
	if !ok {
		panic("mount table refresh failed")
	}
	return s3a.applyBucketQuotas(fc, buckets, buckets, collectionInfos, mounted)
}

// testClock is a settable clock for the sizer's ages and backoff.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestQuotaMountedBucketSizedFromFiler(t *testing.T) {
	for _, tc := range []struct {
		quota        int64
		wantReadOnly bool
	}{
		{2999, true},  // 3000 remote-only bytes over the quota
		{3001, false}, // the same bytes under it
	} {
		f := &quotaTree{tree: remoteOnlyBucket("m")}
		s3a := quotaServer(f, 5*time.Minute)
		buckets := []*filer_pb.Entry{quotaBucket("m", tc.quota)}
		s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/m"), nil)
		s3a.quotaSizer.walkOne(context.Background())

		fc := filer.NewFilerConf()
		changed := quotaPass(s3a, fc, buckets, mountedAt("/buckets/m"), emptyCache)
		if changed != tc.wantReadOnly || quotaReadOnly(fc, "m") != tc.wantReadOnly {
			t.Errorf("quota %d: changed=%v readOnly=%v, want %v", tc.quota, changed, quotaReadOnly(fc, "m"), tc.wantReadOnly)
		}
		if size, fresh, _ := s3a.quotaSizer.size("m"); !fresh || size != 3000 {
			t.Errorf("published size %d fresh=%v, want 3000 for the metric", size, fresh)
		}
	}
}

func TestQuotaUnmountedBucketKeepsCollectionSize(t *testing.T) {
	// the filer would say 3000; an unmounted bucket is never walked and keeps
	// the collection's figure
	f := &quotaTree{tree: remoteOnlyBucket("u")}
	s3a := quotaServer(f, 5*time.Minute)
	buckets := []*filer_pb.Entry{quotaBucket("u", 2999)}
	s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/other"), nil)
	if targets, _, _ := s3a.quotaSizer.walkOne(context.Background()); targets != 0 || f.totalListings() != 0 {
		t.Fatal("unmounted bucket handed to the sizer")
	}
	fc := filer.NewFilerConf()
	if quotaPass(s3a, fc, buckets, mountedAt("/buckets/other"), map[string]*CollectionInfo{"u": {Size: 100}}) || quotaReadOnly(fc, "u") {
		t.Fatal("unmounted bucket under quota by its collection was flipped")
	}
	if !quotaPass(s3a, fc, buckets, mountedAt("/buckets/other"), map[string]*CollectionInfo{"u": {Size: 5000}}) || !quotaReadOnly(fc, "u") {
		t.Fatal("unmounted bucket over quota by its collection was not flipped")
	}
}

func TestQuotaMountedBucketUnknownSizeKeepsState(t *testing.T) {
	overByCollection := map[string]*CollectionInfo{"m": {Size: 1 << 30}}
	buckets := []*filer_pb.Entry{quotaBucket("m", 2999)}
	for _, wasReadOnly := range []bool{false, true} {
		newConf := func() *filer.FilerConf {
			fc := filer.NewFilerConf()
			if wasReadOnly {
				if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/m/", ReadOnly: true}); err != nil {
					t.Fatal(err)
				}
			}
			return fc
		}
		// never walked yet: the collection must not decide for a mounted bucket
		f := &quotaTree{tree: remoteOnlyBucket("m")}
		s3a := quotaServer(f, 5*time.Minute)
		fc := newConf()
		if quotaPass(s3a, fc, buckets, mountedAt("/buckets/m"), overByCollection) || quotaReadOnly(fc, "m") != wasReadOnly {
			t.Errorf("wasReadOnly=%v: never-walked mounted bucket changed", wasReadOnly)
		}
		// a walk error: the partial sum (2500) is under the quota, the whole over it
		f = &quotaTree{tree: remoteOnlyBucket("m"), errDirs: map[string]error{"/buckets/m/ns/t/metadata": errors.New("filer unavailable")}}
		s3a = quotaServer(f, 5*time.Minute)
		s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/m"), nil)
		s3a.quotaSizer.walkOne(context.Background())
		fc = newConf()
		if quotaPass(s3a, fc, buckets, mountedAt("/buckets/m"), overByCollection) || quotaReadOnly(fc, "m") != wasReadOnly {
			t.Errorf("wasReadOnly=%v: changed after a walk error", wasReadOnly)
		}
	}
}

// An unreadable mount table skips enforcement for the pass and leaves the
// sizer's targets as they were.
func TestQuotaUnreadableMountTableSkipsPass(t *testing.T) {
	f := &quotaTree{tree: remoteOnlyBucket("m")}
	s3a := quotaServer(f, 5*time.Minute)
	buckets := []*filer_pb.Entry{quotaBucket("m", 2999), quotaBucket("u", 2999)}
	if _, ok := s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/m"), nil); !ok {
		t.Fatal("good read refused")
	}
	if mounted, ok := s3a.refreshQuotaMounts(buckets, nil, errors.New("filer unavailable")); ok || mounted != nil {
		t.Fatalf("unreadable mount table: ok=%v mounted=%v, want the pass skipped", ok, mounted)
	}
	if targets, walked, _ := s3a.quotaSizer.walkOne(context.Background()); targets != 1 || !walked || f.listings("/buckets/m") != 1 {
		t.Fatalf("targets changed by an unreadable mount table: targets=%d walked=%v", targets, walked)
	}
}

// A quota changed between the pass's bucket listing and filer.conf's read is
// not applied stale.
func TestQuotaChangedDuringPassNotApplied(t *testing.T) {
	f := &quotaTree{tree: remoteOnlyBucket("m")}
	s3a := quotaServer(f, 5*time.Minute)
	buckets := []*filer_pb.Entry{quotaBucket("m", 2999)}
	mounted, _ := s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/m"), nil)
	s3a.quotaSizer.walkOne(context.Background())

	fc := filer.NewFilerConf()
	if s3a.applyBucketQuotas(fc, buckets, []*filer_pb.Entry{quotaBucket("m", 0)}, emptyCache, mounted) || quotaReadOnly(fc, "m") {
		t.Fatal("bucket flipped on a quota removed during the pass")
	}
	if !s3a.applyBucketQuotas(fc, buckets, buckets, emptyCache, mounted) || !quotaReadOnly(fc, "m") {
		t.Fatal("control: unchanged quota not applied")
	}
}

// A size older than the age limit is unknown (a hung or failing walker must
// not keep enforcing an old size), and a walk ends at its deadline.
func TestQuotaSizeGoesStale(t *testing.T) {
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	f := &quotaTree{tree: remoteOnlyBucket("m")}
	s3a := quotaServer(f, 5*time.Minute)
	s3a.quotaSizer.now = clock.Now
	buckets := []*filer_pb.Entry{quotaBucket("m", 2999)}
	s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/m"), nil)
	s3a.quotaSizer.walkOne(context.Background())

	clock.Add(9 * time.Minute) // within max(2 x 5m, 10m)
	if fc := filer.NewFilerConf(); !quotaPass(s3a, fc, buckets, mountedAt("/buckets/m"), emptyCache) {
		t.Fatal("fresh size not applied")
	}

	// the next walk hangs until its deadline
	s3a.quotaSizer.walk = func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	s3a.quotaSizer.walkTimeout = 20 * time.Millisecond
	done := make(chan struct{})
	go func() { s3a.quotaSizer.walkOne(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a hung walk outlived its deadline")
	}

	clock.Add(2 * time.Minute) // 11m after the last good walk
	if _, fresh, _ := s3a.quotaSizer.size("m"); fresh {
		t.Fatal("size older than the age limit still fresh (the metric would keep it)")
	}
	if fc := filer.NewFilerConf(); quotaPass(s3a, fc, buckets, mountedAt("/buckets/m"), emptyCache) || quotaReadOnly(fc, "m") {
		t.Fatal("stale size applied")
	}
}

// A walk is cancelled when its bucket stops being a target (or the lock is
// lost: setTargets(nil)), and a walk started under an earlier generation of
// the same target is not published.
func TestQuotaSizerCancelsAndDropsStaleWalks(t *testing.T) {
	started := make(chan struct{})
	walk := func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	s := newFilerQuotaSizer(nil, walk, 5*time.Minute)
	s.setTargets(map[string]util.FullPath{"a": "/buckets/a"})
	done := make(chan struct{})
	go func() { s.walkOne(context.Background()); close(done) }()
	<-started
	s.setTargets(nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("walk not cancelled when its bucket left the targets")
	}

	// the bucket leaves and comes back while its walk runs: that walk's
	// result belongs to the old generation
	var sz *filerQuotaSizer
	first := true
	sz = newFilerQuotaSizer(nil, func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
		if first {
			first = false
			sz.setTargets(nil)
			sz.setTargets(map[string]util.FullPath{"a": "/buckets/a"})
			return 111, nil
		}
		return 222, nil
	}, 5*time.Minute)
	sz.setTargets(map[string]util.FullPath{"a": "/buckets/a"})
	sz.walkOne(context.Background())
	if _, fresh, _ := sz.size("a"); fresh {
		t.Fatal("a walk from an earlier generation was published")
	}
	sz.walkOne(context.Background())
	if size, fresh, _ := sz.size("a"); !fresh || size != 222 {
		t.Fatalf("current walk: size=%d fresh=%v, want 222", size, fresh)
	}
}

// A failed bucket waits 10 s, doubling up to the interval, even at interval 0.
func TestQuotaSizerBacksOffAfterFailure(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		waits    []time.Duration
	}{
		{0, []time.Duration{10 * time.Second, 10 * time.Second}},
		{time.Minute, []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute}},
	} {
		clock := &testClock{t: time.Unix(1_700_000_000, 0)}
		walks := 0
		s := newFilerQuotaSizer(nil, func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
			walks++
			return 0, errors.New("filer unavailable")
		}, tc.interval)
		s.now = clock.Now
		s.setTargets(map[string]util.FullPath{"a": "/buckets/a"})
		s.walkOne(context.Background())
		for i, wait := range tc.waits {
			if _, walked, retryAt := s.walkOne(context.Background()); walked || retryAt.Sub(clock.Now()) != wait {
				t.Fatalf("interval %v, failure %d: walked=%v retry in %v, want %v", tc.interval, i+1, walked, retryAt.Sub(clock.Now()), wait)
			}
			clock.Add(wait)
			if _, walked, _ := s.walkOne(context.Background()); !walked {
				t.Fatalf("interval %v: not retried after %v", tc.interval, wait)
			}
		}
		if walks != len(tc.waits)+1 {
			t.Fatalf("interval %v: %d walks, want %d", tc.interval, walks, len(tc.waits)+1)
		}
	}
}

func TestQuotaSizerWalksLeastRecentlyWalkedFirst(t *testing.T) {
	var order []string
	walk := func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
		order = append(order, dir.Name())
		time.Sleep(time.Millisecond) // distinct walk times
		return 1, nil
	}
	s := newFilerQuotaSizer(nil, walk, 5*time.Minute)
	s.setTargets(map[string]util.FullPath{"a": "/buckets/a", "b": "/buckets/b", "c": "/buckets/c"})
	for i := 0; i < 4; i++ {
		s.walkOne(context.Background())
	}
	// a new bucket goes before every walked one
	s.setTargets(map[string]util.FullPath{"a": "/buckets/a", "b": "/buckets/b", "c": "/buckets/c", "d": "/buckets/d"})
	s.walkOne(context.Background())
	s.walkOne(context.Background())
	if got := strings.Join(order, ","); got != "a,b,c,a,d,b" {
		t.Fatalf("walk order %s, want a,b,c,a,d,b", got)
	}
}

// A slow walk holds up neither a quota pass nor the other buckets' sizes,
// and a pass makes no listing call of its own.
func TestQuotaPassNeverWaitsOnAWalk(t *testing.T) {
	tree := remoteOnlyBucket("fast")
	for k, v := range remoteOnlyBucket("slow") {
		tree[k] = v
	}
	f := &quotaTree{tree: tree}
	release := make(chan struct{})
	walk := func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
		if dir.Name() == "slow" {
			<-release
		}
		return bucketsize.Size(ctx, client, dir)
	}
	s3a := quotaServer(f, 5*time.Minute)
	s3a.quotaSizer = newFilerQuotaSizer(f, walk, 5*time.Minute)
	buckets := []*filer_pb.Entry{quotaBucket("fast", 2999), quotaBucket("slow", 2999)}
	mounted, _ := s3a.refreshQuotaMounts(buckets, mountedAt("/buckets/fast", "/buckets/slow"), nil)
	s3a.quotaSizer.walkOne(context.Background()) // fast
	walking := make(chan struct{})
	go func() { s3a.quotaSizer.walkOne(context.Background()); close(walking) }() // slow, blocked
	defer func() { close(release); <-walking }()
	time.Sleep(20 * time.Millisecond)

	listingsBefore := f.totalListings()
	done := make(chan bool)
	fc := filer.NewFilerConf()
	go func() { done <- s3a.applyBucketQuotas(fc, buckets, buckets, nil, mounted) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("quota pass waited on a walk in progress")
	}
	if !quotaReadOnly(fc, "fast") || quotaReadOnly(fc, "slow") {
		t.Fatalf("fast readOnly=%v (want true), slow readOnly=%v (want false, unknown)", quotaReadOnly(fc, "fast"), quotaReadOnly(fc, "slow"))
	}
	if n := f.totalListings() - listingsBefore; n != 0 {
		t.Fatalf("quota pass made %d listing calls", n)
	}
}

func TestQuotaSizerPacesWalksOverTheInterval(t *testing.T) {
	if got := paceAfterWalk(5*time.Minute, 5, 10*time.Second); got != 50*time.Second {
		t.Errorf("5 buckets over 5m, walk took 10s: pause %v, want 50s", got)
	}
	if got := paceAfterWalk(5*time.Minute, 5, 2*time.Minute); got != 0 {
		t.Errorf("walk longer than its share: pause %v, want 0", got)
	}

	var mu sync.Mutex
	walked := map[string]int{}
	walk := func(ctx context.Context, client filer_pb.FilerClient, dir util.FullPath) (uint64, error) {
		mu.Lock()
		walked[dir.Name()]++
		mu.Unlock()
		return 1, nil
	}
	count := func(name string) int {
		mu.Lock()
		defer mu.Unlock()
		return walked[name]
	}
	runFor := func(s *filerQuotaSizer, d time.Duration, during func()) {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		go during()
		s.run(ctx)
	}

	// interval 1h: one walk, then a pause that a new target ends at once
	s := newFilerQuotaSizer(nil, walk, time.Hour)
	s.setTargets(map[string]util.FullPath{"a": "/buckets/a"})
	runFor(s, 200*time.Millisecond, func() {
		time.Sleep(50 * time.Millisecond)
		s.setTargets(map[string]util.FullPath{"a": "/buckets/a", "b": "/buckets/b"})
	})
	if count("a") != 1 || count("b") != 1 {
		t.Errorf("interval 1h: walks a=%d b=%d, want 1 each (b promptly on its arrival)", count("a"), count("b"))
	}

	// interval 0: back to back
	s0 := newFilerQuotaSizer(nil, walk, 0)
	s0.setTargets(map[string]util.FullPath{"c": "/buckets/c"})
	runFor(s0, 100*time.Millisecond, func() {})
	if count("c") < 3 {
		t.Errorf("interval 0: %d walks in 100ms, want back to back", count("c"))
	}
}
