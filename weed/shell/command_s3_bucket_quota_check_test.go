package shell

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/bucketsize"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// quotaTreeFiler serves a directory tree from a map and records which
// directories were listed. Listing a directory in errDirs fails.
type quotaTreeFiler struct {
	filer_pb.SeaweedFilerClient
	tree    map[string][]*filer_pb.Entry
	errDirs map[string]error

	mu     sync.Mutex
	listed []string
}

func (f *quotaTreeFiler) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	f.mu.Lock()
	f.listed = append(f.listed, req.Directory)
	f.mu.Unlock()
	if err := f.errDirs[req.Directory]; err != nil {
		return nil, err
	}
	if req.StartFromFileName != "" {
		// every directory here fits in one page
		return &stubListStream{}, nil
	}
	return &stubListStream{entries: f.tree[req.Directory]}, nil
}

func (f *quotaTreeFiler) listedUnder(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, d := range f.listed {
		if d == prefix || strings.HasPrefix(d, prefix+"/") {
			out = append(out, d)
		}
	}
	return out
}

func quotaDir(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
}

// remoteOnly is a remote-mounted file whose local cache is gone: no chunks,
// as remote.uncache and eviction leave it (they keep the attribute size).
func remoteOnly(name string, size int64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		Attributes:  &filer_pb.FuseAttributes{Mtime: 2000, FileSize: uint64(size)},
		RemoteEntry: &filer_pb.RemoteEntry{StorageName: "bunny", RemoteMtime: 1000, RemoteSize: size},
	}
}

// localFile is a file whose bytes are in local chunks; remoteSize > 0 makes it
// also present on the remote (cached and written back).
func localFile(name string, chunkSize uint64, remoteSize int64) *filer_pb.Entry {
	e := &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{Mtime: 2000, FileSize: chunkSize},
		Chunks:     []*filer_pb.FileChunk{{FileId: "1,01", Offset: 0, Size: chunkSize}},
	}
	if remoteSize > 0 {
		e.RemoteEntry = &filer_pb.RemoteEntry{StorageName: "bunny", RemoteMtime: 1000, RemoteSize: remoteSize}
	}
	return e
}

func quotaBucketEntry(name string, quota int64) *filer_pb.Entry {
	e := quotaDir(name)
	e.Quota = quota
	return e
}

// remoteOnlyTree is one Iceberg table whose files are all remote-only,
// 3000 bytes in all, spread over nested directories.
func remoteOnlyTree(bucket string) map[string][]*filer_pb.Entry {
	b := "/buckets/" + bucket
	return map[string][]*filer_pb.Entry{
		b:                    {quotaDir("ns")},
		b + "/ns":            {quotaDir("t")},
		b + "/ns/t":          {quotaDir("data"), quotaDir("metadata")},
		b + "/ns/t/data":     {remoteOnly("a.parquet", 1500), remoteOnly("b.parquet", 1000)},
		b + "/ns/t/metadata": {remoteOnly("00001.metadata.json", 500)},
	}
}

func filerQuotaSizer(f *quotaTreeFiler) bucketSizeFunc {
	return func(entry *filer_pb.Entry) (float64, error) {
		size, err := bucketsize.Size(context.Background(), stubFilerClient{client: f}, util.FullPath("/buckets").Child(entry.Name))
		return float64(size), err
	}
}

// enforceOne enforces one bucket's quota against fc, reporting a size error.
func enforceOne(fc *filer.FilerConf, bucket *filer_pb.Entry, out io.Writer, sizer bucketSizeFunc) (bool, error) {
	buckets := []*filer_pb.Entry{bucket}
	_, changed, unsized, err := (&commandS3BucketQuotaEnforce{}).enforceQuotas(buckets, "/buckets", sizer, func() (*filer.FilerConf, error) { return fc, nil }, sameBuckets(buckets), out)
	if err == nil && len(unsized) > 0 {
		err = errors.New("bucket not sized")
	}
	return changed, err
}

func sameBuckets(buckets []*filer_pb.Entry) func() ([]*filer_pb.Entry, error) {
	return func() ([]*filer_pb.Entry, error) { return buckets, nil }
}

func readOnlyAt(fc *filer.FilerConf, bucket string) bool {
	return fc.MatchStorageRule("/buckets/" + bucket + "/").ReadOnly
}

func TestQuotaEnforceRemoteOnlyBucketOverQuotaGoesReadOnly(t *testing.T) {
	f := &quotaTreeFiler{tree: remoteOnlyTree("b1")}
	fc := filer.NewFilerConf()
	var out bytes.Buffer

	changed, err := enforceOne(fc, quotaBucketEntry("b1", 2999), &out, filerQuotaSizer(f))
	if err != nil {
		t.Fatalf("size error: %v", err)
	}
	if !changed || !readOnlyAt(fc, "b1") {
		t.Fatalf("3000 remote-only bytes over a 2999 quota: changed=%v readOnly=%v, output %q", changed, readOnlyAt(fc, "b1"), out.String())
	}
	if !strings.Contains(out.String(), "size:3000") {
		t.Errorf("output does not report the filer size: %q", out.String())
	}
}

func TestQuotaEnforceRemoteOnlyBucketUnderQuotaStaysWritable(t *testing.T) {
	f := &quotaTreeFiler{tree: remoteOnlyTree("b1")}
	fc := filer.NewFilerConf()
	var out bytes.Buffer

	size, err := bucketsize.Size(context.Background(), stubFilerClient{client: f}, "/buckets/b1")
	if err != nil || size != 3000 {
		t.Fatalf("walk of the same tree: size=%d err=%v, want 3000", size, err)
	}
	changed, err := enforceOne(fc, quotaBucketEntry("b1", 3001), &out, filerQuotaSizer(f))
	if err != nil {
		t.Fatalf("size error: %v", err)
	}
	if changed || readOnlyAt(fc, "b1") {
		t.Fatalf("3000 bytes under a 3001 quota: changed=%v readOnly=%v", changed, readOnlyAt(fc, "b1"))
	}
}

func TestQuotaEnforceSumsLocalAndRemoteOncePerEntry(t *testing.T) {
	b := "/buckets/mix"
	f := &quotaTreeFiler{tree: map[string][]*filer_pb.Entry{
		b: {quotaDir("ns"), localFile("local-only.parquet", 100, 0)},
		b + "/ns": {
			localFile("cached-and-remote.parquet", 200, 200), // counted once: 200
			remoteOnly("remote-only.parquet", 400),           // 400
		},
	}}

	size, err := bucketsize.Size(context.Background(), stubFilerClient{client: f}, util.FullPath(b))
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if size != 100+200+400 {
		t.Fatalf("size = %d, want %d", size, 100+200+400)
	}
}

// auto sizes a mounted bucket from the filer and any other from its collection
func TestQuotaEnforceAutoSizesOnlyMountedBucketsFromFiler(t *testing.T) {
	tree := remoteOnlyTree("mounted")
	for k, v := range remoteOnlyTree("local") {
		tree[k] = v
	}
	f := &quotaTreeFiler{tree: tree}
	mounts := &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/mounted": {Name: "bunny", Bucket: "mounted"},
	}}
	collection := func(entry *filer_pb.Entry) (float64, error) { return 10, nil }
	sizer := autoBucketSize(mounts, "/buckets", filerQuotaSizer(f), collection)

	if size, err := sizer(quotaBucketEntry("mounted", 1)); err != nil || size != 3000 {
		t.Fatalf("mounted bucket: size=%v err=%v, want the filer's 3000", size, err)
	}
	if size, err := sizer(quotaBucketEntry("local", 1)); err != nil || size != 10 {
		t.Fatalf("unmounted bucket: size=%v err=%v, want the collection's 10", size, err)
	}
	if got := f.listedUnder("/buckets/local"); len(got) != 0 {
		t.Fatalf("unmounted bucket was walked: %v", got)
	}
}

func TestQuotaEnforceListingErrorLeavesStateUnchanged(t *testing.T) {
	listErr := errors.New("filer unavailable")
	for _, wasReadOnly := range []bool{false, true} {
		tree := remoteOnlyTree("b1")
		f := &quotaTreeFiler{tree: tree, errDirs: map[string]error{"/buckets/b1/ns/t/metadata": listErr}}
		fc := filer.NewFilerConf()
		if wasReadOnly {
			if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b1/", ReadOnly: true}); err != nil {
				t.Fatal(err)
			}
		}
		// the partial sum (2500) is under the 2999 quota and the full one over
		// it: an error must flip neither way
		var out bytes.Buffer
		changed, err := enforceOne(fc, quotaBucketEntry("b1", 2999), &out, filerQuotaSizer(f))
		if err == nil {
			t.Fatalf("wasReadOnly=%v: listing error not reported", wasReadOnly)
		}
		if changed || readOnlyAt(fc, "b1") != wasReadOnly {
			t.Fatalf("wasReadOnly=%v: changed=%v readOnly=%v after a listing error", wasReadOnly, changed, readOnlyAt(fc, "b1"))
		}
		if !strings.Contains(out.String(), "size error") {
			t.Errorf("listing error not printed: %q", out.String())
		}
	}

	// positive control: the same tree without the error does flip
	f := &quotaTreeFiler{tree: remoteOnlyTree("b1")}
	fc := filer.NewFilerConf()
	if changed, err := enforceOne(fc, quotaBucketEntry("b1", 2999), &bytes.Buffer{}, filerQuotaSizer(f)); err != nil || !changed {
		t.Fatalf("control: changed=%v err=%v", changed, err)
	}
}

func TestQuotaEnforceBucketWithoutQuotaIsNotWalked(t *testing.T) {
	tree := remoteOnlyTree("noquota")
	for k, v := range remoteOnlyTree("withquota") {
		tree[k] = v
	}
	f := &quotaTreeFiler{tree: tree}
	fc := filer.NewFilerConf()

	if changed, err := enforceOne(fc, quotaBucketEntry("noquota", 0), &bytes.Buffer{}, filerQuotaSizer(f)); changed || err != nil {
		t.Fatalf("no quota: changed=%v err=%v", changed, err)
	}
	if got := f.listedUnder("/buckets/noquota"); len(got) != 0 {
		t.Fatalf("bucket without a quota was walked: %v", got)
	}

	// positive control: a bucket with a quota is walked
	if _, err := enforceOne(fc, quotaBucketEntry("withquota", 1<<30), &bytes.Buffer{}, filerQuotaSizer(f)); err != nil {
		t.Fatal(err)
	}
	if got := f.listedUnder("/buckets/withquota"); len(got) != 5 {
		t.Fatalf("bucket with a quota: listed %v, want its 5 directories", got)
	}
}

// filer.conf is read after the walks: an operator clearing another bucket's
// flag while this one is walked is not overwritten by a stale copy
func TestQuotaEnforceKeepsFilerConfChangesMadeWhileSizing(t *testing.T) {
	stored := filer.NewFilerConf()
	if err := stored.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/other/", ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	readConf := func() (*filer.FilerConf, error) {
		var buf bytes.Buffer
		if err := stored.ToText(&buf); err != nil {
			return nil, err
		}
		fc := filer.NewFilerConf()
		return fc, fc.LoadFromBytes(buf.Bytes())
	}
	sizer := func(entry *filer_pb.Entry) (float64, error) {
		stored.DeleteLocationConf("/buckets/other/") // the operator's change, mid-walk
		return 3000, nil
	}

	buckets := []*filer_pb.Entry{quotaBucketEntry("b1", 2999)}
	fc, changed, _, err := (&commandS3BucketQuotaEnforce{}).enforceQuotas(buckets, "/buckets", sizer, readConf, sameBuckets(buckets), &bytes.Buffer{})
	if err != nil || !changed || !readOnlyAt(fc, "b1") {
		t.Fatalf("b1 over quota: changed=%v readOnly=%v err=%v", changed, fc != nil && readOnlyAt(fc, "b1"), err)
	}
	if readOnlyAt(fc, "other") {
		t.Fatal("the filer.conf change made while sizing was overwritten")
	}
}

// a quota removed (or changed) while the bucket is sized is not applied stale
func TestQuotaEnforceSkipsQuotaChangedWhileSizing(t *testing.T) {
	buckets := []*filer_pb.Entry{quotaBucketEntry("b1", 2999)}
	current := []*filer_pb.Entry{quotaBucketEntry("b1", 2999)}
	sizer := func(entry *filer_pb.Entry) (float64, error) {
		current = []*filer_pb.Entry{quotaBucketEntry("b1", 0)} // the quota is removed mid-walk
		return 3000, nil
	}
	relist := func() ([]*filer_pb.Entry, error) { return current, nil }
	fc, changed, _, err := (&commandS3BucketQuotaEnforce{}).enforceQuotas(buckets, "/buckets", sizer, func() (*filer.FilerConf, error) { return filer.NewFilerConf(), nil }, relist, &bytes.Buffer{})
	if err != nil || changed || readOnlyAt(fc, "b1") {
		t.Fatalf("quota removed while sizing: changed=%v readOnly=%v err=%v", changed, fc != nil && readOnlyAt(fc, "b1"), err)
	}
}
