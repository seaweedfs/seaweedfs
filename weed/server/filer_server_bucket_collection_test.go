package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestBucketDefaultCollection(t *testing.T) {
	cases := []struct {
		name       string
		filerGroup string
		path       string
		want       string
	}{
		{
			name:       "filer group prefixes bucket",
			filerGroup: "group1",
			path:       "/buckets/bucket1/path/to/object",
			want:       "group1_bucket1",
		},
		{
			name: "no filer group keeps bucket name",
			path: "/buckets/bucket1/path/to/object",
			want: "bucket1",
		},
		{
			name:       "bucket root still resolves",
			filerGroup: "group1",
			path:       "/buckets/bucket1",
			want:       "group1_bucket1",
		},
		{
			name:       "unrelated path has no bucket collection",
			filerGroup: "group1",
			path:       "/other/path",
			want:       "",
		},
		{
			name:       "buckets folder itself has no bucket",
			filerGroup: "group1",
			path:       "/buckets/",
			want:       "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &FilerServer{
				option: &FilerOption{FilerGroup: tc.filerGroup},
				filer:  &filer.Filer{DirBucketsPath: "/buckets"},
			}
			if got := fs.bucketDefaultCollection(tc.path); got != tc.want {
				t.Fatalf("bucketDefaultCollection(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestDetectStorageOptionUsesFilerGroupBucketCollection(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{FilerGroup: "group1", DefaultReplication: "001"},
		filer: &filer.Filer{
			DirBucketsPath: "/buckets",
			FilerConf:      filer.NewFilerConf(),
		},
	}

	so, err := fs.detectStorageOption(context.Background(), "/buckets/bucket1/key", "", "", 0, "", "", "", "")
	if err != nil {
		t.Fatalf("detectStorageOption: %v", err)
	}
	if so.Collection != "group1_bucket1" {
		t.Fatalf("collection = %q, want %q", so.Collection, "group1_bucket1")
	}
}

func newBucketRuleFiler(t *testing.T) *filer.Filer {
	t.Helper()
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bucket1",
		Collection:     "archive",
	}); err != nil {
		t.Fatalf("SetLocationConf: %v", err)
	}
	return &filer.Filer{DirBucketsPath: "/buckets", FilerConf: fc}
}

// Direct filer writes keep the path rule. S3 copy passes the grouped bucket
// collection explicitly, so it overrides the rule the same way PutObject does.
func TestDetectStorageOptionPreservesRuleForDirectWrites(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{FilerGroup: "group1", DefaultReplication: "001"},
		filer:  newBucketRuleFiler(t),
	}

	so, err := fs.detectStorageOption(context.Background(), "/buckets/bucket1/key", "", "", 0, "", "", "", "")
	if err != nil {
		t.Fatalf("detectStorageOption: %v", err)
	}
	if so.Collection != "archive" {
		t.Fatalf("collection = %q, want %q", so.Collection, "archive")
	}
}

// An explicit request collection wins over both the grouped bucket default and
// the path rule; S3 copy and PutObject rely on this precedence.
func TestDetectStorageOptionExplicitCollectionWins(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{FilerGroup: "group1", DefaultReplication: "001"},
		filer:  newBucketRuleFiler(t),
	}

	so, err := fs.detectStorageOption(context.Background(), "/buckets/bucket1/key", "explicit", "", 0, "", "", "", "")
	if err != nil {
		t.Fatalf("detectStorageOption: %v", err)
	}
	if so.Collection != "explicit" {
		t.Fatalf("collection = %q, want %q", so.Collection, "explicit")
	}
}

// Without a filer group the path rule still names the collection, as before.
func TestDetectStorageOptionRuleWinsWithoutFilerGroup(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{DefaultReplication: "001"},
		filer:  newBucketRuleFiler(t),
	}

	so, err := fs.detectStorageOption(context.Background(), "/buckets/bucket1/key", "", "", 0, "", "", "", "")
	if err != nil {
		t.Fatalf("detectStorageOption: %v", err)
	}
	if so.Collection != "archive" {
		t.Fatalf("collection = %q, want %q", so.Collection, "archive")
	}
}
