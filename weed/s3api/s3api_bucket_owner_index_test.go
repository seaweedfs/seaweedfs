package s3api

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestEscapeOwnerName(t *testing.T) {
	tests := []struct {
		owner string
		want  string
	}{
		{"alice", "alice"},
		{"arn:aws:iam::123:user/bob", "arn:aws:iam::123:user%2Fbob"},
		{".", "%2E"},
		{"..", "%2E."},
		{".complete", "%2Ecomplete"},
		{"../../../etc", "%2E.%2F..%2F..%2Fetc"},
		{"%2E", "%252E"},
	}
	seen := make(map[string]string)
	for _, tt := range tests {
		got := escapeOwnerName(tt.owner)
		if got != tt.want {
			t.Errorf("escapeOwnerName(%q) = %q, want %q", tt.owner, got, tt.want)
		}
		// No result may resolve to a different directory level or collide
		// with index-internal dot-names.
		if strings.ContainsRune(got, '/') || strings.HasPrefix(got, ".") {
			t.Errorf("escapeOwnerName(%q) = %q is not a safe single path segment", tt.owner, got)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("escapeOwnerName collision: %q and %q both map to %q", prev, tt.owner, got)
		}
		seen[got] = tt.owner
	}
}

func ownedPages(pages ...[]string) bucketPageLister {
	// Serves a static sorted stream of owned bucket names, honoring startFrom.
	var all []string
	for _, p := range pages {
		all = append(all, p...)
	}
	return func(startFrom string) ([]*filer_pb.Entry, bool, error) {
		var out []*filer_pb.Entry
		for _, name := range all {
			if name <= startFrom {
				continue
			}
			out = append(out, &filer_pb.Entry{
				Name:       name,
				Attributes: &filer_pb.FuseAttributes{Crtime: 42},
			})
			if len(out) == 2 { // small pages to exercise paging
				return out, false, nil
			}
		}
		return out, true, nil
	}
}

func names(buckets []ListAllMyBucketsEntry) []string {
	var out []string
	for _, b := range buckets {
		out = append(out, b.Name)
	}
	return out
}

func TestListMergedBuckets(t *testing.T) {
	granted := func(n ...string) (out []ListAllMyBucketsEntry) {
		for _, name := range n {
			out = append(out, ListAllMyBucketsEntry{Name: name})
		}
		return out
	}

	t.Run("interleave and dedup", func(t *testing.T) {
		buckets, token, err := listMergedBuckets(ownedPages([]string{"a", "c", "e"}), granted("b", "c", "f"), "", 10)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"a", "b", "c", "e", "f"}
		if strings.Join(names(buckets), ",") != strings.Join(want, ",") {
			t.Errorf("got %v, want %v", names(buckets), want)
		}
		if token != "" {
			t.Errorf("unexpected token %q", token)
		}
	})

	t.Run("truncation and resume", func(t *testing.T) {
		buckets, token, err := listMergedBuckets(ownedPages([]string{"a", "c", "e"}), granted("b", "f"), "", 3)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(names(buckets), ",") != "a,b,c" {
			t.Errorf("page 1 = %v", names(buckets))
		}
		startAfter, err := decodeContinuationToken(token)
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		if startAfter != "c" {
			t.Errorf("token resumes after %q, want c", startAfter)
		}
		// granted entries for page 2 are pre-filtered by startAfter upstream
		buckets, token, err = listMergedBuckets(ownedPages([]string{"a", "c", "e"}), granted("f"), startAfter, 3)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(names(buckets), ",") != "e,f" {
			t.Errorf("page 2 = %v", names(buckets))
		}
		if token != "" {
			t.Errorf("unexpected token %q on final page", token)
		}
	})

	t.Run("no owned stream", func(t *testing.T) {
		buckets, token, err := listMergedBuckets(nil, granted("a", "b"), "", 1)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(names(buckets), ",") != "a" || token == "" {
			t.Errorf("got %v token %q", names(buckets), token)
		}
	})

	t.Run("owned only", func(t *testing.T) {
		buckets, token, err := listMergedBuckets(ownedPages([]string{"a", "b", "c", "d", "e"}), nil, "b", 2)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(names(buckets), ",") != "c,d" {
			t.Errorf("got %v", names(buckets))
		}
		if startAfter, _ := decodeContinuationToken(token); startAfter != "d" {
			t.Errorf("token resumes after %q, want d", startAfter)
		}
	})
}

func TestBucketEntryOwner(t *testing.T) {
	if owner := bucketEntryOwner(nil); owner != "" {
		t.Errorf("nil entry owner = %q", owner)
	}
	if owner := bucketEntryOwner(&filer_pb.Entry{Name: "b"}); owner != "" {
		t.Errorf("ownerless entry owner = %q", owner)
	}
	entry := &filer_pb.Entry{
		Name:     "b",
		Extended: map[string][]byte{s3_constants.AmzIdentityId: []byte("alice")},
	}
	if owner := bucketEntryOwner(entry); owner != "alice" {
		t.Errorf("owner = %q, want alice", owner)
	}
}
