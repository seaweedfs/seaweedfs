package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func entryWithETag(etag string, mtime time.Time) *filer.Entry {
	return &filer.Entry{
		FullPath: "/test/obj",
		Attr:     filer.Attr{Mtime: mtime},
		Extended: map[string][]byte{s3_constants.ExtETagKey: []byte(etag)},
	}
}

func TestWriteConditionSatisfied(t *testing.T) {
	base := time.Unix(1700000000, 0)
	present := entryWithETag("abc", base)

	cases := []struct {
		name string
		cond *filer_pb.WriteCondition
		cur  *filer.Entry
		want bool
	}{
		{"none-absent", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_NONE}, nil, true},
		{"ifnotexists-absent", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}, nil, true},
		{"ifnotexists-present", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}, present, false},
		{"ifexists-absent", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_EXISTS}, nil, false},
		{"ifexists-present", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_EXISTS}, present, true},
		{"etagmatch-hit", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etag: `"abc"`}, present, true},
		{"etagmatch-miss", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etag: `"zzz"`}, present, false},
		{"etagmatch-absent", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etag: `"abc"`}, nil, false},
		{"etagnotmatch-hit", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etag: `"abc"`}, present, false},
		{"etagnotmatch-miss", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etag: `"zzz"`}, present, true},
		{"etagnotmatch-absent", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etag: `"abc"`}, nil, true},
		{"unmodsince-ok", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: base.Unix()}, present, true},
		{"unmodsince-fail", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: base.Unix() - 1}, present, false},
		{"modsince-ok", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_MODIFIED_SINCE, UnixTime: base.Unix() - 1}, present, true},
		{"modsince-fail", &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_MODIFIED_SINCE, UnixTime: base.Unix()}, present, false},
	}
	for _, tc := range cases {
		if got := writeConditionSatisfied(tc.cond, tc.cur); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// storedEntryETag prefers the stored Seaweed ETag attribute and falls back to
// the Md5-derived ETag, matching the S3 gateway.
func TestStoredEntryETag(t *testing.T) {
	withExt := entryWithETag("explicit", time.Unix(0, 0))
	if got := storedEntryETag(withExt); got != "explicit" {
		t.Errorf("extended etag: got %q", got)
	}
	md5Only := &filer.Entry{Attr: filer.Attr{Md5: []byte{0xab, 0xcd}}}
	if got := storedEntryETag(md5Only); got != "abcd" {
		t.Errorf("md5 fallback: got %q", got)
	}
}

// The CreateEntry handler enforces the precondition atomically: a matching
// If-Match overwrites, a non-matching one returns PRECONDITION_FAILED.
func TestCreateEntryConditionEnforced(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/test/obj"] = &filer.Entry{
		FullPath: "/test/obj",
		Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000000, 0)},
		Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc")},
	}
	f := newRenameTestFiler(store)
	f.DirBucketsPath = "/buckets"
	fs := &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}

	req := func(etag string) *filer_pb.CreateEntryRequest {
		return &filer_pb.CreateEntryRequest{
			Directory:                "/test",
			SkipCheckParentDirectory: true,
			Entry: &filer_pb.Entry{
				Name:       "obj",
				Attributes: &filer_pb.FuseAttributes{Mtime: 1700000001, FileMode: 0644, Inode: 2},
			},
			Condition: &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etag: etag},
		}
	}

	resp, err := fs.CreateEntry(context.Background(), req(`"zzz"`))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.ErrorCode != filer_pb.FilerError_PRECONDITION_FAILED {
		t.Fatalf("mismatched etag: want PRECONDITION_FAILED, got %v (%q)", resp.ErrorCode, resp.Error)
	}

	resp, err = fs.CreateEntry(context.Background(), req(`"abc"`))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("matching etag should overwrite, got error %q", resp.Error)
	}
}
