package weed_server

import (
	"context"
	"strconv"
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

// one wraps a single clause into a condition.
func one(c *filer_pb.WriteCondition_Clause) *filer_pb.WriteCondition {
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{c}}
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
		{"empty-absent", &filer_pb.WriteCondition{}, nil, true},
		{"ifnotexists-absent", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}), nil, true},
		{"ifnotexists-present", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}), present, false},
		{"ifexists-absent", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_EXISTS}), nil, false},
		{"ifexists-present", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_EXISTS}), present, true},
		{"etagmatch-hit", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"abc"`}}), present, true},
		{"etagmatch-miss", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"zzz"`}}), present, false},
		{"etagmatch-absent", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"abc"`}}), nil, false},
		{"etagnotmatch-hit", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etags: []string{`"abc"`}}), present, false},
		{"etagnotmatch-miss", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etags: []string{`"zzz"`}}), present, true},
		{"etagnotmatch-absent", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etags: []string{`"abc"`}}), nil, true},
		{"unmodsince-ok", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: base.Unix()}), present, true},
		{"unmodsince-fail", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: base.Unix() - 1}), present, false},
		{"modsince-ok", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_MODIFIED_SINCE, UnixTime: base.Unix() - 1}), present, true},
		{"modsince-fail", one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_MODIFIED_SINCE, UnixTime: base.Unix()}), present, false},
	}
	for _, tc := range cases {
		if got := writeConditionSatisfied(tc.cond, tc.cur); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestWriteConditionClauses(t *testing.T) {
	base := time.Unix(1700000000, 0)
	present := entryWithETag("abc", base)

	matchAny := func(etags ...string) *filer_pb.WriteCondition {
		return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: etags},
		}}
	}
	noneOf := func(etags ...string) *filer_pb.WriteCondition {
		return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etags: etags},
		}}
	}

	cases := []struct {
		name string
		cond *filer_pb.WriteCondition
		cur  *filer.Entry
		want bool
	}{
		{"set-match-hit", matchAny(`"x"`, `"abc"`, `"y"`), present, true},
		{"set-match-miss", matchAny(`"x"`, `"y"`), present, false},
		{"set-none-clean", noneOf(`"x"`, `"y"`), present, true},
		{"set-none-hit", noneOf(`"abc"`, `"y"`), present, false},
		// A weak request ETag never matches under strong comparison.
		{"weak-strong-fails", matchAny(`W/"abc"`), present, false},
		// allow_weak compares ignoring the W/ marker.
		{"weak-allowed", &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`W/"abc"`}, AllowWeak: true},
		}}, present, true},
		// Compound clauses are ANDed.
		{"compound-ok", &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"abc"`}},
			{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: base.Unix()},
		}}, present, true},
		{"compound-second-fails", &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"abc"`}},
			{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: base.Unix() - 1},
		}}, present, false},
	}
	for _, tc := range cases {
		if got := writeConditionSatisfied(tc.cond, tc.cur); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// The generic IF_EXTENDED_* guards express object-lock without S3 knowledge in
// the filer: a legal hold (IF_EXTENDED_NOT_EQUAL) and a retention deadline
// (IF_EXTENDED_TIME_ELAPSED).
func TestWriteConditionObjectLockGuards(t *testing.T) {
	now := time.Now()
	withExt := func(ext map[string]string) *filer.Entry {
		e := &filer.Entry{FullPath: "/test/obj", Attr: filer.Attr{Mtime: now}, Extended: map[string][]byte{}}
		for k, v := range ext {
			e.Extended[k] = []byte(v)
		}
		return e
	}
	legalHold := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
		{Kind: filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL, ExtKey: "lock-hold", ExtValue: "ON"},
	}}
	retention := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
		{Kind: filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED, ExtKey: "retain-until"},
	}}
	// Governance bypass: the retention guard is gated to COMPLIANCE mode, so a
	// governance-mode (or unmoded) entry is deletable while compliance stays
	// protected.
	gatedRetention := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
		{Kind: filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED, ExtKey: "retain-until", GateKey: "lock-mode", GateValue: "COMPLIANCE"},
	}}
	future := strconv.FormatInt(now.Add(time.Hour).Unix(), 10)
	past := strconv.FormatInt(now.Add(-time.Hour).Unix(), 10)

	cases := []struct {
		name string
		cond *filer_pb.WriteCondition
		cur  *filer.Entry
		want bool
	}{
		{"hold-on-blocks", legalHold, withExt(map[string]string{"lock-hold": "ON"}), false},
		{"hold-off-allows", legalHold, withExt(map[string]string{"lock-hold": "OFF"}), true},
		{"hold-absent-allows", legalHold, withExt(nil), true},
		{"hold-on-new-object", legalHold, nil, true}, // nothing to protect yet
		{"retain-future-blocks", retention, withExt(map[string]string{"retain-until": future}), false},
		{"retain-past-allows", retention, withExt(map[string]string{"retain-until": past}), true},
		{"retain-absent-allows", retention, withExt(nil), true},
		{"retain-malformed-blocks", retention, withExt(map[string]string{"retain-until": "soon"}), false},
		// Governance bypass (gated to COMPLIANCE): compliance still blocks, but
		// governance and unmoded entries become deletable despite future retention.
		{"bypass-compliance-blocks", gatedRetention, withExt(map[string]string{"retain-until": future, "lock-mode": "COMPLIANCE"}), false},
		{"bypass-governance-allows", gatedRetention, withExt(map[string]string{"retain-until": future, "lock-mode": "GOVERNANCE"}), true},
		{"bypass-no-mode-allows", gatedRetention, withExt(map[string]string{"retain-until": future}), true},
		// Composed WORM guard: legal hold AND retention, both clear -> allowed.
		{"worm-both-clear", &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL, ExtKey: "lock-hold", ExtValue: "ON"},
			{Kind: filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED, ExtKey: "retain-until"},
		}}, withExt(map[string]string{"lock-hold": "OFF", "retain-until": past}), true},
		// Either guard tripping blocks the whole WORM condition.
		{"worm-hold-trips", &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL, ExtKey: "lock-hold", ExtValue: "ON"},
			{Kind: filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED, ExtKey: "retain-until"},
		}}, withExt(map[string]string{"lock-hold": "ON", "retain-until": past}), false},
	}
	for _, tc := range cases {
		if got := writeConditionSatisfied(tc.cond, tc.cur); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// An unrecognized clause kind (e.g. from a newer client) fails closed, so a
// guard can't be silently bypassed by an older filer. NONE stays a no-op.
func TestWriteConditionUnknownKindFailsClosed(t *testing.T) {
	present := entryWithETag("abc", time.Now())
	unknown := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
		{Kind: filer_pb.WriteCondition_Kind(9999)},
	}}
	if writeConditionSatisfied(unknown, present) {
		t.Error("unknown clause kind must not be satisfied (fail closed) for an existing entry")
	}
	if writeConditionSatisfied(unknown, nil) {
		t.Error("unknown clause kind must not be satisfied (fail closed) for an absent entry")
	}
	none := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
		{Kind: filer_pb.WriteCondition_NONE},
	}}
	if !writeConditionSatisfied(none, present) {
		t.Error("a NONE clause must be satisfied (no-op)")
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
			Condition: one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{etag}}),
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

// CreateEntry reuses a provided existing entry instead of reading the store
// again; passing nil makes it look the path up itself.
func TestCreateEntryReusesProvidedExisting(t *testing.T) {
	existing := &filer.Entry{
		FullPath: "/test/obj",
		Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000000, 0), Crtime: time.Unix(1700000000, 0)},
		Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc")},
	}
	newEntry := func() *filer.Entry {
		return &filer.Entry{
			FullPath: "/test/obj",
			Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000001, 0)},
		}
	}

	// Provided existing vs nil: the only difference is CreateEntry's own lookup,
	// so providing it must save exactly one path read (other store-layer reads
	// during the overwrite are the same in both runs).
	store := newRenameTestStore()
	store.entries["/test/obj"] = existing.ShallowClone()
	f := newRenameTestFiler(store)
	if err := f.CreateEntry(context.Background(), newEntry(), existing, false, false, nil, true, f.MaxFilenameLength); err != nil {
		t.Fatalf("create with existing: %v", err)
	}
	withExisting := store.findEntryCallCount("/test/obj")

	store2 := newRenameTestStore()
	store2.entries["/test/obj"] = existing.ShallowClone()
	f2 := newRenameTestFiler(store2)
	if err := f2.CreateEntry(context.Background(), newEntry(), nil, false, false, nil, true, f2.MaxFilenameLength); err != nil {
		t.Fatalf("create with nil: %v", err)
	}
	withNil := store2.findEntryCallCount("/test/obj")

	if withNil != withExisting+1 {
		t.Fatalf("providing existing should save one path lookup: existing=%d nil=%d", withExisting, withNil)
	}
}
