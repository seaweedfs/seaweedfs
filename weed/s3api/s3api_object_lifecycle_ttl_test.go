package s3api

import (
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func enabledRule(prefix string, days int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{
		ID:             "r-" + prefix,
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         prefix,
		ExpirationDays: days,
	}
}

func mustResolver(t *testing.T, rules ...*s3lifecycle.Rule) *LifecycleTTLResolver {
	t.Helper()
	return NewLifecycleTTLResolver(rules, false)
}

func TestNewLifecycleTTLResolver_NilOnEmpty(t *testing.T) {
	if got := NewLifecycleTTLResolver(nil, false); got != nil {
		t.Fatalf("nil rules → resolver=%v, want nil", got)
	}
	if got := NewLifecycleTTLResolver([]*s3lifecycle.Rule{}, false); got != nil {
		t.Fatalf("empty rules → resolver=%v, want nil", got)
	}
}

func TestNewLifecycleTTLResolver_NilOnVersionedBucket(t *testing.T) {
	// Versioned buckets cannot use volume TTL — TTL volumes destroy
	// noncurrent versions as a unit. Resolver collapses to nil so the
	// PUT path's nil-receiver Resolve returns 0 without checking flags.
	rules := []*s3lifecycle.Rule{enabledRule("logs/", 7)}
	if got := NewLifecycleTTLResolver(rules, true); got != nil {
		t.Fatalf("versioned bucket → resolver=%v, want nil", got)
	}
}

func TestNewLifecycleTTLResolver_DropsTagFilteredRules(t *testing.T) {
	// HIGH-priority finding: tag-filtered rules are unsafe on the fast
	// path. Tags can be replaced via PutObjectTagging after the write,
	// but the volume TTL is irreversible. Worker handles tag-filtered
	// rules at scan time; the fast path drops them entirely.
	tagRule := enabledRule("logs/", 7)
	tagRule.FilterTags = map[string]string{"k": "v"}
	plainRule := enabledRule("data/", 30)

	r := mustResolver(t, tagRule, plainRule)
	if r == nil {
		t.Fatalf("plain rule should still produce a resolver")
	}
	// Tag-filtered key would have matched but is now invisible.
	if got := r.Resolve("logs/foo", 1); got != 0 {
		t.Fatalf("tag-filtered rule must not appear on fast path, got %d", got)
	}
	if got := r.Resolve("data/foo", 1); got != 30*86400 {
		t.Fatalf("plain rule still applies, got %d", got)
	}
}

func TestNewLifecycleTTLResolver_DropsDisabledAndNonExpirationDays(t *testing.T) {
	disabled := enabledRule("logs/", 7)
	disabled.Status = s3lifecycle.StatusDisabled
	noExp := &s3lifecycle.Rule{
		ID: "r", Status: s3lifecycle.StatusEnabled, Prefix: "logs/",
		NoncurrentVersionExpirationDays: 7,
	}
	if got := NewLifecycleTTLResolver([]*s3lifecycle.Rule{disabled, noExp}, false); got != nil {
		t.Fatalf("only-ineligible rules → resolver=%v, want nil", got)
	}
}

func TestResolve_PrefixMatch(t *testing.T) {
	r := mustResolver(t, enabledRule("logs/", 7))
	if got := r.Resolve("logs/foo.txt", 1); got != 7*86400 {
		t.Fatalf("want 7d in seconds, got %d", got)
	}
	if got := r.Resolve("data/foo.txt", 1); got != 0 {
		t.Fatalf("non-matching prefix should yield 0, got %d", got)
	}
}

func TestResolve_OverlappingRulesShorterExpirationWins(t *testing.T) {
	// MEDIUM-priority finding: AWS overlapping-rule precedence is
	// "shorter expiration wins". Sort ascending by ExpirationDays so
	// the first prefix match is also the shortest applicable rule.
	r := mustResolver(t,
		enabledRule("logs/", 30),          // broad, long
		enabledRule("logs/critical/", 90), // specific, longer
		enabledRule("logs/", 7),           // broad, short
	)
	// "logs/foo" matches both broad rules; the shorter (7d) wins.
	if got := r.Resolve("logs/foo", 1); got != 7*86400 {
		t.Fatalf("shorter expiration must win, got %d (want 7d)", got)
	}
	// "logs/critical/x" matches all three; 7d still wins (shorter than
	// the more specific 90d). Longest-prefix is NOT the AWS rule.
	if got := r.Resolve("logs/critical/x", 1); got != 7*86400 {
		t.Fatalf("shorter expiration must win across overlaps, got %d (want 7d)", got)
	}
}

func TestResolve_OverflowDefersToWorker(t *testing.T) {
	// MEDIUM-priority finding: capping at math.MaxInt32 seconds (~68y)
	// would expire LONGER policies early. Return 0 instead so the
	// worker enforces the actual policy on its own schedule.
	bigDays := int(math.MaxInt32/secondsPerDay) + 1
	r := mustResolver(t, enabledRule("anything", bigDays))
	if got := r.Resolve("anything-x", 1); got != 0 {
		t.Fatalf("overflow must yield 0 (worker handles), got %d", got)
	}
}

func TestResolve_OverflowSkipsButShorterStillFires(t *testing.T) {
	// Pathological case: short and overflowing rule on overlapping
	// prefix. Ascending sort puts the short one first; the overflow
	// rule never gets a chance to mis-cap.
	bigDays := int(math.MaxInt32/secondsPerDay) + 1
	r := mustResolver(t,
		enabledRule("anything", bigDays),
		enabledRule("anything", 7),
	)
	if got := r.Resolve("anything-x", 1); got != 7*86400 {
		t.Fatalf("shorter rule must still fire on overlap, got %d", got)
	}
}

func TestResolve_SizeFilter(t *testing.T) {
	rule := enabledRule("logs/", 7)
	rule.FilterSizeGreaterThan = 1024
	r := mustResolver(t, rule)

	// Below threshold → skip.
	if got := r.Resolve("logs/foo", 100); got != 0 {
		t.Fatalf("size <= threshold must skip, got %d", got)
	}
	// Above threshold → apply.
	if got := r.Resolve("logs/foo", 2048); got != 7*86400 {
		t.Fatalf("size > threshold must apply, got %d", got)
	}
	// Unknown size + size filter → skip (conservative).
	if got := r.Resolve("logs/foo", -1); got != 0 {
		t.Fatalf("unknown size with filter must skip, got %d", got)
	}
}

func TestResolve_NilReceiverReturnsZero(t *testing.T) {
	// nil-receiver-safe Resolve avoids the call site needing to check
	// whether the bucket has a resolver at all.
	var r *LifecycleTTLResolver
	if got := r.Resolve("logs/foo", 1); got != 0 {
		t.Fatalf("nil resolver must return 0, got %d", got)
	}
}

func BenchmarkLifecycleTTLResolver_Resolve_NilReceiver(b *testing.B) {
	// Common case: bucket has no lifecycle config → resolver is nil.
	var r *LifecycleTTLResolver
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve("logs/foo.txt", 4096)
	}
}

func BenchmarkLifecycleTTLResolver_Resolve_OneRule(b *testing.B) {
	// Typical case: one Expiration.Days rule that the key matches.
	r := NewLifecycleTTLResolver([]*s3lifecycle.Rule{
		enabledRule("logs/", 7),
	}, false)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve("logs/foo.txt", 4096)
	}
}

func BenchmarkLifecycleTTLResolver_Resolve_FiveRulesNoMatch(b *testing.B) {
	// Worst typical case: walks all rules and none match.
	r := NewLifecycleTTLResolver([]*s3lifecycle.Rule{
		enabledRule("a/", 1),
		enabledRule("b/", 7),
		enabledRule("c/", 30),
		enabledRule("d/", 90),
		enabledRule("e/", 365),
	}, false)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve("z/foo.txt", 4096)
	}
}

func TestNewBucketConfigFromEntry_RefreshesLifecycleTTL(t *testing.T) {
	// Regression: storeBucketLifecycleConfiguration only updates
	// Entry.Extended; if the cache-refresh path doesn't re-derive
	// LifecycleTTL, an Add → Update → Delete dance would leave a stale
	// resolver applying the wrong volume TTL to subsequent writes. Walk
	// the three transitions and assert the resolver follows.
	s := &S3ApiServer{}

	xmlAdd := []byte(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`)
	xmlReplace := []byte(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`)

	ext := map[string][]byte{
		s3_constants.ExtLifecycleTtlFastPathKey: []byte("true"),
	}
	entry := &filer_pb.Entry{Extended: ext}

	// 1) No XML yet → no resolver.
	cfg := s.newBucketConfigFromEntry("bk", entry)
	if cfg.LifecycleTTL != nil {
		t.Fatalf("no XML must yield nil resolver, got %v", cfg.LifecycleTTL)
	}

	// 2) Add: 7d.
	ext[bucketLifecycleConfigurationXMLKey] = xmlAdd
	cfg = s.newBucketConfigFromEntry("bk", entry)
	if got := cfg.LifecycleTTL.Resolve("logs/foo", 1); got != 7*86400 {
		t.Fatalf("after add, want 7d, got %d", got)
	}

	// 3) Replace: 30d. The previous resolver must NOT linger.
	ext[bucketLifecycleConfigurationXMLKey] = xmlReplace
	cfg = s.newBucketConfigFromEntry("bk", entry)
	if got := cfg.LifecycleTTL.Resolve("logs/foo", 1); got != 30*86400 {
		t.Fatalf("after replace, want 30d, got %d (stale resolver?)", got)
	}

	// 4) Delete: nil resolver. The most dangerous regression — leaving
	//    the old resolver here would keep stamping irreversible volume
	//    TTL onto writes after the policy was removed.
	delete(ext, bucketLifecycleConfigurationXMLKey)
	cfg = s.newBucketConfigFromEntry("bk", entry)
	if cfg.LifecycleTTL != nil {
		t.Fatalf("after delete, want nil resolver, got %v", cfg.LifecycleTTL)
	}
}

func TestNewBucketConfigFromEntry_ObjectLockTreatedAsVersioned(t *testing.T) {
	// Object Lock requires versioning, so a bucket with ObjectLock but
	// no explicit Versioning header is still effectively versioned —
	// volume TTL would expire all noncurrent versions as a unit. The
	// resolver-construction site must mirror BucketIsVersioned and
	// treat ObjectLockConfig != nil as versioned.
	s := &S3ApiServer{}
	xml := []byte(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`)
	cfg := s.newBucketConfigFromEntry("bk", &filer_pb.Entry{Extended: map[string][]byte{
		s3_constants.ExtObjectLockEnabledKey:    []byte(s3_constants.ObjectLockEnabled),
		s3_constants.ExtLifecycleTtlFastPathKey: []byte("true"),
		bucketLifecycleConfigurationXMLKey:      xml,
	}})
	if cfg.ObjectLockConfig == nil {
		t.Fatal("test setup: ObjectLockConfig should be parsed")
	}
	if cfg.LifecycleTTL != nil {
		t.Fatalf("ObjectLock buckets must skip the fast-path resolver, got %v", cfg.LifecycleTTL)
	}
}

func TestNewBucketConfigFromEntry_TtlFastPathOptIn(t *testing.T) {
	// The fast path is opt-in per bucket: lifecycle XML alone must not
	// stamp volume TTL on writes. Only the explicit flag builds the
	// resolver.
	s := &S3ApiServer{}
	xml := []byte(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`)

	// XML present but flag off → nil resolver (worker drives expiration).
	ext := map[string][]byte{
		bucketLifecycleConfigurationXMLKey: xml,
	}
	entry := &filer_pb.Entry{Extended: ext}
	cfg := s.newBucketConfigFromEntry("bk", entry)
	if cfg.LifecycleTTL != nil {
		t.Fatalf("fast path off must yield nil resolver, got %v", cfg.LifecycleTTL)
	}

	// Flag on → resolver applies the rule.
	ext[s3_constants.ExtLifecycleTtlFastPathKey] = []byte("true")
	cfg = s.newBucketConfigFromEntry("bk", entry)
	if got := cfg.LifecycleTTL.Resolve("logs/foo", 1); got != 7*86400 {
		t.Fatalf("fast path on, want 7d, got %d", got)
	}
}
