package shell

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func TestPlanRouteTTL_AddsSimpleRule(t *testing.T) {
	fc := filer.NewFilerConf()
	rules := []*s3lifecycle.Rule{
		{ID: "expire-logs", Status: s3lifecycle.StatusEnabled, Prefix: "logs/", ExpirationDays: 7},
	}
	added, skipped, changed := planRouteTTL(fc, "bk", "/buckets", rules)
	if !changed {
		t.Fatalf("expected changed=true, got false")
	}
	if len(added) != 1 || !strings.Contains(added[0], "logs/") || !strings.Contains(added[0], "ttl=7d") {
		t.Fatalf("added=%v, want a single 'logs/ ttl=7d' line", added)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped=%v, want none", skipped)
	}
	conf, ok := fc.GetLocationConf("/buckets/bk/logs/")
	if !ok {
		t.Fatalf("PathConf not added for /buckets/bk/logs/")
	}
	if conf.Ttl != "7d" || conf.Collection != "bk" {
		t.Fatalf("PathConf=%+v, want Ttl=7d Collection=bk", conf)
	}
}

func TestPlanRouteTTL_SkipsTagSizeFilters(t *testing.T) {
	// Tag and size filters mean "TTL doesn't apply uniformly under the
	// prefix" — must stay on the worker's per-object evaluation path.
	fc := filer.NewFilerConf()
	rules := []*s3lifecycle.Rule{
		{ID: "tag", Status: s3lifecycle.StatusEnabled, Prefix: "a/", ExpirationDays: 7,
			FilterTags: map[string]string{"k": "v"}},
		{ID: "size-min", Status: s3lifecycle.StatusEnabled, Prefix: "b/", ExpirationDays: 7,
			FilterSizeGreaterThan: 1024},
		{ID: "size-max", Status: s3lifecycle.StatusEnabled, Prefix: "c/", ExpirationDays: 7,
			FilterSizeLessThan: 1024},
	}
	_, skipped, changed := planRouteTTL(fc, "bk", "/buckets", rules)
	if changed {
		t.Fatalf("changed=true, want false: tag/size filters must not route")
	}
	if len(skipped) != 3 {
		t.Fatalf("expected 3 skipped lines, got %v", skipped)
	}
	for _, line := range skipped {
		if !strings.Contains(line, "tag / size filter") {
			t.Errorf("unexpected skip reason: %q", line)
		}
	}
}

func TestPlanRouteTTL_SkipsDisabledAndNoExpirationDays(t *testing.T) {
	fc := filer.NewFilerConf()
	rules := []*s3lifecycle.Rule{
		{ID: "off", Status: s3lifecycle.StatusDisabled, Prefix: "a/", ExpirationDays: 7},
		{ID: "noncurrent-only", Status: s3lifecycle.StatusEnabled, Prefix: "b/",
			NoncurrentVersionExpirationDays: 7},
		{ID: "abort-mpu-only", Status: s3lifecycle.StatusEnabled, Prefix: "c/",
			AbortMPUDaysAfterInitiation: 7},
	}
	_, skipped, changed := planRouteTTL(fc, "bk", "/buckets", rules)
	if changed {
		t.Fatalf("changed=true, want false: no rule has Expiration.Days on the simple path")
	}
	if len(skipped) != 3 {
		t.Fatalf("expected 3 skipped lines, got %v", skipped)
	}
}

func TestPlanRouteTTL_DedupesAlreadyRouted(t *testing.T) {
	// Re-running on a bucket whose routing already matches must be a no-op
	// — operators run this idempotently after lifecycle XML changes.
	fc := filer.NewFilerConf()
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bk/logs/",
		Collection:     "bk",
		Ttl:            "7d",
	}); err != nil {
		t.Fatalf("seed AddLocationConf: %v", err)
	}
	rules := []*s3lifecycle.Rule{
		{ID: "expire-logs", Status: s3lifecycle.StatusEnabled, Prefix: "logs/", ExpirationDays: 7},
	}
	added, skipped, changed := planRouteTTL(fc, "bk", "/buckets", rules)
	if changed {
		t.Fatalf("changed=true; idempotent re-run must be a no-op")
	}
	if len(added) != 0 {
		t.Fatalf("added=%v, want none", added)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0], "already routed") {
		t.Fatalf("skipped=%v, want 'already routed' line", skipped)
	}
}

func TestPlanRouteTTL_DifferentTTLOverwrites(t *testing.T) {
	// Days changed in the lifecycle XML: re-run must overwrite the stale
	// PathConf so the new TTL gets picked up.
	fc := filer.NewFilerConf()
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bk/logs/",
		Collection:     "bk",
		Ttl:            "30d",
	}); err != nil {
		t.Fatalf("seed AddLocationConf: %v", err)
	}
	rules := []*s3lifecycle.Rule{
		{ID: "expire-logs", Status: s3lifecycle.StatusEnabled, Prefix: "logs/", ExpirationDays: 7},
	}
	added, _, changed := planRouteTTL(fc, "bk", "/buckets", rules)
	if !changed {
		t.Fatalf("changed=false; TTL drift must trigger an update")
	}
	if len(added) != 1 {
		t.Fatalf("added=%v, want 1 line", added)
	}
	if got, _ := fc.GetLocationConf("/buckets/bk/logs/"); got.Ttl != "7d" {
		t.Fatalf("Ttl=%q, want 7d", got.Ttl)
	}
}
