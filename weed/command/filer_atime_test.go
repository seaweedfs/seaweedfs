package command

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func TestFilerOptions_BuildAtimePolicy_NilFlagsDefaultToOff(t *testing.T) {
	fo := &FilerOptions{}
	policy, err := fo.buildAtimePolicy()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy.Mode != filer.AtimeModeOff {
		t.Fatalf("expected default policy off, got %q", policy.Mode)
	}
	if policy.AppliesToPath("/anywhere") {
		t.Fatal("default policy must never apply")
	}
}

func TestFilerOptions_BuildAtimePolicy_HonoursConfiguredFlags(t *testing.T) {
	mode := string(filer.AtimeModeRelatime)
	threshold := 30 * time.Minute
	paths := "/buckets/a,/buckets/b"
	fo := &FilerOptions{
		atimeMode:              &mode,
		atimeRelatimeThreshold: &threshold,
		atimePaths:             &paths,
	}
	policy, err := fo.buildAtimePolicy()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy.Mode != filer.AtimeModeRelatime {
		t.Fatalf("expected relatime, got %q", policy.Mode)
	}
	if policy.RelatimeThreshold != threshold {
		t.Fatalf("expected threshold %v, got %v", threshold, policy.RelatimeThreshold)
	}
	if !policy.AppliesToPath("/buckets/a/object") {
		t.Fatal("expected configured prefix to apply")
	}
	if policy.AppliesToPath("/buckets/other/object") {
		t.Fatal("expected non-matching prefix to be skipped")
	}
}
