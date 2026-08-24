package command

import "testing"

// The offset key is a persistence contract: a changed form orphans saved
// checkpoints, so the historical forms must survive verbatim and the
// target-scoped form must stay stable.
func TestGetSignaturePrefixByPath_KeyForms(t *testing.T) {
	cases := []struct {
		sourcePath, targetPath, want string
	}{
		// historical forms, kept so existing deployments resume unchanged
		{"/", "/", "sync."},
		{"/data", "/", "sync./data"},
		// the target path participates once it deviates from "/"
		{"/", "/backup-a", "sync.=>/backup-a"},
		{"/data", "/backup-a", "sync./data=>/backup-a"},
	}
	for _, tc := range cases {
		if got := getSignaturePrefixByPath(tc.sourcePath, tc.targetPath); got != tc.want {
			t.Errorf("getSignaturePrefixByPath(%q, %q) = %q, want %q", tc.sourcePath, tc.targetPath, got, tc.want)
		}
	}
}

// Two syncs from the same source filer and path to different directories on
// the same target filer must keep separate checkpoints, or the running one
// pushes the shared offset past events the other never processed.
func TestGetSignaturePrefixByPath_DistinctTargets(t *testing.T) {
	a := getSignaturePrefixByPath("/", "/backup-a")
	b := getSignaturePrefixByPath("/", "/backup-b")
	if a == b {
		t.Errorf("different target paths share offset key %q", a)
	}
	if root := getSignaturePrefixByPath("/", "/"); a == root {
		t.Errorf("non-root target path shares offset key %q with the root form", a)
	}
}

// Over the real KV wire path: a sync that predates target-scoped keys resumes
// from the historical key, and once each sync checkpoints under its own key
// they no longer disturb each other.
func TestSyncOffset_HistoricalFallbackThenIndependence(t *testing.T) {
	filerAddr, dial := startKvFiler(t)
	const sourceSig = int32(12345)
	historical := getSignaturePrefixByPath("/data", "/")
	prefixA := getSignaturePrefixByPath("/data", "/backup-a")
	prefixB := getSignaturePrefixByPath("/data", "/backup-b")

	// pre-upgrade state: both syncs shared the historical key
	if err := setOffset(dial, filerAddr, historical, sourceSig, 111); err != nil {
		t.Fatalf("seed historical offset: %v", err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, prefixA, sourceSig, historical, sourceSig); err != nil || got != 111 {
		t.Fatalf("A fallback read = (%d, %v), want (111, nil)", got, err)
	}

	// A checkpoints under its own key; B still resumes from the historical one
	if err := setOffset(dial, filerAddr, prefixA, sourceSig, 222); err != nil {
		t.Fatalf("write A offset: %v", err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, prefixA, sourceSig, historical, sourceSig); err != nil || got != 222 {
		t.Fatalf("A read = (%d, %v), want (222, nil)", got, err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, prefixB, sourceSig, historical, sourceSig); err != nil || got != 111 {
		t.Fatalf("B read = (%d, %v), want (111, nil)", got, err)
	}
}
