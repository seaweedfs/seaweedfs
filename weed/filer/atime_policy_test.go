package filer

import (
	"testing"
	"time"
)

func TestAtimePolicy_Off_NeverUpdates(t *testing.T) {
	policy := &AtimePolicy{Mode: AtimeModeOff}
	now := time.Now()
	existing := Attr{Atime: now.Add(-48 * time.Hour), Mtime: now.Add(-48 * time.Hour), Ctime: now.Add(-48 * time.Hour)}
	if policy.ShouldUpdate(existing, now) {
		t.Fatal("off mode must not update")
	}
}

func TestAtimePolicy_Strict_UpdatesWhenCandidateAdvances(t *testing.T) {
	policy := &AtimePolicy{Mode: AtimeModeStrict}
	existing := Attr{
		Atime: time.Unix(1_700_000_000, 0),
		Mtime: time.Unix(1_700_000_000, 0),
		Ctime: time.Unix(1_700_000_000, 0),
	}
	if !policy.ShouldUpdate(existing, time.Unix(1_700_000_001, 0)) {
		t.Fatal("strict must update when candidate is newer")
	}
	if policy.ShouldUpdate(existing, existing.Atime) {
		t.Fatal("strict must not update when candidate equals existing atime")
	}
}

func TestAtimePolicy_Relatime_AtimeOlderThanMtime(t *testing.T) {
	policy := &AtimePolicy{Mode: AtimeModeRelatime, RelatimeThreshold: 24 * time.Hour}
	existing := Attr{
		Atime: time.Unix(1_700_000_000, 0),
		Mtime: time.Unix(1_700_500_000, 0),
		Ctime: time.Unix(1_700_500_000, 0),
	}
	if !policy.ShouldUpdate(existing, time.Unix(1_700_500_001, 0)) {
		t.Fatal("relatime must update when atime < mtime")
	}
}

func TestAtimePolicy_Relatime_ThresholdExpired(t *testing.T) {
	policy := &AtimePolicy{Mode: AtimeModeRelatime, RelatimeThreshold: time.Hour}
	atime := time.Unix(1_700_000_000, 0)
	existing := Attr{Atime: atime, Mtime: atime, Ctime: atime}
	candidate := atime.Add(2 * time.Hour)
	if !policy.ShouldUpdate(existing, candidate) {
		t.Fatal("relatime must update once threshold elapses")
	}
}

func TestAtimePolicy_Relatime_DebouncesFreshReads(t *testing.T) {
	policy := &AtimePolicy{Mode: AtimeModeRelatime, RelatimeThreshold: 24 * time.Hour}
	atime := time.Unix(1_700_000_000, 0)
	existing := Attr{Atime: atime, Mtime: atime, Ctime: atime}
	candidate := atime.Add(time.Minute)
	if policy.ShouldUpdate(existing, candidate) {
		t.Fatal("relatime must not update within threshold when atime >= mtime/ctime")
	}
}

func TestParseAtimeMode(t *testing.T) {
	cases := map[string]AtimeMode{
		"":          AtimeModeOff,
		"relatime":  AtimeModeRelatime,
		"RelAtime":  AtimeModeRelatime,
		"off":       AtimeModeOff,
		"  strict ": AtimeModeStrict,
	}
	for input, expected := range cases {
		got, err := ParseAtimeMode(input)
		if err != nil {
			t.Fatalf("ParseAtimeMode(%q): unexpected error: %v", input, err)
		}
		if got != expected {
			t.Fatalf("ParseAtimeMode(%q): expected %q, got %q", input, expected, got)
		}
	}

	if _, err := ParseAtimeMode("nope"); err == nil {
		t.Fatal("expected ParseAtimeMode to reject unknown mode")
	}
}

func TestNewAtimePolicy_DefaultsToOff(t *testing.T) {
	policy, err := NewAtimePolicy("", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy.Mode != AtimeModeOff {
		t.Fatalf("expected default policy off, got %q", policy.Mode)
	}
	if policy.AppliesToPath("/anything") {
		t.Fatal("off policy must never apply")
	}
}

func TestParsePathPrefixes_TrimsAndDropsEmpty(t *testing.T) {
	got := ParsePathPrefixes("  /buckets/a , ,/buckets/b ,  ")
	want := []string{"/buckets/a", "/buckets/b"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i, p := range want {
		if got[i] != p {
			t.Fatalf("prefix %d: expected %q, got %q", i, p, got[i])
		}
	}
	if ParsePathPrefixes("") != nil {
		t.Fatal("empty input must yield nil")
	}
	if ParsePathPrefixes(" , , ") != nil {
		t.Fatal("whitespace-only input must yield nil")
	}
}

func TestAtimePolicy_AppliesToPath(t *testing.T) {
	cases := []struct {
		name     string
		policy   *AtimePolicy
		path     string
		expected bool
	}{
		{"nil policy never applies", nil, "/anywhere", false},
		{"off mode never applies", &AtimePolicy{Mode: AtimeModeOff}, "/anywhere", false},
		{"no prefixes applies globally", &AtimePolicy{Mode: AtimeModeStrict}, "/anywhere", true},
		{
			"matching prefix applies",
			&AtimePolicy{Mode: AtimeModeRelatime, PathPrefixes: []string{"/buckets/cache"}},
			"/buckets/cache/object",
			true,
		},
		{
			"non-matching prefix is skipped",
			&AtimePolicy{Mode: AtimeModeRelatime, PathPrefixes: []string{"/buckets/cache"}},
			"/buckets/other/object",
			false,
		},
		{
			"prefix match respects component boundary",
			&AtimePolicy{Mode: AtimeModeRelatime, PathPrefixes: []string{"/buckets/cache"}},
			"/buckets/cache_backup/object",
			false,
		},
		{
			"exact path equals prefix",
			&AtimePolicy{Mode: AtimeModeRelatime, PathPrefixes: []string{"/buckets/cache"}},
			"/buckets/cache",
			true,
		},
		{
			"trailing slash prefix matches sub-path",
			&AtimePolicy{Mode: AtimeModeRelatime, PathPrefixes: []string{"/buckets/cache/"}},
			"/buckets/cache/object",
			true,
		},
		{
			"empty prefix bypassing constructor matches everything",
			&AtimePolicy{Mode: AtimeModeRelatime, PathPrefixes: []string{""}},
			"/anywhere/at/all",
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.AppliesToPath(tc.path); got != tc.expected {
				t.Fatalf("AppliesToPath(%q): expected %v, got %v", tc.path, tc.expected, got)
			}
		})
	}
}
