package util

import (
	"testing"
)

func TestPathPrefixFilter_Empty(t *testing.T) {
	pf := NewPathPrefixFilter("", "", nil)

	if pf.HasFilters() {
		t.Error("empty filter should have no filters")
	}

	// Should include everything when no filters
	tests := []string{"/", "/foo", "/foo/bar", "/buckets/test"}
	for _, path := range tests {
		if !pf.ShouldInclude(path) {
			t.Errorf("empty filter should include %q", path)
		}
	}
}

func TestPathPrefixFilter_ExcludeOnly(t *testing.T) {
	pf := NewPathPrefixFilter("", "/buckets/legacy,/buckets/old", nil)

	tests := []struct {
		path    string
		include bool
	}{
		{"/buckets/active", true},
		{"/buckets/active/file.txt", true},
		{"/buckets/legacy", false},
		{"/buckets/legacy/file.txt", false},
		{"/buckets/legacy_new", true}, // boundary check: not a prefix match
		{"/buckets/old", false},
		{"/buckets/old/data", false},
		{"/other", true},
	}

	for _, tc := range tests {
		got := pf.ShouldInclude(tc.path)
		if got != tc.include {
			t.Errorf("ShouldInclude(%q) = %v, want %v", tc.path, got, tc.include)
		}
	}
}

func TestPathPrefixFilter_IncludeOnly(t *testing.T) {
	pf := NewPathPrefixFilter("/buckets/important,/data", "", nil)

	tests := []struct {
		path    string
		include bool
	}{
		{"/buckets/important", true},
		{"/buckets/important/file.txt", true},
		{"/data", true},
		{"/data/file.txt", true},
		{"/buckets/other", false}, // not in include list
		{"/other", false},
	}

	for _, tc := range tests {
		got := pf.ShouldInclude(tc.path)
		if got != tc.include {
			t.Errorf("ShouldInclude(%q) = %v, want %v", tc.path, got, tc.include)
		}
	}
}

func TestPathPrefixFilter_DeeperPrefixWins(t *testing.T) {
	// Exclude /buckets/keep but include /buckets/keep/important
	pf := NewPathPrefixFilter("/buckets/keep/important", "/buckets/keep", nil)

	tests := []struct {
		path    string
		include bool
	}{
		{"/buckets/keep", false},
		{"/buckets/keep/other", false},
		{"/buckets/keep/important", true},          // deeper include wins
		{"/buckets/keep/important/file.txt", true}, // deeper include wins
		{"/buckets/other", false},                  // not matched, include required
	}

	for _, tc := range tests {
		got := pf.ShouldInclude(tc.path)
		if got != tc.include {
			t.Errorf("ShouldInclude(%q) = %v, want %v", tc.path, got, tc.include)
		}
	}
}

func TestPathPrefixFilter_DeeperExcludeWins(t *testing.T) {
	// Include /buckets but exclude /buckets/legacy
	pf := NewPathPrefixFilter("/buckets", "/buckets/legacy", nil)

	tests := []struct {
		path    string
		include bool
	}{
		{"/buckets", true},
		{"/buckets/active", true},
		{"/buckets/legacy", false},          // deeper exclude wins
		{"/buckets/legacy/file.txt", false}, // deeper exclude wins
		{"/other", false},                   // not in include list
	}

	for _, tc := range tests {
		got := pf.ShouldInclude(tc.path)
		if got != tc.include {
			t.Errorf("ShouldInclude(%q) = %v, want %v", tc.path, got, tc.include)
		}
	}
}

func TestPathPrefixFilter_MultipleOverlappingPrefixes(t *testing.T) {
	// Complex scenario with multiple overlapping prefixes
	pf := NewPathPrefixFilter(
		"/a,/a/b/c/d",     // includes
		"/a/b,/a/b/c/d/e", // excludes
		nil,
	)

	tests := []struct {
		path    string
		include bool
	}{
		{"/a", true},            // direct include match
		{"/a/x", true},          // under include /a
		{"/a/b", false},         // deeper exclude /a/b beats /a
		{"/a/b/x", false},       // under exclude /a/b
		{"/a/b/c", false},       // under exclude /a/b
		{"/a/b/c/d", true},      // deeper include /a/b/c/d beats /a/b
		{"/a/b/c/d/x", true},    // under include /a/b/c/d
		{"/a/b/c/d/e", false},   // deeper exclude /a/b/c/d/e beats /a/b/c/d
		{"/a/b/c/d/e/f", false}, // under exclude /a/b/c/d/e
	}

	for _, tc := range tests {
		got := pf.ShouldInclude(tc.path)
		if got != tc.include {
			t.Errorf("ShouldInclude(%q) = %v, want %v", tc.path, got, tc.include)
		}
	}
}

func TestPathPrefixFilter_InvalidPrefixes(t *testing.T) {
	var warnings []string
	warn := func(format string, args ...interface{}) {
		warnings = append(warnings, format)
	}

	pf := NewPathPrefixFilter("invalid,/valid", "also_invalid", warn)

	if len(warnings) != 2 {
		t.Errorf("expected 2 warnings, got %d", len(warnings))
	}

	// Only valid prefix should be stored
	if len(pf.includePrefixes) != 1 {
		t.Errorf("expected 1 include prefix, got %d", len(pf.includePrefixes))
	}
	if len(pf.excludePrefixes) != 0 {
		t.Errorf("expected 0 exclude prefixes, got %d", len(pf.excludePrefixes))
	}
}

func TestPathPrefixFilter_TrailingSlashNormalization(t *testing.T) {
	pf := NewPathPrefixFilter("/path/to/dir", "/exclude/this/", nil)

	// Both should be normalized with trailing slash
	if pf.includePrefixes[0] != "/path/to/dir/" {
		t.Errorf("include prefix not normalized: %q", pf.includePrefixes[0])
	}
	if pf.excludePrefixes[0] != "/exclude/this/" {
		t.Errorf("exclude prefix not normalized: %q", pf.excludePrefixes[0])
	}
}

func TestPathPrefixFilter_BoundaryMatching(t *testing.T) {
	pf := NewPathPrefixFilter("", "/buckets/legacy1", nil)

	tests := []struct {
		path    string
		include bool
	}{
		{"/buckets/legacy1", false},
		{"/buckets/legacy1/file", false},
		{"/buckets/legacy1_backup", true}, // not a prefix match due to boundary
		{"/buckets/legacy10", true},       // not a prefix match due to boundary
		{"/buckets/legacy", true},
	}

	for _, tc := range tests {
		got := pf.ShouldInclude(tc.path)
		if got != tc.include {
			t.Errorf("ShouldInclude(%q) = %v, want %v", tc.path, got, tc.include)
		}
	}
}
