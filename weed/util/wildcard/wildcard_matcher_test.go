package wildcard

import "testing"

func TestMatchesWildcard(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{"Exact match", "test", "test", true},
		{"Single wildcard", "*", "anything", true},
		{"Empty string with wildcard", "*", "", true},
		{"Prefix wildcard", "test*", "test123", true},
		{"Suffix wildcard", "*test", "123test", true},
		{"Middle wildcard", "test*123", "testABC123", true},
		{"Multiple wildcards", "test*abc*123", "testXYZabcDEF123", true},
		{"No match", "test*", "other", false},
		{"Single question mark", "test?", "test1", true},
		{"Multiple question marks", "test??", "test12", true},
		{"Question mark no match", "test?", "test12", false},
		{"Mixed wildcards", "test*abc?def", "testXYZabc1def", true},
		{"Empty pattern", "", "", true},
		{"Empty pattern with string", "", "test", false},
		{"Pattern with string empty", "test", "", false},
		{"Pattern with regex special chars", "test[abc]", "test[abc]", true},
		{"Pattern with dots", "test.txt", "test.txt", true},
		{"Pattern with dots and wildcard", "*.txt", "test.txt", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchesWildcard(tt.pattern, tt.str)
			if got != tt.expected {
				t.Errorf("MatchesWildcard(%q, %q) = %v, want %v", tt.pattern, tt.str, got, tt.expected)
			}
		})
	}
}

func TestWildcardMatcherMatch(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		inputs   []string
		expected []bool
	}{
		{"Simple star", "test*", []string{"test", "test123", "testing", "other"}, []bool{true, true, true, false}},
		{"Question mark", "test?", []string{"test1", "test2", "test", "test12"}, []bool{true, true, false, false}},
		{"Extension filter", "*.txt", []string{"file.txt", "test.txt", "file.doc", "txt"}, []bool{true, true, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewWildcardMatcher(tt.pattern)
			if err != nil {
				t.Fatalf("NewWildcardMatcher: %v", err)
			}
			for i, s := range tt.inputs {
				got := m.Match(s)
				if got != tt.expected[i] {
					t.Errorf("Match(%q) = %v, want %v", s, got, tt.expected[i])
				}
			}
		})
	}
}

func TestCompileWildcardPattern(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		want    bool
	}{
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Get?bject", "s3:GetObject", true},
		{"s3:*Object*", "s3:GetObjectAcl", true},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			re, err := CompileWildcardPattern(tt.pattern)
			if err != nil {
				t.Fatalf("CompileWildcardPattern: %v", err)
			}
			if got := re.MatchString(tt.input); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFastMatchesWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		want    bool
	}{
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Put*", "s3:GetObject", false},
		{"arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/file.txt", true},
		{"user:admin-*", "user:admin-john", true},
		{"user:admin-*", "user:guest-john", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.input, func(t *testing.T) {
			got := FastMatchesWildcard(tt.pattern, tt.input)
			if got != tt.want {
				t.Errorf("FastMatchesWildcard(%q, %q) = %v, want %v", tt.pattern, tt.input, got, tt.want)
			}
		})
	}
}

func TestWildcardMatcherCaching(t *testing.T) {
	m1, err := GetCachedWildcardMatcher("s3:Get*")
	if err != nil {
		t.Fatal(err)
	}
	m2, err := GetCachedWildcardMatcher("s3:Get*")
	if err != nil {
		t.Fatal(err)
	}
	if m1 != m2 {
		t.Error("expected same cached instance")
	}
	if !m1.Match("s3:GetObject") {
		t.Error("expected match")
	}
}

func TestWildcardMatcherCacheBounding(t *testing.T) {
	wildcardMatcherCache.ClearCache()
	orig := wildcardMatcherCache.maxSize
	wildcardMatcherCache.maxSize = 3
	defer func() {
		wildcardMatcherCache.maxSize = orig
		wildcardMatcherCache.ClearCache()
	}()
	for _, p := range []string{"p1", "p2", "p3"} {
		GetCachedWildcardMatcher(p)
	}
	size, maxSize := wildcardMatcherCache.GetCacheStats()
	if size != 3 {
		t.Errorf("expected size 3, got %d", size)
	}
	if maxSize != 3 {
		t.Errorf("expected maxSize 3, got %d", maxSize)
	}
	GetCachedWildcardMatcher("p4")
	size, _ = wildcardMatcherCache.GetCacheStats()
	if size != 3 {
		t.Errorf("expected size 3 after eviction, got %d", size)
	}
	wildcardMatcherCache.mu.RLock()
	defer wildcardMatcherCache.mu.RUnlock()
	if _, ok := wildcardMatcherCache.matchers["p1"]; ok {
		t.Error("p1 should have been evicted (LRU)")
	}
	if _, ok := wildcardMatcherCache.matchers["p4"]; !ok {
		t.Error("p4 should be in cache")
	}
}

func TestWildcardMatcherCacheLRU(t *testing.T) {
	wildcardMatcherCache.ClearCache()
	orig := wildcardMatcherCache.maxSize
	wildcardMatcherCache.maxSize = 3
	defer func() {
		wildcardMatcherCache.maxSize = orig
		wildcardMatcherCache.ClearCache()
	}()
	for _, p := range []string{"p1", "p2", "p3"} {
		GetCachedWildcardMatcher(p)
	}
	GetCachedWildcardMatcher("p1") // access p1 to make it most-recently used
	GetCachedWildcardMatcher("p4") // should evict p2 (now LRU)
	wildcardMatcherCache.mu.RLock()
	defer wildcardMatcherCache.mu.RUnlock()
	if _, ok := wildcardMatcherCache.matchers["p2"]; ok {
		t.Error("p2 should be evicted (least recently used)")
	}
	if _, ok := wildcardMatcherCache.matchers["p1"]; !ok {
		t.Error("p1 should remain (recently accessed)")
	}
	if _, ok := wildcardMatcherCache.matchers["p3"]; !ok {
		t.Error("p3 should remain")
	}
	if _, ok := wildcardMatcherCache.matchers["p4"]; !ok {
		t.Error("p4 should be in cache")
	}
}

func TestWildcardMatcherCacheClear(t *testing.T) {
	GetCachedWildcardMatcher("test")
	wildcardMatcherCache.ClearCache()
	size, _ := wildcardMatcherCache.GetCacheStats()
	if size != 0 {
		t.Errorf("expected 0 after clear, got %d", size)
	}
}

func BenchmarkMatchesWildcard(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MatchesWildcard("s3:Get*", "s3:GetObject")
	}
}

func BenchmarkFastMatchesWildcard(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FastMatchesWildcard("s3:Get*", "s3:GetObject")
	}
}
