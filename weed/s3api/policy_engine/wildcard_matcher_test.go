package policy_engine

import (
	"testing"
)

func TestMatchesWildcard(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		// Basic functionality tests
		{
			name:     "Exact match",
			pattern:  "test",
			str:      "test",
			expected: true,
		},
		{
			name:     "Single wildcard",
			pattern:  "*",
			str:      "anything",
			expected: true,
		},
		{
			name:     "Empty string with wildcard",
			pattern:  "*",
			str:      "",
			expected: true,
		},

		// Star (*) wildcard tests
		{
			name:     "Prefix wildcard",
			pattern:  "test*",
			str:      "test123",
			expected: true,
		},
		{
			name:     "Suffix wildcard",
			pattern:  "*test",
			str:      "123test",
			expected: true,
		},
		{
			name:     "Middle wildcard",
			pattern:  "test*123",
			str:      "testABC123",
			expected: true,
		},
		{
			name:     "Multiple wildcards",
			pattern:  "test*abc*123",
			str:      "testXYZabcDEF123",
			expected: true,
		},
		{
			name:     "No match",
			pattern:  "test*",
			str:      "other",
			expected: false,
		},

		// Question mark (?) wildcard tests
		{
			name:     "Single question mark",
			pattern:  "test?",
			str:      "test1",
			expected: true,
		},
		{
			name:     "Multiple question marks",
			pattern:  "test??",
			str:      "test12",
			expected: true,
		},
		{
			name:     "Question mark no match",
			pattern:  "test?",
			str:      "test12",
			expected: false,
		},
		{
			name:     "Mixed wildcards",
			pattern:  "test*abc?def",
			str:      "testXYZabc1def",
			expected: true,
		},

		// Edge cases
		{
			name:     "Empty pattern",
			pattern:  "",
			str:      "",
			expected: true,
		},
		{
			name:     "Empty pattern with string",
			pattern:  "",
			str:      "test",
			expected: false,
		},
		{
			name:     "Pattern with string empty",
			pattern:  "test",
			str:      "",
			expected: false,
		},

		// Special characters
		{
			name:     "Pattern with regex special chars",
			pattern:  "test[abc]",
			str:      "test[abc]",
			expected: true,
		},
		{
			name:     "Pattern with dots",
			pattern:  "test.txt",
			str:      "test.txt",
			expected: true,
		},
		{
			name:     "Pattern with dots and wildcard",
			pattern:  "*.txt",
			str:      "test.txt",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesWildcard(tt.pattern, tt.str)
			if result != tt.expected {
				t.Errorf("Pattern %s against %s: expected %v, got %v", tt.pattern, tt.str, tt.expected, result)
			}
		})
	}
}

func TestWildcardMatcher(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		strings  []string
		expected []bool
	}{
		{
			name:     "Simple star pattern",
			pattern:  "test*",
			strings:  []string{"test", "test123", "testing", "other"},
			expected: []bool{true, true, true, false},
		},
		{
			name:     "Question mark pattern",
			pattern:  "test?",
			strings:  []string{"test1", "test2", "test", "test12"},
			expected: []bool{true, true, false, false},
		},
		{
			name:     "Mixed pattern",
			pattern:  "*.txt",
			strings:  []string{"file.txt", "test.txt", "file.doc", "txt"},
			expected: []bool{true, true, false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := NewWildcardMatcher(tt.pattern)
			if err != nil {
				t.Fatalf("Failed to create matcher: %v", err)
			}

			for i, str := range tt.strings {
				result := matcher.Match(str)
				if result != tt.expected[i] {
					t.Errorf("Pattern %s against %s: expected %v, got %v", tt.pattern, str, tt.expected[i], result)
				}
			}
		})
	}
}

func TestCompileWildcardPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		input   string
		want    bool
	}{
		{"Star wildcard", "s3:Get*", "s3:GetObject", true},
		{"Question mark wildcard", "s3:Get?bject", "s3:GetObject", true},
		{"Mixed wildcards", "s3:*Object*", "s3:GetObjectAcl", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex, err := CompileWildcardPattern(tt.pattern)
			if err != nil {
				t.Errorf("CompileWildcardPattern() error = %v", err)
				return
			}
			got := regex.MatchString(tt.input)
			if got != tt.want {
				t.Errorf("CompileWildcardPattern() = %v, want %v", got, tt.want)
			}
		})
	}
}

// BenchmarkWildcardMatchingPerformance demonstrates the performance benefits of caching
func BenchmarkWildcardMatchingPerformance(b *testing.B) {
	patterns := []string{
		"s3:Get*",
		"s3:Put*",
		"s3:Delete*",
		"s3:List*",
		"arn:aws:s3:::bucket/*",
		"arn:aws:s3:::bucket/prefix*",
		"user:*",
		"user:admin-*",
	}

	inputs := []string{
		"s3:GetObject",
		"s3:PutObject",
		"s3:DeleteObject",
		"s3:ListBucket",
		"arn:aws:s3:::bucket/file.txt",
		"arn:aws:s3:::bucket/prefix/file.txt",
		"user:admin",
		"user:admin-john",
	}

	b.Run("WithoutCache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, pattern := range patterns {
				for _, input := range inputs {
					MatchesWildcard(pattern, input)
				}
			}
		}
	})

	b.Run("WithCache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, pattern := range patterns {
				for _, input := range inputs {
					FastMatchesWildcard(pattern, input)
				}
			}
		}
	})
}

// BenchmarkWildcardMatcherReuse demonstrates the performance benefits of reusing WildcardMatcher instances
func BenchmarkWildcardMatcherReuse(b *testing.B) {
	pattern := "s3:Get*"
	input := "s3:GetObject"

	b.Run("NewMatcherEveryTime", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			matcher, _ := NewWildcardMatcher(pattern)
			matcher.Match(input)
		}
	})

	b.Run("CachedMatcher", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			matcher, _ := GetCachedWildcardMatcher(pattern)
			matcher.Match(input)
		}
	})
}

// TestWildcardMatcherCaching verifies that caching works correctly
func TestWildcardMatcherCaching(t *testing.T) {
	pattern := "s3:Get*"

	// Get the first matcher
	matcher1, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		t.Fatalf("Failed to get cached matcher: %v", err)
	}

	// Get the second matcher - should be the same instance
	matcher2, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		t.Fatalf("Failed to get cached matcher: %v", err)
	}

	// Check that they're the same instance (same pointer)
	if matcher1 != matcher2 {
		t.Errorf("Expected same matcher instance, got different instances")
	}

	// Test that both matchers work correctly
	testInput := "s3:GetObject"
	if !matcher1.Match(testInput) {
		t.Errorf("First matcher failed to match %s", testInput)
	}
	if !matcher2.Match(testInput) {
		t.Errorf("Second matcher failed to match %s", testInput)
	}
}

// TestFastMatchesWildcard verifies that the fast matching function works correctly
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
