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
		name     string
		pattern  string
		testStr  string
		expected bool
	}{
		{
			name:     "Star wildcard",
			pattern:  "test*",
			testStr:  "test123",
			expected: true,
		},
		{
			name:     "Question mark wildcard",
			pattern:  "test?",
			testStr:  "test1",
			expected: true,
		},
		{
			name:     "Mixed wildcards",
			pattern:  "test*abc?",
			testStr:  "testXYZabc1",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex, err := CompileWildcardPattern(tt.pattern)
			if err != nil {
				t.Fatalf("Failed to compile pattern: %v", err)
			}

			result := regex.MatchString(tt.testStr)
			if result != tt.expected {
				t.Errorf("Pattern %s against %s: expected %v, got %v", tt.pattern, tt.testStr, tt.expected, result)
			}
		})
	}
}

func BenchmarkMatchesWildcard(b *testing.B) {
	patterns := []string{"test*", "test?", "*.txt", "test*abc*123"}
	strings := []string{"test123", "test1", "file.txt", "testXYZabc456123"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, pattern := range patterns {
			MatchesWildcard(pattern, strings[j])
		}
	}
}

func BenchmarkWildcardMatcher(b *testing.B) {
	patterns := []string{"test*", "test?", "*.txt", "test*abc*123"}
	strings := []string{"test123", "test1", "file.txt", "testXYZabc456123"}

	matchers := make([]*WildcardMatcher, len(patterns))
	for i, pattern := range patterns {
		matcher, _ := NewWildcardMatcher(pattern)
		matchers[i] = matcher
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, matcher := range matchers {
			matcher.Match(strings[j])
		}
	}
}
