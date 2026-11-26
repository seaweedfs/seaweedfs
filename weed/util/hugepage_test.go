package util

import "testing"

func TestParseTHPMode(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "always active", content: "[always] madvise never", want: "always"},
		{name: "madvise active", content: "always [madvise] never", want: "madvise"},
		{name: "never active", content: "always madvise [never]", want: "never"},
		{name: "trailing newline", content: "always [madvise] never\n", want: "madvise"},
		{name: "defer plus madvise active", content: "always defer [defer+madvise] madvise never", want: "defer+madvise"},
		{name: "no bracket", content: "always madvise never", want: ""},
		{name: "empty", content: "", want: ""},
		{name: "partial bracket", content: "[madvise never", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseTHPMode(tc.content); got != tc.want {
				t.Errorf("parseTHPMode(%q) = %q, want %q", tc.content, got, tc.want)
			}
		})
	}
}

func TestParseTHPValue(t *testing.T) {
	if got := parseTHPValue("511\n"); got != "511" {
		t.Errorf("parseTHPValue trimmed trailing newline: got %q, want %q", got, "511")
	}
	if got := parseTHPValue(""); got != "" {
		t.Errorf("parseTHPValue of empty content: got %q, want empty", got)
	}
}

func TestGoHeapHugePagesDisabled(t *testing.T) {
	tests := []struct {
		godebug string
		want    bool
	}{
		{godebug: "", want: false},
		{godebug: "disablethp=1", want: true},
		{godebug: "disablethp=0", want: false},
		{godebug: "disablethp=2", want: true},
		{godebug: "gctrace=1,disablethp=1", want: true},
		{godebug: "disablethp=1,disablethp=0", want: false},
		{godebug: "disablethp=0,disablethp=1", want: true},
		{godebug: "disablethp=1,disablethp=on", want: true},
		{godebug: "gctrace=1, disablethp=1", want: false},
	}
	for _, tc := range tests {
		if got := goHeapHugePagesDisabled(tc.godebug); got != tc.want {
			t.Errorf("goHeapHugePagesDisabled(%q) = %t, want %t", tc.godebug, got, tc.want)
		}
	}
}

func TestAnonHugePagesAlways(t *testing.T) {
	tests := []struct {
		name      string
		enabled   string
		sizeModes []string
		want      bool
	}{
		{name: "no per-size policies, always", enabled: "always", want: true},
		{name: "no per-size policies, madvise", enabled: "madvise", want: false},
		{name: "default sizes, always", enabled: "always", sizeModes: []string{"never", "inherit", "never"}, want: true},
		{name: "default sizes, madvise", enabled: "madvise", sizeModes: []string{"never", "inherit", "never"}, want: false},
		{name: "small size always under madvise", enabled: "madvise", sizeModes: []string{"always", "inherit"}, want: true},
		{name: "small size always under never", enabled: "never", sizeModes: []string{"always", "never"}, want: true},
		{name: "pmd size never under always", enabled: "always", sizeModes: []string{"never", "never"}, want: false},
		{name: "per-size madvise under always", enabled: "always", sizeModes: []string{"madvise", "never"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := anonHugePagesAlways(tc.enabled, tc.sizeModes); got != tc.want {
				t.Errorf("anonHugePagesAlways(%q, %q) = %t, want %t", tc.enabled, tc.sizeModes, got, tc.want)
			}
		})
	}
}
