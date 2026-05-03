package command

import (
	"reflect"
	"testing"
)

func TestParseBucketList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"single", "one", []string{"one"}},
		{"multi", "one,two,three", []string{"one", "two", "three"}},
		{"trims whitespace", " one , two , three ", []string{"one", "two", "three"}},
		{"drops empty entries", "one,,two,", []string{"one", "two"}},
		{"dedupes preserving order", "one,two,one,three,two", []string{"one", "two", "three"}},
		{"only commas", ",,,", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBucketList(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseBucketList(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
