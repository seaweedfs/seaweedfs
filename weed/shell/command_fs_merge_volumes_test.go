package shell

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestFilterAmbiguousDuplicateVolumeIds(t *testing.T) {
	duplicates := map[needle.VolumeId][]string{
		needle.VolumeId(10): {"alpha", "beta"},
		needle.VolumeId(20): {"beta", "gamma"},
	}

	tests := []struct {
		name       string
		collection string
		from       needle.VolumeId
		to         needle.VolumeId
		expected   map[needle.VolumeId][]string
	}{
		{
			name:       "all collections include all duplicates",
			collection: "*",
			expected: map[needle.VolumeId][]string{
				needle.VolumeId(10): {"alpha", "beta"},
				needle.VolumeId(20): {"beta", "gamma"},
			},
		},
		{
			name:       "collection scope keeps matching duplicates",
			collection: "alpha",
			expected: map[needle.VolumeId][]string{
				needle.VolumeId(10): {"alpha", "beta"},
			},
		},
		{
			name:       "from volume narrows duplicate set",
			collection: "*",
			from:       needle.VolumeId(20),
			expected: map[needle.VolumeId][]string{
				needle.VolumeId(20): {"beta", "gamma"},
			},
		},
		{
			name:       "explicit pair checks both ends",
			collection: "*",
			from:       needle.VolumeId(10),
			to:         needle.VolumeId(20),
			expected: map[needle.VolumeId][]string{
				needle.VolumeId(10): {"alpha", "beta"},
				needle.VolumeId(20): {"beta", "gamma"},
			},
		},
		{
			name:       "collection plus explicit volume ignores unrelated duplicate",
			collection: "alpha",
			to:         needle.VolumeId(20),
			expected:   map[needle.VolumeId][]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := filterAmbiguousDuplicateVolumeIds(duplicates, tc.collection, tc.from, tc.to)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("unexpected duplicates: got=%v want=%v", got, tc.expected)
			}
		})
	}
}
