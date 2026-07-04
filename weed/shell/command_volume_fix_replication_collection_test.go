package shell

import "testing"

func TestMatchCollectionPattern(t *testing.T) {
	tests := []struct {
		name       string
		pattern    string
		collection string
		expected   bool
	}{
		{name: "empty pattern matches any collection", pattern: "", collection: "jfs-hdfs-test", expected: true},
		{name: "empty pattern matches empty collection", pattern: "", collection: "", expected: true},
		{name: "default pattern matches empty collection", pattern: CollectionDefault, collection: "", expected: true},
		{name: "default pattern rejects named collection", pattern: CollectionDefault, collection: "jfs-hdfs-test", expected: false},
		{name: "exact match", pattern: "smart-highlevel-test", collection: "smart-highlevel-test", expected: true},
		{name: "exact mismatch", pattern: "smart-highlevel-test", collection: "jfs-hdfs-test", expected: false},
		{name: "prefix wildcard match", pattern: "smart*", collection: "smart-highlevel-test", expected: true},
		{name: "prefix wildcard mismatch", pattern: "smart*", collection: "jfs-hdfs-test", expected: false},
		{name: "single char wildcard match", pattern: "vol?", collection: "vol1", expected: true},
		{name: "single char wildcard mismatch", pattern: "vol?", collection: "vol42", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern := tt.pattern
			c := &commandVolumeFixReplication{collectionPattern: &pattern}
			if got := c.matchCollectionPattern(tt.collection); got != tt.expected {
				t.Errorf("matchCollectionPattern(pattern=%q, collection=%q) = %v, want %v",
					tt.pattern, tt.collection, got, tt.expected)
			}
		})
	}
}
