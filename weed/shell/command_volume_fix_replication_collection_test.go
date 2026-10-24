package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

func TestFixReplicationCollectionPattern(t *testing.T) {
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
		{name: "list matches a listed collection", pattern: "smart-highlevel-test,jfs-hdfs-test", collection: "jfs-hdfs-test", expected: true},
		{name: "list rejects an unlisted collection", pattern: "smart-highlevel-test,jfs-hdfs-test", collection: "other", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := wildcard.CompileCollectionMatcher(tt.pattern)
			if err != nil {
				t.Fatalf("CompileCollectionMatcher(%q): %v", tt.pattern, err)
			}
			c := &commandVolumeFixReplication{collectionMatcher: matcher}
			if got := c.collectionMatcher.Matches(tt.collection); got != tt.expected {
				t.Errorf("collection pattern %q against collection %q = %v, want %v",
					tt.pattern, tt.collection, got, tt.expected)
			}
		})
	}
}
