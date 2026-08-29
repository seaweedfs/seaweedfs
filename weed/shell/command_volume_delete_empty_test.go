package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

func TestIsEmptyVolumeDeleteCandidateCollectionPattern(t *testing.T) {
	now := int64(1000)
	quietSeconds := int64(100)
	quietEmptyVolume := &master_pb.VolumeInformationMessage{
		Size:             0,
		ModifiedAtSecond: now - quietSeconds - 1,
		Collection:       "important-logs",
	}

	tests := []struct {
		name       string
		pattern    string
		collection string
		want       bool
	}{
		{name: "empty pattern matches named collection", pattern: "", collection: "important-logs", want: true},
		{name: "wildcard matches collection", pattern: "important*", collection: "important-logs", want: true},
		{name: "wildcard rejects collection", pattern: "important*", collection: "other-logs", want: false},
		{name: "default pattern matches empty collection", pattern: CollectionDefault, collection: "", want: true},
		{name: "default pattern rejects named collection", pattern: CollectionDefault, collection: "important-logs", want: false},
		{name: "list matches a listed collection", pattern: "other-logs,important-logs", collection: "important-logs", want: true},
		{name: "list rejects an unlisted collection", pattern: "other-logs,important-logs", collection: "audit-logs", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := *quietEmptyVolume
			v.Collection = tt.collection
			matcher, err := wildcard.CompileCollectionMatcher(tt.pattern)
			if err != nil {
				t.Fatalf("CompileCollectionMatcher(%q): %v", tt.pattern, err)
			}
			if got := isEmptyVolumeDeleteCandidate(&v, quietSeconds, now, matcher); got != tt.want {
				t.Fatalf("isEmptyVolumeDeleteCandidate(collection=%q, pattern=%q) = %v, want %v",
					tt.collection, tt.pattern, got, tt.want)
			}
		})
	}
}
