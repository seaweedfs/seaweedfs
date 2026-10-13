package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

func TestDebugWildcardPairs(t *testing.T) {
	pairs := [][2]string{
		{"arn:aws:iam:::*", "arn:aws:iam::000000000000:user/team-parent"},
		{"arn:aws:iam::*", "arn:aws:iam::000000000000:user/team-parent"},
		{"a:*", "a:b/c"},
		{"a::*", "a::b"},
		{"arn:aws:iam:::*", "arn:aws:iam:::user/team-parent"},
		{"*", "anything"},
	}
	for _, p := range pairs {
		t.Logf("pattern=%q value=%q => %v", p[0], p[1], wildcard.MatchesWildcard(p[0], p[1]))
	}
}
