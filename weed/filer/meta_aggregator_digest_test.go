package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCheckPeerDigest(t *testing.T) {
	defer util.SetDigest("")
	cases := []struct {
		own, peer string
		shared    bool
		refuse    bool
	}{
		{util.DigestMd5, util.DigestMd5, false, false},
		{util.DigestSha256, util.DigestSha256, false, false},
		{util.DigestMd5, "", false, false},
		{util.DigestMd5, util.DigestSha256, false, true},
		{util.DigestSha256, util.DigestMd5, false, true},
		{util.DigestMd5, util.DigestSha256, true, false},
	}
	for _, c := range cases {
		if err := util.SetDigest(c.own); err != nil {
			t.Fatal(err)
		}
		err := checkPeerDigest("peer:8888", c.peer, c.shared)
		if (err != nil) != c.refuse {
			t.Errorf("own %s, peer %q, shared store %v: checkPeerDigest = %v, want refused %v", c.own, c.peer, c.shared, err, c.refuse)
		}
	}
}
