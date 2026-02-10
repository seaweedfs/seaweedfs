package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestIsTheFirstOneIgnoresGrpcPort(t *testing.T) {
	self := pb.ServerAddress("127.0.0.1:9000.19000")
	peers := []pb.ServerAddress{
		"127.0.0.1:9000",
		"127.0.0.1:9002.19002",
		"127.0.0.1:9003.19003",
	}

	if !isTheFirstOne(self, peers) {
		t.Fatalf("expected first peer match by HTTP address between %q and %+v", self, peers)
	}
}

func TestCheckPeersAddsSelfWhenGrpcPortMismatches(t *testing.T) {
	self, peers := checkPeers("127.0.0.1", 9000, 19000, "127.0.0.1:9002,127.0.0.1:9003")

	found := false
	for _, peer := range peers {
		if peer.ToHttpAddress() == self.ToHttpAddress() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected peers %+v to contain self %s by HTTP address", peers, self)
	}
}
