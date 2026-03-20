package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestPeerIndexIgnoresGrpcPort(t *testing.T) {
	self := pb.ServerAddress("127.0.0.1:9000.19000")
	peers := []pb.ServerAddress{
		"127.0.0.1:9000",
		"127.0.0.1:9002.19002",
		"127.0.0.1:9003.19003",
	}

	if idx := peerIndex(self, peers); idx != 0 {
		t.Fatalf("expected peer index 0 for %q among %+v, got %d", self, peers, idx)
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

func TestCheckPeersCanonicalizesSelfEntry(t *testing.T) {
	self, peers := checkPeers("127.0.0.1", 9000, 19000, "127.0.0.1:9000,127.0.0.1:9002,127.0.0.1:9003")

	for _, peer := range peers {
		if peer.ToHttpAddress() == self.ToHttpAddress() && peer != self {
			t.Fatalf("expected self peer to be canonicalized to %q, got %q", self, peer)
		}
	}
}

func TestCheckPeersDeduplicatesAliasPeers(t *testing.T) {
	_, peers := checkPeers("127.0.0.1", 9000, 19000, "127.0.0.1:9002,127.0.0.1:9002.19002,127.0.0.1:9003")

	if len(peers) != 3 {
		t.Fatalf("expected 3 unique peers after normalization, got %d: %+v", len(peers), peers)
	}

	count9002 := 0
	for _, peer := range peers {
		if peer.ToHttpAddress() == "127.0.0.1:9002" {
			count9002++
			if string(peer) != "127.0.0.1:9002.19002" {
				t.Fatalf("expected canonical peer 127.0.0.1:9002.19002, got %q", peer)
			}
		}
	}
	if count9002 != 1 {
		t.Fatalf("expected one peer for 127.0.0.1:9002, got %d in %+v", count9002, peers)
	}
}
