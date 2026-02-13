package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestRaftServerAddressComparison(t *testing.T) {
	// Scenario: Master node configured as "localhost:9333" but peers see it as "127.0.0.1:9333"
	// or potentially different port formattings.

	tests := []struct {
		name  string
		addr1 string
		addr2 string
		same  bool
	}{
		{
			name:  "Exact match",
			addr1: "localhost:9333",
			addr2: "localhost:9333",
			same:  true,
		},
		{
			name:  "Host mismatch",
			addr1: "localhost:9333",
			addr2: "127.0.0.1:9333",
			same:  false, // Still false as we don't resolve DNS in Equals yet, but IsLeader uses ToHttpAddress which handles ports
		},
		{
			name:  "GRPC port mismatch style - explicit vs implicit",
			addr1: "localhost:9333",
			addr2: "localhost:9333.19333",
			same:  true, // Should be true with Equals()
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a1 := pb.ServerAddress(tt.addr1)
			a2 := pb.ServerAddress(tt.addr2)

			if got := a1.Equals(a2); got != tt.same {
				t.Errorf("Equals %s == %s got %v, want %v", a1, a2, got, tt.same)
			}
		})
	}
}

func TestRaftTopologyLeaderLogic(t *testing.T) {
	myAddress := pb.ServerAddress("master01:9333")
	leaderAddress := pb.ServerAddress("master01:9333.19333")

	if !myAddress.Equals(leaderAddress) {
		t.Errorf("Mismatch: '%s' != '%s' even with Equals()", myAddress, leaderAddress)
	} else {
		t.Logf("Success: '%s' equals '%s' with Equals()", myAddress, leaderAddress)
	}
}
