package weed_server

import (
	"sort"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestRaftServerID(t *testing.T) {
	tests := []struct {
		name string
		addr pb.ServerAddress
		want string
	}{
		{
			name: "without grpc suffix",
			addr: pb.ServerAddress("master-0:9333"),
			want: "master-0:9333",
		},
		{
			name: "with grpc suffix",
			addr: pb.NewServerAddress("master-0", 9333, 19333),
			want: "master-0:9333",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := raftServerID(tt.addr); got != tt.want {
				t.Fatalf("raftServerID(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestGetPeerIdxUsesCanonicalID(t *testing.T) {
	peers := map[string]pb.ServerAddress{
		"master-0:9333": pb.ServerAddress("master-0:9333"),
		"master-1:9333": pb.ServerAddress("master-1:9333"),
		"master-2:9333": pb.ServerAddress("master-2:9333"),
	}
	self := pb.NewServerAddress("master-2", 9333, 19333)

	if got := getPeerIdx(self, peers); got != 2 {
		t.Fatalf("getPeerIdx(%q) = %d, want 2", self, got)
	}
}

func TestAddPeersConfigurationUsesCanonicalIDs(t *testing.T) {
	rs := &RaftServer{
		peers: map[string]pb.ServerAddress{
			"master-0:9333": pb.ServerAddress("master-0:9333"),
			"master-1:9333": pb.ServerAddress("master-1:9333"),
			"master-2:9333": pb.ServerAddress("master-2:9333"),
		},
	}

	cfg := rs.AddPeersConfiguration()
	if len(cfg.Servers) != 3 {
		t.Fatalf("len(cfg.Servers) = %d, want 3", len(cfg.Servers))
	}

	var ids []string
	var addrs []string
	for _, s := range cfg.Servers {
		if s.Suffrage != raft.Voter {
			t.Fatalf("server %q has suffrage %q, want %q", s.ID, s.Suffrage, raft.Voter)
		}
		ids = append(ids, string(s.ID))
		addrs = append(addrs, string(s.Address))
	}
	sort.Strings(ids)
	sort.Strings(addrs)

	wantIDs := []string{"master-0:9333", "master-1:9333", "master-2:9333"}
	wantAddrs := []string{"master-0:19333", "master-1:19333", "master-2:19333"}
	for i := range wantIDs {
		if ids[i] != wantIDs[i] {
			t.Fatalf("ids[%d] = %q, want %q", i, ids[i], wantIDs[i])
		}
		if addrs[i] != wantAddrs[i] {
			t.Fatalf("addrs[%d] = %q, want %q", i, addrs[i], wantAddrs[i])
		}
	}
}
