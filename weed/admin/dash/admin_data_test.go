package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestRaftMasterHttpAddress(t *testing.T) {
	tests := []struct {
		name, id, address, want string
	}{
		{"goraft names members by full address", "127.101.0.1:11111.11112", "127.101.0.1:11112", "127.101.0.1:11111"},
		{"hashicorp names members by http address", "127.102.0.1:2222", "127.102.0.1:3333", "127.102.0.1:2222"},
		{"default ports", "10.0.0.1:9333", "10.0.0.1:19333", "10.0.0.1:9333"},
		{"ipv6", "[::1]:2222.3333", "[::1]:3333", "[::1]:2222"},
		{"id that is not an address", "master-a", "10.0.0.2:19333", "10.0.0.2:9333"},
		{"id without a valid port", "10.0.0.3:0", "10.0.0.3:19334", "10.0.0.3:9334"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := raftMasterHttpAddress(&master_pb.RaftListClusterServersResponse_ClusterServers{Id: tt.id, Address: tt.address})
			if got != tt.want {
				t.Errorf("raftMasterHttpAddress(%q, %q) = %q, want %q", tt.id, tt.address, got, tt.want)
			}
		})
	}
}
