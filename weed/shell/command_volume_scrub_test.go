package shell

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestVolumeServerAddressesUseRegisteredGrpcPort(t *testing.T) {
	dataNodes := []*master_pb.DataNodeInfo{
		{Id: "10.0.0.1:8080", Address: "10.0.0.1:8080", GrpcPort: 18080},
		{Id: "10.0.0.2:7000", Address: "10.0.0.2:7000", GrpcPort: 7500},
		{Id: "10.0.0.3:8081"},
	}
	tests := []struct {
		nodes    string
		wantGrpc []string
	}{
		{"", []string{"10.0.0.1:18080", "10.0.0.2:7500", "10.0.0.3:18081"}},
		{"10.0.0.2:7000", []string{"10.0.0.2:7500"}},
		{" 10.0.0.2:7000 ,,10.0.0.1:8080", []string{"10.0.0.2:7500", "10.0.0.1:18080"}},
		{"10.0.0.9:7000.7600", []string{"10.0.0.9:7600"}},
	}
	for _, tt := range tests {
		var gotGrpc []string
		for _, addr := range volumeServerAddresses(dataNodes, tt.nodes) {
			gotGrpc = append(gotGrpc, addr.ToGrpcAddress())
		}
		if !reflect.DeepEqual(gotGrpc, tt.wantGrpc) {
			t.Errorf("volumeServerAddresses(%q) dials %v, want %v", tt.nodes, gotGrpc, tt.wantGrpc)
		}
	}
}
