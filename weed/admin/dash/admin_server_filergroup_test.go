package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
)

func TestListClusterNodesRequestIncludesFilerGroup(t *testing.T) {
	server := &AdminServer{filerGroup: "tenant-a"}

	req := server.listClusterNodesRequest(cluster.FilerType)

	if req.ClientType != cluster.FilerType {
		t.Fatalf("ClientType = %q, want %q", req.ClientType, cluster.FilerType)
	}
	if req.FilerGroup != "tenant-a" {
		t.Fatalf("FilerGroup = %q, want %q", req.FilerGroup, "tenant-a")
	}
}

func TestListClusterNodesRequestKeepsDefaultFilerGroupEmpty(t *testing.T) {
	server := &AdminServer{}

	req := server.listClusterNodesRequest(cluster.FilerType)

	if req.FilerGroup != "" {
		t.Fatalf("FilerGroup = %q, want empty", req.FilerGroup)
	}
}

func TestVectorDiscoveryUsesRegistrationGroup(t *testing.T) {
	s := &AdminServer{filerGroup: "tenant-a"}
	request := s.listClusterNodesRequest(cluster.VectorType)
	if request.FilerGroup != s.filerGroup {
		t.Fatalf("vector registration group must match the filer: %+v", request)
	}
}
