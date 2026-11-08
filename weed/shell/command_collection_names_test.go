package shell

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
)

type offlineCollectionMaster struct {
	fakeDiscoveryServer
	topologyCalls atomic.Int32
}

func (s *offlineCollectionMaster) CollectionList(context.Context, *master_pb.CollectionListRequest) (*master_pb.CollectionListResponse, error) {
	return &master_pb.CollectionListResponse{Collections: []*master_pb.Collection{{Name: "live"}, {Name: "unrelated-offline"}, {Name: "prospective"}}}, nil
}
func (s *offlineCollectionMaster) VolumeList(_ context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	s.topologyCalls.Add(1)
	volumes := []*master_pb.VolumeInformationMessage{{Id: 1, Collection: "live", Size: 100, FileCount: 3}}
	if req.TopologyOnly {
		volumes = nil
	}
	return &master_pb.VolumeListResponse{TopologyInfo: &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		RackInfos: []*master_pb.RackInfo{{DataNodeInfos: []*master_pb.DataNodeInfo{{
			DiskInfos: map[string]*master_pb.DiskInfo{"": {VolumeInfos: volumes}},
		}}}},
	}}}}, nil
}
func TestCollectionListIncludesOfflineNamespace(t *testing.T) {
	lis, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	srv := &offlineCollectionMaster{}
	master_pb.RegisterSeaweedServer(server, srv)
	go server.Serve(lis)
	defer server.Stop()
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	srv.leader = "127.0.0.1:0." + port
	ce, cancel := discoveryEnv(srv.leader, 3*time.Second)
	defer cancel()
	var out bytes.Buffer
	if e := (&commandCollectionList{}).Do(nil, ce, &out); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"unrelated-offline", "prospective"} {
		if !strings.Contains(out.String(), "collection:\""+name+"\"\tvolumeCount:UNKNOWN\n") {
			t.Fatalf("offline name omitted: %s", out.String())
		}
	}
	if !strings.Contains(out.String(), "collection:\"live\"\tvolumeCount:1\tsize:100\tfileCount:3\t") {
		t.Fatalf("live statistics missing: %s", out.String())
	}
	if !strings.Contains(out.String(), "Total 3 collections.\n") || srv.topologyCalls.Load() != 1 {
		t.Fatalf("incomplete listing: %s calls%d", out.String(), srv.topologyCalls.Load())
	}
}
