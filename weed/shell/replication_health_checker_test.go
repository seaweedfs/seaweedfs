package shell

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"strconv"
	"strings"
	"testing"
)

var topologyLayout = `
{
  "dc1":{
    "rack1":{
      "server111":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":2, "size":12312},
          {"id":3, "size":12312}
        ],
        "limit":3
      },
      "server112":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":10
      }
    },
    "rack2":{
      "server121":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":4
      },
      "server122":{
        "volumes":[],
        "limit":4
      },
      "server123":{
        "volumes":[
          {"id":2, "size":12312},
          {"id":3, "size":12311},
          {"id":4, "size":12312}
        ],
        "limit":5
      }
    }
  },
  "dc3":{
  }
}
`

func setup(topologyLayout string) *topology.Topology {
	var data interface{}
	err := json.Unmarshal([]byte(topologyLayout), &data)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("data:", data)

	//need to connect all nodes first before server adding volumes
	var portT int
	topo := topology.NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5)
	mTopology := data.(map[string]interface{})
	for dcKey, dcValue := range mTopology {
		dc := topology.NewDataCenter(dcKey)
		dcMap := dcValue.(map[string]interface{})
		topo.LinkChildNode(dc)
		for rackKey, rackValue := range dcMap {
			rack := topology.NewRack(rackKey)
			rackMap := rackValue.(map[string]interface{})
			dc.LinkChildNode(rack)
			for serverKey, serverValue := range rackMap {
				server := topology.NewDataNode(serverKey)
				server.Ip = "localhost"
				portT += 2
				server.Port = portT
				server.PublicUrl = server.Ip + ":" + strconv.FormatUint(uint64(server.Port), 10)
				serverMap := serverValue.(map[string]interface{})
				rack.LinkChildNode(server)
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					vi := storage.VolumeInfo{
						Id:               needle.VolumeId(int64(m["id"].(float64))),
						Size:             uint64(m["size"].(float64)),
						Version:          needle.CurrentVersion,
						ReplicaPlacement: &storage.ReplicaPlacement{1, 0, 0},
						Ttl:              needle.EMPTY_TTL,
						Collection:       "",
					}
					server.AddOrUpdateVolume(vi)
				}
				server.UpAdjustMaxVolumeCountDelta(int64(serverMap["limit"].(float64)))
			}
		}
	}

	return topo
}

func TestGetVolumeList(t *testing.T) {
	topo := setup(topologyLayout)
	topoInfo := topo.ToTopologyInfo()
	if nil == topoInfo {
		t.Errorf("failed.")
	}
}

func TestReplicationHealthChecker_GetErrorReplications(t *testing.T) {
	topo := setup(topologyLayout)
	topoInfo := topo.ToTopologyInfo()
	if nil == topoInfo {
		t.Errorf("failed.")
	}

	checker := NewReplicationHealthChecker(context.Background(), grpc.EmptyDialOption{})
	errVids, err := checker.Check(topoInfo)
	if err != nil {
		t.Error(err)
		return
	} else {
		fmt.Printf("error vids : %v\n", errVids)
	}
}

// this UT need mock or real seaweedfs service
func TestReplicationHealthChecker_GetErrorReplications2(t *testing.T) {
	masters := "localhost:9633,localhost:9733,localhost:9833"
	ctx := context.Background()
	masterClient := wdclient.NewMasterClient(ctx, grpc.WithInsecure(), "shell", strings.Split(masters, ","))
	go masterClient.KeepConnectedToMaster()
	masterClient.WaitUntilConnected()

	var resp *master_pb.VolumeListResponse
	if err := masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		var err error
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	}); err != nil {
		t.Error(err)
	}

	//respBytes, err := json.Marshal(resp)
	//if err != nil {
	//	t.Error(err)
	//}
	//t.Log(string(respBytes[:]))

	checker := NewReplicationHealthChecker(ctx, grpc.WithInsecure())
	errVids, err := checker.Check(resp.TopologyInfo)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("error vids : %v\n", errVids)

	repair := NewReplicationHealthRepair(ctx, grpc.WithInsecure())
	success, failed, err := repair.Repair(resp.TopologyInfo, errVids)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("success:%v, failed:%v", success, failed)
}
