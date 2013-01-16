package replication

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"pkg/storage"
	"pkg/topology"
	"testing"
	"time"
)

var topologyLayout = `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":2, "size":12312},
          {"id":3, "size":12312}
        ],
        "limit":3
      },
      "server2":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":10
      }
    },
    "rack2":{
      "server1":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":4
      },
      "server2":{
        "volumes":[],
        "limit":4
      },
      "server3":{
        "volumes":[
          {"id":2, "size":12312},
          {"id":3, "size":12312},
          {"id":4, "size":12312}
        ],
        "limit":2
      }
    }
  },
  "dc2":{
  },
  "dc3":{
    "rack2":{
      "server1":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":3, "size":12312},
          {"id":5, "size":12312}
        ],
        "limit":4
      }
    }
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
	topo := topology.NewTopology("mynetwork", "/etc/weedfs/weedfs.conf", "/tmp", "testing", 32*1024, 5)
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
				serverMap := serverValue.(map[string]interface{})
				rack.LinkChildNode(server)
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					vi := storage.VolumeInfo{Id: storage.VolumeId(int64(m["id"].(float64))), Size: int64(m["size"].(float64)), Version: storage.CurrentVersion}
					server.AddOrUpdateVolume(vi)
				}
				server.UpAdjustMaxVolumeCountDelta(int(serverMap["limit"].(float64)))
			}
		}
	}

	return topo
}

func TestRemoveDataCenter(t *testing.T) {
	topo := setup(topologyLayout)
	topo.UnlinkChildNode(topology.NodeId("dc2"))
	if topo.GetActiveVolumeCount() != 15 {
		t.Fail()
	}
	topo.UnlinkChildNode(topology.NodeId("dc3"))
	if topo.GetActiveVolumeCount() != 12 {
		t.Fail()
	}
}

func TestReserveOneVolume(t *testing.T) {
	topo := setup(topologyLayout)
	rand.Seed(time.Now().UnixNano())
	vg := &VolumeGrowth{copy1factor: 3, copy2factor: 2, copy3factor: 1, copyAll: 4}
	if c, e := vg.GrowByCountAndType(1, storage.Copy000, topo); e == nil {
		t.Log("reserved", c)
	}
}
