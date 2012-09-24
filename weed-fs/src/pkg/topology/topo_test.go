package topology

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"pkg/storage"
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

func setup(topologyLayout string) *Topology {
	var data interface{}
	err := json.Unmarshal([]byte(topologyLayout), &data)
	if err != nil {
		fmt.Println("error:", err)
	}

	//need to connect all nodes first before server adding volumes
	topo := NewTopology("mynetwork","/etc/weed.conf","/tmp","test",234,5)
	mTopology := data.(map[string]interface{})
	for dcKey, dcValue := range mTopology {
		dc := NewDataCenter(dcKey)
		dcMap := dcValue.(map[string]interface{})
		topo.LinkChildNode(dc)
		for rackKey, rackValue := range dcMap {
			rack := NewRack(rackKey)
			rackMap := rackValue.(map[string]interface{})
			dc.LinkChildNode(rack)
			for serverKey, serverValue := range rackMap {
				server := NewDataNode(serverKey)
				serverMap := serverValue.(map[string]interface{})
				rack.LinkChildNode(server)
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					vi := storage.VolumeInfo{Id: storage.VolumeId(int64(m["id"].(float64))), Size: int64(m["size"].(float64))}
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
	topo.UnlinkChildNode(NodeId("dc2"))
	if topo.GetActiveVolumeCount() != 15 {
		t.Fail()
	}
	topo.UnlinkChildNode(NodeId("dc3"))
	if topo.GetActiveVolumeCount() != 12 {
		t.Fail()
	}
}

func TestReserveOneVolume(t *testing.T) {
	topo := setup(topologyLayout)
  rand.Seed(time.Now().UnixNano())
  rand.Seed(1)
	ret, node, vid := topo.RandomlyReserveOneVolume()
  fmt.Println("assigned :", ret, ", node :", node,", volume id:", vid)

}
