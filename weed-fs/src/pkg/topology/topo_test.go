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

func setup() *Topology {
	var data interface{}
	err := json.Unmarshal([]byte(topologyLayout), &data)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("data:", data)
	printMap(data)

	//need to connect all nodes first before server adding volumes
	topo := NewTopology(NodeId("mynetwork"))
	mTopology := data.(map[string]interface{})
	for dcKey, dcValue := range mTopology {
		dc := NewDataCenter(NodeId(dcKey))
		dc.Node.parent = &topo.Node
		dcMap := dcValue.(map[string]interface{})
		topo.Node.AddNode(&dc.Node)
		for rackKey, rackValue := range dcMap {
			rack := NewRack(NodeId(rackKey))
			rack.Node.parent = &dc.Node
			rackMap := rackValue.(map[string]interface{})
			dc.Node.AddNode(&rack.Node)
			for serverKey, serverValue := range rackMap {
				server := NewServer(NodeId(serverKey))
				server.Node.parent = &rack.Node
				serverMap := serverValue.(map[string]interface{})
				rack.Node.AddNode(&server.Node)
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					vi := &storage.VolumeInfo{Id: storage.VolumeId(int64(m["id"].(float64))), Size: int64(m["size"].(float64))}
					server.AddVolume(vi)
				}
				server.Node.AddMaxVolumeCount(int(serverMap["limit"].(float64)))
			}
		}
	}

	fmt.Println("topology:", *topo)

	bytes, err := json.Marshal(topo.Node.children)
	if err != nil {
		fmt.Println("json error:", err)
	}
	fmt.Println("json topo:", string(bytes))

	return topo
}

func printMap(mm interface{}) {
	m := mm.(map[string]interface{})
	for k, v := range m {
		switch vv := v.(type) {
		case string:
			fmt.Println(k, "\"", vv, "\"")
		case int, float64:
			fmt.Println(k, ":", vv)
		case []interface{}:
			fmt.Println(k, ":[")
			for _, u := range vv {
				fmt.Println(u)
				fmt.Println(",")
			}
			fmt.Println("]")
		default:
			fmt.Println(k, ":")
			printMap(vv)
		}
	}
}

func TestAddVolume(t *testing.T) {
	topo := setup()
	v := &storage.VolumeInfo{}
	topo.AddVolume(v)
}

func TestRemoveDataCenter(t *testing.T) {
	topo := setup()
	topo.RemoveNode(NodeId("dc2"))
	if topo.activeVolumeCount != 15 {
		t.Fail()
	}
	topo.RemoveNode(NodeId("dc3"))
	if topo.activeVolumeCount != 12 {
		t.Fail()
	}
}

func TestReserveOneVolume(t *testing.T) {
	topo := setup()
  rand.Seed(time.Now().UnixNano())
	ret, node, vid := topo.RandomlyReserveOneVolume()
	fmt.Println("topology:", topo.Node)
  fmt.Println("assigned :", ret)
  fmt.Println("assigned node :", node)
	fmt.Println("assigned volume id:", vid)
}
