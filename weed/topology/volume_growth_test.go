package topology

import (
	"encoding/json"
	"fmt"
	"testing"

	"strings"

	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/syndtr/goleveldb/leveldb/errors"
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
        "limit":15
      },
      "server112":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":10
      },
      "server113":{
        "volumes":[
          {"id":7, "size":12312},
          {"id":8, "size":12312},
          {"id":9, "size":12312}
        ],
        "limit":8
      },
      "server114":{
        "volumes":[],
        "limit":8
      }
    },
    "rack2":{
      "server121":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":8
      },
      "server122":{
        "volumes":[],
        "limit":8
      },
      "server124":{
        "volumes":[],
        "limit":8
      },
      "server123":{
        "volumes":[
          {"id":2, "size":12312},
          {"id":3, "size":12312},
          {"id":4, "size":12312}
        ],
        "limit":5
      }
    }
  },
  "dc2":{
  	"rack2":{
  		"server221":{
  			"volumes":[],
        	"limit":8
  		},
  		"server222":{
  			"volumes":[],
        	"limit":8
  		}
  	}
  },
  "dc3":{
    "rack2":{
      "server321":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":3, "size":12312},
          {"id":5, "size":12312}
        ],
        "limit":8
      },
      "server322":{
        "volumes":[],
        "limit":7
      }
    }
  }
}
`

var testLocList = [][]string{
	{"server111", "server121"},
	{"server111", "server112"},
	{"server111", "server112", "server113"},
	{"server111", "server221", "server321"},
	{"server112"},
}

func setup(topologyLayout string) *Topology {
	var data interface{}
	err := json.Unmarshal([]byte(topologyLayout), &data)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("data:", data)

	//need to connect all nodes first before server adding volumes
	topo, err := NewTopology("weedfs", "/etc/weedfs/weedfs.conf",
		storage.NewCollectionSettings("000", "0.3"),
		sequence.NewMemorySequencer(), 32*1024, 5)
	if err != nil {
		panic("error: " + err.Error())
	}
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
					vi := &storage.VolumeInfo{
						Id:      storage.VolumeId(int64(m["id"].(float64))),
						Size:    uint64(m["size"].(float64)),
						Version: storage.CurrentVersion}
					server.AddOrUpdateVolume(vi)
				}
				server.UpAdjustMaxVolumeCountDelta(int(serverMap["limit"].(float64)))
			}
		}
	}

	return topo
}

func TestFindEmptySlotsForOneVolume(t *testing.T) {
	topo := setup(topologyLayout)
	rp, _ := storage.NewReplicaPlacementFromString("111")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "",
		ReplicaPlacement: rp,
		DataCenter:       "dc1",
		Rack:             "",
		DataNode:         "",
	}
	servers, err := FindEmptySlotsForOneVolume(topo, volumeGrowOption, nil)
	if err != nil {
		fmt.Println("finding empty slots error :", err)
		t.Fail()
	}
	for _, server := range servers {
		fmt.Printf("assigned node: %s, free space: %d\n", server.Id(), server.FreeSpace())
	}

}

func getDataNodeFromId(topo *Topology, id string) (foundDn *DataNode) {
	nid := NodeId(id)
	topo.WalkDataNode(func(dn *DataNode) (e error) {
		if dn.Id() == nid {
			foundDn = dn
			e = errors.New("Found.")
		}
		return
	})
	return
}

func setupTestLocationList(topo *Topology) (ret []*VolumeLocationList) {

	for _, ll := range testLocList {
		vl := &VolumeLocationList{}
		for _, nid := range ll {
			if n := getDataNodeFromId(topo, nid); n != nil {
				vl.list = append(vl.list, n)
			}
		}
		ret = append(ret, vl)
	}
	return
}

func joinNodeId(dns []*DataNode) string {
	ss := []string{}
	for _, dn := range dns {
		ss = append(ss, string(dn.Id()))
	}
	return strings.Join(ss, ", ")
}

func TestFindEmptySlotsWithExistsNodes(t *testing.T) {
	topo := setup(topologyLayout)
	rp, _ := storage.NewReplicaPlacementFromString("112")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "",
		ReplicaPlacement: rp,
		DataCenter:       "dc1",
		Rack:             "",
		DataNode:         "",
	}
	testLocationList := setupTestLocationList(topo)
	for _, locationList := range testLocationList {
		lrp := locationList.CalcReplicaPlacement()
		t.Logf("location list: [%s], replica placement = %s\n", joinNodeId(locationList.list), lrp.String())
		if lrp.Compare(rp) < 0 {
			servers, err := FindEmptySlotsForOneVolume(topo, volumeGrowOption, locationList)
			if err != nil {
				t.Log("finding empty slots error :", err)
				t.Fail()
			}
			t.Logf("assigned node: %s\n\n", joinNodeId(servers))
		}
	}
}
