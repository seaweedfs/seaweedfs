package topology

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
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
          {"id":3, "size":12312},
          {"id":4, "size":12312}
        ],
        "limit":5
      }
    }
  },
  "dc2":{
  },
  "dc3":{
    "rack2":{
      "server321":{
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
	fmt.Println("data:", data)

	//need to connect all nodes first before server adding volumes
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
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
					vi := storage.VolumeInfo{
						Id:      needle.VolumeId(int64(m["id"].(float64))),
						Size:    uint64(m["size"].(float64)),
						Version: needle.CurrentVersion}
					server.AddOrUpdateVolume(vi)
				}

				disk := server.getOrCreateDisk("")
				deltaDiskUsages := newDiskUsages()
				deltaDiskUsage := deltaDiskUsages.getOrCreateDisk("")
				deltaDiskUsage.maxVolumeCount = int64(serverMap["limit"].(float64))
				disk.UpAdjustDiskUsageDelta(deltaDiskUsages)

			}
		}
	}

	return topo
}

func TestFindEmptySlotsForOneVolume(t *testing.T) {
	topo := setup(topologyLayout)
	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("002")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "",
		ReplicaPlacement: rp,
		DataCenter:       "dc1",
		Rack:             "",
		DataNode:         "",
	}
	servers, err := vg.findEmptySlotsForOneVolume(topo, volumeGrowOption)
	if err != nil {
		fmt.Println("finding empty slots error :", err)
		t.Fail()
	}
	for _, server := range servers {
		fmt.Println("assigned node :", server.Id())
	}
}

var topologyLayout2 = `
{
  "dc1":{
    "rack1":{
      "server111":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":2, "size":12312},
          {"id":3, "size":12312}
        ],
        "limit":300
      },
      "server112":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":300
      },
      "server113":{
        "volumes":[],
        "limit":300
      },
      "server114":{
        "volumes":[],
        "limit":300
      },
      "server115":{
        "volumes":[],
        "limit":300
      },
      "server116":{
        "volumes":[],
        "limit":300
      }
    },
    "rack2":{
      "server121":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":300
      },
      "server122":{
        "volumes":[],
        "limit":300
      },
      "server123":{
        "volumes":[
          {"id":2, "size":12312},
          {"id":3, "size":12312},
          {"id":4, "size":12312}
        ],
        "limit":300
      },
      "server124":{
        "volumes":[],
        "limit":300
      },
      "server125":{
        "volumes":[],
        "limit":300
      },
      "server126":{
        "volumes":[],
        "limit":300
      }
    },
    "rack3":{
      "server131":{
        "volumes":[],
        "limit":300
      },
      "server132":{
        "volumes":[],
        "limit":300
      },
      "server133":{
        "volumes":[],
        "limit":300
      },
      "server134":{
        "volumes":[],
        "limit":300
      },
      "server135":{
        "volumes":[],
        "limit":300
      },
      "server136":{
        "volumes":[],
        "limit":300
      }
    }
  }
}
`

func TestReplication011(t *testing.T) {
	topo := setup(topologyLayout2)
	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("011")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "MAIL",
		ReplicaPlacement: rp,
		DataCenter:       "dc1",
		Rack:             "",
		DataNode:         "",
	}
	servers, err := vg.findEmptySlotsForOneVolume(topo, volumeGrowOption)
	if err != nil {
		fmt.Println("finding empty slots error :", err)
		t.Fail()
	}
	for _, server := range servers {
		fmt.Println("assigned node :", server.Id())
	}
}

var topologyLayout3 = `
{
  "dc1":{
    "rack1":{
      "server111":{
        "volumes":[],
        "limit":2000
      }
    }
  },
  "dc2":{
    "rack2":{
      "server222":{
        "volumes":[],
        "limit":2000
      }
    }
  },
  "dc3":{
    "rack3":{
      "server333":{
        "volumes":[],
        "limit":1000
      }
    }
  },
  "dc4":{
    "rack4":{
      "server444":{
        "volumes":[],
        "limit":1000
      }
    }
  },
  "dc5":{
    "rack5":{
      "server555":{
        "volumes":[],
        "limit":500
      }
    }
  },
  "dc6":{
    "rack6":{
      "server666":{
        "volumes":[],
        "limit":500
      }
    }
  }
}
`

func TestFindEmptySlotsForOneVolumeScheduleByWeight(t *testing.T) {
	topo := setup(topologyLayout3)
	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("100")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "Weight",
		ReplicaPlacement: rp,
		DataCenter:       "",
		Rack:             "",
		DataNode:         "",
	}

	distribution := map[NodeId]int{}
	// assign 1000 volumes
	for i := 0; i < 1000; i++ {
		servers, err := vg.findEmptySlotsForOneVolume(topo, volumeGrowOption)
		if err != nil {
			fmt.Println("finding empty slots error :", err)
			t.Fail()
		}
		for _, server := range servers {
			// fmt.Println("assigned node :", server.Id())
			if _, ok := distribution[server.id]; !ok {
				distribution[server.id] = 0
			}
			distribution[server.id] += 1
		}
	}

	for k, v := range distribution {
		fmt.Printf("%s : %d\n", k, v)
	}
}
