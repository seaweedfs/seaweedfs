package topology

import (
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
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
			dcRack := NewRack(rackKey)
			rackMap := rackValue.(map[string]interface{})
			dc.LinkChildNode(dcRack)
			for serverKey, serverValue := range rackMap {
				server := NewDataNode(serverKey)
				serverMap := serverValue.(map[string]interface{})
				if ip, ok := serverMap["ip"]; ok {
					server.Ip = ip.(string)
				}
				dcRack.LinkChildNode(server)
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					vi := storage.VolumeInfo{
						Id:      needle.VolumeId(int64(m["id"].(float64))),
						Size:    uint64(m["size"].(float64)),
						Version: needle.CurrentVersion,
					}
					if mVal, ok := m["collection"]; ok {
						vi.Collection = mVal.(string)
					}
					if mVal, ok := m["replication"]; ok {
						rp, _ := super_block.NewReplicaPlacementFromString(mVal.(string))
						vi.ReplicaPlacement = rp
					}
					if vi.ReplicaPlacement != nil {
						vl := topo.GetVolumeLayout(vi.Collection, vi.ReplicaPlacement, needle.EMPTY_TTL, types.HardDriveType)
						vl.RegisterVolume(&vi, server)
						vl.setVolumeWritable(vi.Id)
					}
					server.AddOrUpdateVolume(vi)
				}

				disk := server.getOrCreateDisk("")
				disk.UpAdjustDiskUsageDelta("", &DiskUsageCounts{
					maxVolumeCount: int64(serverMap["limit"].(float64)),
				})

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

var topologyLayout4 = `
{
  "dc1":{
    "rack1":{
      "serverdc111":{
		"ip": "127.0.0.1",
        "volumes":[
          {"id":1, "size":12312, "collection":"test", "replication":"001"},
          {"id":2, "size":12312, "collection":"test", "replication":"100"},
          {"id":4, "size":12312, "collection":"test", "replication":"100"},
          {"id":6, "size":12312, "collection":"test", "replication":"010"}
        ],
        "limit":100
      }
    }
  },
  "dc2":{
    "rack1":{
      "serverdc211":{
		"ip": "127.0.0.2",
        "volumes":[
          {"id":2, "size":12312, "collection":"test", "replication":"100"},
          {"id":3, "size":12312, "collection":"test", "replication":"010"},
          {"id":5, "size":12312, "collection":"test", "replication":"001"},
          {"id":6, "size":12312, "collection":"test", "replication":"010"}
		],
        "limit":100
      }
    }
  },
  "dc3":{
    "rack1":{
      "serverdc311":{
		"ip": "127.0.0.3",
        "volumes":[
          {"id":1, "size":12312, "collection":"test", "replication":"001"},
          {"id":3, "size":12312, "collection":"test", "replication":"010"},
          {"id":4, "size":12312, "collection":"test", "replication":"100"},
          {"id":5, "size":12312, "collection":"test", "replication":"001"}
		],
        "limit":100
      }
    }
  }
}
`

func TestPickForWrite(t *testing.T) {
	topo := setup(topologyLayout4)
	volumeGrowOption := &VolumeGrowOption{
		Collection: "test",
		DataCenter: "",
		Rack:       "",
		DataNode:   "",
	}
	VolumeGrowStrategy.Threshold = 0.9
	for _, rpStr := range []string{"001", "010", "100"} {
		rp, _ := super_block.NewReplicaPlacementFromString(rpStr)
		vl := topo.GetVolumeLayout("test", rp, needle.EMPTY_TTL, types.HardDriveType)
		volumeGrowOption.ReplicaPlacement = rp
		for _, dc := range []string{"", "dc1", "dc2", "dc3", "dc0"} {
			volumeGrowOption.DataCenter = dc
			for _, r := range []string{""} {
				volumeGrowOption.Rack = r
				for _, dn := range []string{""} {
					if dc == "" && dn != "" {
						continue
					}
					volumeGrowOption.DataNode = dn
					fileId, count, _, shouldGrow, err := topo.PickForWrite(1, volumeGrowOption, vl)
					if dc == "dc0" {
						if err == nil || count != 0 || !shouldGrow {
							fmt.Println(dc, r, dn, "pick for write should be with error")
							t.Fail()
						}
					} else if err != nil {
						fmt.Println(dc, r, dn, "pick for write error :", err)
						t.Fail()
					} else if count == 0 {
						fmt.Println(dc, r, dn, "pick for write count is zero")
						t.Fail()
					} else if len(fileId) == 0 {
						fmt.Println(dc, r, dn, "pick for write file id is empty")
						t.Fail()
					} else if shouldGrow {
						fmt.Println(dc, r, dn, "pick for write error : not should grow")
						t.Fail()
					}
				}
			}
		}
	}
}
