package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"pkg/storage"
	"pkg/topology"
	"pkg/util"
	"strconv"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowth struct {
	copy1factor int
	copy2factor int
	copy3factor int
	copyAll     int
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{copy1factor: 7, copy2factor: 6, copy3factor: 3}
}

func (vg *VolumeGrowth) GrowByType(repType storage.ReplicationType, topo *topology.Topology) {
	switch repType {
	case storage.Copy00:
		vg.GrowByCountAndType(vg.copy1factor, repType, topo)
	case storage.Copy10:
		vg.GrowByCountAndType(vg.copy2factor, repType, topo)
	case storage.Copy20:
		vg.GrowByCountAndType(vg.copy3factor, repType, topo)
	case storage.Copy01:
		vg.GrowByCountAndType(vg.copy2factor, repType, topo)
	case storage.Copy11:
		vg.GrowByCountAndType(vg.copy3factor, repType, topo)
	}

}
func (vg *VolumeGrowth) GrowByCountAndType(count int, repType storage.ReplicationType, topo *topology.Topology) {
	switch repType {
	case storage.Copy00:
		for i := 0; i < count; i++ {
			ret, server, vid := topo.RandomlyReserveOneVolume()
			if ret {
				vg.grow(topo, *vid, repType, server)
			}
		}
	case storage.Copy10:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(2)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				for _, n := range picked {
					if n.FreeSpace() > 0 {
						if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid); ok {
							servers = append(servers, server)
						}
					}
				}
				if len(servers) == 2 {
					vg.grow(topo, vid, repType, servers[0], servers[1])
				}
			}
		}
	case storage.Copy20:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(3)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				for _, n := range picked {
					if n.FreeSpace() > 0 {
						if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid); ok {
							servers = append(servers, server)
						}
					}
				}
				if len(servers) == 3 {
					vg.grow(topo, vid, repType, servers[0], servers[1], servers[2])
				}
			}
		}
	case storage.Copy01:
		for i := 0; i < count; i++ {
			//randomly pick one server, and then choose from the same rack
			ret, server1, vid := topo.RandomlyReserveOneVolume()
			if ret {
				rack := server1.Parent()
				exclusion := make(map[string]topology.Node)
				exclusion[server1.String()] = server1
				newNodeList := topology.NewNodeList(rack.Children(), exclusion)
				if newNodeList.FreeSpace() > 0 {
					ret2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), *vid)
					if ret2 {
						vg.grow(topo, *vid, repType, server1, server2)
					}
				}
			}
		}
	case storage.Copy11:
		for i := 0; i < count; i++ {
		}
	}

}
func (vg *VolumeGrowth) grow(topo *topology.Topology, vid storage.VolumeId, repType storage.ReplicationType, servers ...*topology.DataNode) {
	for _, server := range servers {
		if err := AllocateVolume(server, vid, repType); err == nil {
			vi := &storage.VolumeInfo{Id: vid, Size: 0}
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, server)
			fmt.Println("added", vid, "to", server)
		} else {
			//TODO: need error handling
			fmt.Println("Failed to assign", vid, "to", servers)
		}
	}
	fmt.Println("Assigning", vid, "to", servers)
}

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *topology.DataNode, vid storage.VolumeId, repType storage.ReplicationType) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("replicationType", repType.String())
	jsonBlob, err := util.Post("http://"+dn.Ip+":"+strconv.Itoa(dn.Port)+"/admin/assign_volume", values)
	if err != nil {
		return err
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
