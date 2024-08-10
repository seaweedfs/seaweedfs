package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend/memory_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collectionName := r.FormValue("collection")
	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection %s does not exist", collectionName))
		return
	}
	for _, server := range collection.ListVolumeServers() {
		err := operation.WithVolumeServerClient(false, server.ServerAddress(), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collection.Name,
			})
			return deleteErr
		})
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		}
	}
	ms.Topo.DeleteCollection(collectionName)

	w.WriteHeader(http.StatusNoContent)
	return
}

func (ms *MasterServer) dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.Version()
	m["Topology"] = ms.Topo.ToInfo()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (ms *MasterServer) volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcString := r.FormValue("garbageThreshold")
	gcThreshold := ms.option.GarbageThreshold
	if gcString != "" {
		var err error
		gcThreshold, err = strconv.ParseFloat(gcString, 32)
		if err != nil {
			glog.V(0).Infof("garbageThreshold %s is not a valid float number: %v", gcString, err)
			writeJsonError(w, r, http.StatusNotAcceptable, fmt.Errorf("garbageThreshold %s is not a valid float number", gcString))
			return
		}
	}
	// glog.Infoln("garbageThreshold =", gcThreshold)
	ms.Topo.Vacuum(ms.grpcDialOption, gcThreshold, 0, "", ms.preallocateSize)
	ms.dirStatusHandler(w, r)
}

func (ms *MasterServer) volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := uint64(0)
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}
	glog.V(0).Infof("volumeGrowHandler received %v from %v", option.String(), r.RemoteAddr)

	if count, err = strconv.ParseUint(r.FormValue("count"), 10, 32); err == nil {
		replicaCount := int64(count * uint64(option.ReplicaPlacement.GetCopyCount()))
		if ms.Topo.AvailableSpaceFor(option) < replicaCount {
			err = fmt.Errorf("only %d volumes left, not enough for %d", ms.Topo.AvailableSpaceFor(option), replicaCount)
		} else if !ms.Topo.DataCenterExists(option.DataCenter) {
			err = fmt.Errorf("data center %v not found in topology", option.DataCenter)
		} else {
			var newVidLocations []*master_pb.VolumeLocation
			newVidLocations, err = ms.vg.GrowByCountAndType(ms.grpcDialOption, uint32(count), option, ms.Topo)
			count = uint64(len(newVidLocations))
		}
	} else {
		err = fmt.Errorf("can not parse parameter count %s", r.FormValue("count"))
	}

	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	} else {
		writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"count": count})
	}
}

func (ms *MasterServer) volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.Version()
	m["Volumes"] = ms.Topo.ToVolumeMap()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (ms *MasterServer) redirectHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	collection := r.FormValue("collection")
	location := ms.findVolumeLocation(collection, vid)
	if location.Error == "" {
		loc := location.Locations[rand.Intn(len(location.Locations))]
		url, _ := util_http.NormalizeUrl(loc.PublicUrl)
		if r.URL.RawQuery != "" {
			url = url + r.URL.Path + "?" + r.URL.RawQuery
		} else {
			url = url + r.URL.Path
		}
		http.Redirect(w, r, url, http.StatusPermanentRedirect)
	} else {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("volume id %s not found: %s", vid, location.Error))
	}
}

func (ms *MasterServer) submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		submitForClientHandler(w, r, func(ctx context.Context) pb.ServerAddress { return ms.option.Master }, ms.grpcDialOption)
	} else {
		masterUrl, err := ms.Topo.Leader()
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		} else {
			submitForClientHandler(w, r, func(ctx context.Context) pb.ServerAddress { return masterUrl }, ms.grpcDialOption)
		}
	}
}

func (ms *MasterServer) getVolumeGrowOption(r *http.Request) (*topology.VolumeGrowOption, error) {
	replicationString := r.FormValue("replication")
	if replicationString == "" {
		replicationString = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(replicationString)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(r.FormValue("ttl"))
	if err != nil {
		return nil, err
	}
	memoryMapMaxSizeMb, err := memory_map.ReadMemoryMapMaxSizeMb(r.FormValue("memoryMapMaxSizeMb"))
	if err != nil {
		return nil, err
	}
	diskType := types.ToDiskType(r.FormValue("disk"))

	preallocate := ms.preallocateSize
	if r.FormValue("preallocate") != "" {
		preallocate, err = strconv.ParseInt(r.FormValue("preallocate"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse int64 preallocate = %s: %v", r.FormValue("preallocate"), err)
		}
	}
	volumeGrowOption := &topology.VolumeGrowOption{
		Collection:         r.FormValue("collection"),
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		DiskType:           diskType,
		Preallocate:        preallocate,
		DataCenter:         r.FormValue("dataCenter"),
		Rack:               r.FormValue("rack"),
		DataNode:           r.FormValue("dataNode"),
		MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
	}
	return volumeGrowOption, nil
}
