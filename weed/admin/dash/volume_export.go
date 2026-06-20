package dash

import (
	"context"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// VolumeListExport is a full-cluster volume report, downloadable from the admin
// UI as JSON. It mirrors the topology the `volume.list` shell command walks
// (data center -> rack -> node -> disk -> volumes / EC shards) and adds derived
// fields the shell command does not print: garbage and fullness ratios, a
// human-readable modified time, remote-tiering keys, per-disk capacity counts,
// and the cluster-wide duplicate-volume-id scan.
type VolumeListExport struct {
	GeneratedAt        time.Time           `json:"generated_at"`
	VolumeSizeLimitMB  uint64              `json:"volume_size_limit_mb"`
	FilterCollection   string              `json:"filter_collection,omitempty"`
	Totals             VolumeExportTotals  `json:"totals"`
	DuplicateVolumeIds []DuplicateVolumeId `json:"duplicate_volume_ids"`
	DataCenters        []*ExportDataCenter `json:"data_centers"`
}

// VolumeExportTotals aggregates the included records. TotalSize sums normal
// volume sizes and EC shard sizes (matching volume.list's statistics); the
// file/delete totals cover normal volumes only.
type VolumeExportTotals struct {
	VolumeCount      int    `json:"volume_count"`
	EcShardCount     int    `json:"ec_shard_count"`
	TotalSize        uint64 `json:"total_size"`
	FileCount        uint64 `json:"file_count"`
	DeletedFileCount uint64 `json:"deleted_file_count"`
	DeletedBytes     uint64 `json:"deleted_bytes"`
}

// DuplicateVolumeId flags a volume id that appears under more than one
// collection, where collection.delete or a bare-id operation is dangerous.
type DuplicateVolumeId struct {
	VolumeId    uint32   `json:"volume_id"`
	Collections []string `json:"collections"`
}

type ExportDataCenter struct {
	Id    string        `json:"id"`
	Racks []*ExportRack `json:"racks"`
}

type ExportRack struct {
	Id    string        `json:"id"`
	Nodes []*ExportNode `json:"nodes"`
}

type ExportNode struct {
	Id       string        `json:"id"`
	GrpcPort uint32        `json:"grpc_port,omitempty"`
	Disks    []*ExportDisk `json:"disks"`
}

// ExportDisk capacity counts describe the whole physical disk; the Volumes /
// EcShards lists are filtered when a collection filter is active.
type ExportDisk struct {
	DiskType          string           `json:"disk_type"`
	DiskId            uint32           `json:"disk_id"`
	VolumeCount       int64            `json:"volume_count"`
	MaxVolumeCount    int64            `json:"max_volume_count"`
	ActiveVolumeCount int64            `json:"active_volume_count"`
	FreeVolumeCount   int64            `json:"free_volume_count"`
	RemoteVolumeCount int64            `json:"remote_volume_count"`
	Volumes           []*ExportVolume  `json:"volumes,omitempty"`
	EcShards          []*ExportEcShard `json:"ec_shards,omitempty"`
}

type ExportVolume struct {
	Id                uint32  `json:"id"`
	Collection        string  `json:"collection"`
	Size              uint64  `json:"size"`
	FileCount         uint64  `json:"file_count"`
	DeleteCount       uint64  `json:"delete_count"`
	DeletedByteCount  uint64  `json:"deleted_byte_count"`
	GarbageRatio      float64 `json:"garbage_ratio"`
	FullnessRatio     float64 `json:"fullness_ratio"`
	ReplicaPlacement  string  `json:"replica_placement"`
	Version           uint32  `json:"version"`
	Ttl               string  `json:"ttl"`
	ReadOnly          bool    `json:"read_only"`
	CompactRevision   uint32  `json:"compact_revision"`
	ModifiedAtSecond  int64   `json:"modified_at_second"`
	ModifiedAt        string  `json:"modified_at,omitempty"`
	DiskType          string  `json:"disk_type"`
	DiskId            uint32  `json:"disk_id"`
	RemoteStorageName string  `json:"remote_storage_name,omitempty"`
	RemoteStorageKey  string  `json:"remote_storage_key,omitempty"`
}

type ExportEcShard struct {
	Id          uint32   `json:"id"`
	Collection  string   `json:"collection"`
	ShardIds    []uint32 `json:"shard_ids"`
	ShardSizes  []int64  `json:"shard_sizes"`
	TotalSize   int64    `json:"total_size"`
	FileCount   uint64   `json:"file_count"`
	DeleteCount uint64   `json:"delete_count"`
	DiskType    string   `json:"disk_type"`
	DiskId      uint32   `json:"disk_id"`
	ExpireAtSec uint64   `json:"expire_at_sec,omitempty"`
}

// ExportClusterVolumeList builds a full-cluster volume report from the master
// topology. A non-empty collection limits the listed volumes and EC shards to
// that collection; the duplicate-id scan always covers the whole cluster.
// generatedAt is passed in so the report is deterministic and testable.
func (s *AdminServer) ExportClusterVolumeList(ctx context.Context, collection string, generatedAt time.Time) (*VolumeListExport, error) {
	export := &VolumeListExport{
		GeneratedAt:        generatedAt,
		FilterCollection:   collection,
		DuplicateVolumeIds: []DuplicateVolumeId{},
		DataCenters:        []*ExportDataCenter{},
	}

	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}
		export.VolumeSizeLimitMB = resp.VolumeSizeLimitMb
		topo := resp.TopologyInfo
		if topo == nil {
			return nil
		}

		volumeSizeLimit := resp.VolumeSizeLimitMb * 1024 * 1024
		export.DuplicateVolumeIds = findDuplicateVolumeIdsForExport(topo)

		dcs := append([]*master_pb.DataCenterInfo(nil), topo.DataCenterInfos...)
		sort.Slice(dcs, func(i, j int) bool { return dcs[i].Id < dcs[j].Id })
		for _, dc := range dcs {
			exportDc := &ExportDataCenter{Id: dc.Id, Racks: []*ExportRack{}}
			racks := append([]*master_pb.RackInfo(nil), dc.RackInfos...)
			sort.Slice(racks, func(i, j int) bool { return racks[i].Id < racks[j].Id })
			for _, rack := range racks {
				exportRack := &ExportRack{Id: rack.Id, Nodes: []*ExportNode{}}
				nodes := append([]*master_pb.DataNodeInfo(nil), rack.DataNodeInfos...)
				sort.Slice(nodes, func(i, j int) bool { return nodes[i].Id < nodes[j].Id })
				for _, node := range nodes {
					exportNode := &ExportNode{Id: node.Id, GrpcPort: node.GrpcPort, Disks: []*ExportDisk{}}
					for _, diskType := range sortedDiskTypes(node.DiskInfos) {
						// Split per physical disk like volume.list so DiskId is meaningful.
						for _, diskInfo := range node.DiskInfos[diskType].SplitByPhysicalDisk() {
							if disk := buildExportDisk(diskInfo, collection, volumeSizeLimit, &export.Totals); disk != nil {
								exportNode.Disks = append(exportNode.Disks, disk)
							}
						}
					}
					if len(exportNode.Disks) > 0 {
						exportRack.Nodes = append(exportRack.Nodes, exportNode)
					}
				}
				if len(exportRack.Nodes) > 0 {
					exportDc.Racks = append(exportDc.Racks, exportRack)
				}
			}
			if len(exportDc.Racks) > 0 {
				export.DataCenters = append(export.DataCenters, exportDc)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return export, nil
}

func sortedDiskTypes(diskInfos map[string]*master_pb.DiskInfo) []string {
	types := make([]string, 0, len(diskInfos))
	for t := range diskInfos {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// buildExportDisk returns nil when the disk holds nothing matching the filter.
func buildExportDisk(diskInfo *master_pb.DiskInfo, collection string, volumeSizeLimit uint64, totals *VolumeExportTotals) *ExportDisk {
	diskType := diskInfo.Type
	if diskType == "" {
		diskType = "hdd"
	}

	vols := append([]*master_pb.VolumeInformationMessage(nil), diskInfo.VolumeInfos...)
	sort.Slice(vols, func(i, j int) bool { return vols[i].Id < vols[j].Id })
	var volumes []*ExportVolume
	for _, vi := range vols {
		if collection != "" && !matchesCollection(vi.Collection, collection) {
			continue
		}
		volumes = append(volumes, buildExportVolume(vi, volumeSizeLimit))
		totals.VolumeCount++
		totals.TotalSize += vi.Size
		totals.FileCount += vi.FileCount
		totals.DeletedFileCount += vi.DeleteCount
		totals.DeletedBytes += vi.DeletedByteCount
	}

	shards := append([]*master_pb.VolumeEcShardInformationMessage(nil), diskInfo.EcShardInfos...)
	sort.Slice(shards, func(i, j int) bool { return shards[i].Id < shards[j].Id })
	var ecShards []*ExportEcShard
	for _, eci := range shards {
		if collection != "" && !matchesCollection(eci.Collection, collection) {
			continue
		}
		shard := buildExportEcShard(eci)
		ecShards = append(ecShards, shard)
		totals.EcShardCount++
		totals.TotalSize += uint64(shard.TotalSize)
	}

	if len(volumes) == 0 && len(ecShards) == 0 {
		return nil
	}

	return &ExportDisk{
		DiskType:          diskType,
		DiskId:            diskInfo.DiskId,
		VolumeCount:       diskInfo.VolumeCount,
		MaxVolumeCount:    diskInfo.MaxVolumeCount,
		ActiveVolumeCount: diskInfo.ActiveVolumeCount,
		FreeVolumeCount:   diskInfo.FreeVolumeCount,
		RemoteVolumeCount: diskInfo.RemoteVolumeCount,
		Volumes:           volumes,
		EcShards:          ecShards,
	}
}

func buildExportVolume(m *master_pb.VolumeInformationMessage, volumeSizeLimit uint64) *ExportVolume {
	v := &ExportVolume{
		Id:                m.Id,
		Collection:        m.Collection,
		Size:              m.Size,
		FileCount:         m.FileCount,
		DeleteCount:       m.DeleteCount,
		DeletedByteCount:  m.DeletedByteCount,
		Version:           m.Version,
		ReadOnly:          m.ReadOnly,
		CompactRevision:   m.CompactRevision,
		ModifiedAtSecond:  m.ModifiedAtSecond,
		DiskType:          m.DiskType,
		DiskId:            m.DiskId,
		RemoteStorageName: m.RemoteStorageName,
		RemoteStorageKey:  m.RemoteStorageKey,
	}
	// Decode replica placement and TTL the way volume.list does.
	if vi, err := storage.NewVolumeInfo(m); err == nil {
		v.ReplicaPlacement = vi.ReplicaPlacement.String()
		v.Ttl = vi.Ttl.String()
	}
	if m.Size > 0 {
		v.GarbageRatio = float64(m.DeletedByteCount) / float64(m.Size)
	}
	if volumeSizeLimit > 0 {
		v.FullnessRatio = float64(m.Size) / float64(volumeSizeLimit)
	}
	if m.ModifiedAtSecond > 0 {
		v.ModifiedAt = time.Unix(m.ModifiedAtSecond, 0).UTC().Format(time.RFC3339)
	}
	return v
}

func buildExportEcShard(m *master_pb.VolumeEcShardInformationMessage) *ExportEcShard {
	si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(m)
	return &ExportEcShard{
		Id:          m.Id,
		Collection:  m.Collection,
		ShardIds:    si.IdsUint32(),
		ShardSizes:  si.SizesInt64(),
		TotalSize:   int64(si.TotalSize()),
		FileCount:   m.FileCount,
		DeleteCount: m.DeleteCount,
		DiskType:    m.DiskType,
		DiskId:      m.DiskId,
		ExpireAtSec: m.ExpireAtSec,
	}
}

// findDuplicateVolumeIdsForExport reports volume ids living under more than one
// collection. Volume ids are meant to be globally unique; a duplicate is a
// cluster-health hazard (collection.delete destroys one collection's copy,
// bare-id lookups are ambiguous). This mirrors volume.list's findDuplicateVolumeIds
// rather than reaching into the shell package, per the admin-renderer design.
// First collection per id is tracked alone, and a set is allocated only on the
// first clash, keeping allocations O(duplicates) on million-volume clusters.
func findDuplicateVolumeIdsForExport(topo *master_pb.TopologyInfo) []DuplicateVolumeId {
	firstCollectionByVid := make(map[uint32]string)
	collectionsByVid := make(map[uint32]map[string]struct{})
	note := func(vid uint32, collection string) {
		if collections := collectionsByVid[vid]; collections != nil {
			collections[collection] = struct{}{}
			return
		}
		first, seen := firstCollectionByVid[vid]
		if !seen {
			firstCollectionByVid[vid] = collection
			return
		}
		if first == collection {
			return
		}
		collectionsByVid[vid] = map[string]struct{}{first: {}, collection: {}}
	}
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, disk := range node.DiskInfos {
					for _, vi := range disk.VolumeInfos {
						note(vi.Id, vi.Collection)
					}
					for _, ec := range disk.EcShardInfos {
						note(ec.Id, ec.Collection)
					}
				}
			}
		}
	}

	duplicates := make([]DuplicateVolumeId, 0, len(collectionsByVid))
	for vid, set := range collectionsByVid {
		names := make([]string, 0, len(set))
		for name := range set {
			names = append(names, name)
		}
		sort.Strings(names)
		duplicates = append(duplicates, DuplicateVolumeId{VolumeId: vid, Collections: names})
	}
	sort.Slice(duplicates, func(i, j int) bool { return duplicates[i].VolumeId < duplicates[j].VolumeId })
	return duplicates
}
