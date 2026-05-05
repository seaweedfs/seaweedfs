package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func init() {
	Commands = append(Commands, &commandCollectionList{})
}

type commandCollectionList struct {
}

func (c *commandCollectionList) Name() string {
	return "collection.list"
}

func (c *commandCollectionList) Help() string {
	return `list all collections`
}

func (c *commandCollectionList) HasTag(CommandTag) bool {
	return false
}

type CollectionInfo struct {
	FileCount        float64
	DeleteCount      float64
	DeletedByteCount float64
	Size             float64
	VolumeCount      int
}

func (c *commandCollectionList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	collections, err := ListCollectionNames(commandEnv, true, true)

	if err != nil {
		return err
	}

	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	collectionInfos := make(map[string]*CollectionInfo)

	collectCollectionInfo(topologyInfo, collectionInfos)

	for _, c := range collections {
		cif, found := collectionInfos[c]
		if !found {
			continue
		}
		fmt.Fprintf(writer, "collection:\"%s\"\tvolumeCount:%d\tsize:%.0f\tfileCount:%.0f\tdeletedBytes:%.0f\tdeletion:%.0f\n", c, cif.VolumeCount, cif.Size, cif.FileCount, cif.DeletedByteCount, cif.DeleteCount)
	}

	fmt.Fprintf(writer, "Total %d collections.\n", len(collections))

	return nil
}

func ListCollectionNames(commandEnv *CommandEnv, includeNormalVolumes, includeEcVolumes bool) (collections []string, err error) {
	var resp *master_pb.CollectionListResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.CollectionList(context.Background(), &master_pb.CollectionListRequest{
			IncludeNormalVolumes: includeNormalVolumes,
			IncludeEcVolumes:     includeEcVolumes,
		})
		return err
	})
	if err != nil {
		return
	}
	for _, c := range resp.Collections {
		collections = append(collections, c.Name)
	}
	return
}

// volumeKey uniquely identifies a volume for per-collection dedupe. Volume
// IDs are scoped to a collection, so we key by (collection, volumeId) to
// avoid cross-collection aliasing if the same numeric ID is ever reused.
type volumeKey struct {
	collection string
	volumeId   uint32
}

// addToCollection folds one replica of a regular volume into the collection
// totals. Size/FileCount/DeleteCount/DeletedByteCount are divided by the
// replication factor so that summing over all replicas yields the whole-
// volume value. VolumeCount is deduped across replicas via seenVolumes so
// it reports logical volumes (same semantics as the S3 bucket metrics
// collector and the EC branch below), not shard/replica presences.
func addToCollection(collectionInfos map[string]*CollectionInfo, seenVolumes map[volumeKey]bool, vif *master_pb.VolumeInformationMessage) {
	c := vif.Collection
	cif, found := collectionInfos[c]
	if !found {
		cif = &CollectionInfo{}
		collectionInfos[c] = cif
	}
	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(vif.ReplicaPlacement))
	copyCount := float64(replicaPlacement.GetCopyCount())
	cif.Size += float64(vif.Size) / copyCount
	cif.DeleteCount += float64(vif.DeleteCount) / copyCount
	cif.FileCount += float64(vif.FileCount) / copyCount
	cif.DeletedByteCount += float64(vif.DeletedByteCount) / copyCount

	key := volumeKey{collection: c, volumeId: vif.Id}
	if !seenVolumes[key] {
		seenVolumes[key] = true
		cif.VolumeCount++
	}
}

// ecCollectionAgg accumulates per-EC-volume counts across the shard holders.
// fileCount is volume-wide (every holder reports the same .ecx count) so it
// is deduped via max; deleteCount is node-local to each .ecj and summed.
type ecCollectionAgg struct {
	collection  string
	fileCount   uint64
	deleteCount uint64
}

func collectCollectionInfo(t *master_pb.TopologyInfo, collectionInfos map[string]*CollectionInfo) {
	seenVolumes := make(map[volumeKey]bool)
	ecVolumes := make(map[volumeKey]*ecCollectionAgg)
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						addToCollection(collectionInfos, seenVolumes, vi)
					}
					for _, esi := range diskInfo.EcShardInfos {
						c := esi.Collection
						cif, found := collectionInfos[c]
						if !found {
							cif = &CollectionInfo{}
							collectionInfos[c] = cif
						}

						// EC shards are node-local, so data-shard sizes sum
						// across nodes to give the logical volume size.
						// Upstream OSS uses the fixed 10+4 ratio; forks with
						// per-volume ratio metadata should pass the
						// configured dataShards value here.
						cif.Size += float64(erasure_coding.EcShardsDataSize(esi, 0))

						key := volumeKey{collection: c, volumeId: esi.Id}
						agg, ok := ecVolumes[key]
						if !ok {
							agg = &ecCollectionAgg{collection: c}
							ecVolumes[key] = agg
							cif.VolumeCount++
						}
						if esi.FileCount > agg.fileCount {
							agg.fileCount = esi.FileCount
						}
						agg.deleteCount += esi.DeleteCount
					}
				}
			}
		}
	}

	for _, agg := range ecVolumes {
		cif := collectionInfos[agg.collection]
		if cif == nil {
			continue
		}
		cif.FileCount += float64(agg.fileCount)
		cif.DeleteCount += float64(agg.deleteCount)
	}
}
