package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"io"
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

func addToCollection(collectionInfos map[string]*CollectionInfo, vif *master_pb.VolumeInformationMessage) {
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
	cif.VolumeCount++
}

func collectCollectionInfo(t *master_pb.TopologyInfo, collectionInfos map[string]*CollectionInfo) {
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						addToCollection(collectionInfos, vi)
					}
					//for _, ecShardInfo := range diskInfo.EcShardInfos {
					//
					//}
				}
			}
		}
	}
}
