package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
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

type CollectionInfo struct {
	FileCount        uint64
	DeleteCount      uint64
	DeletedByteCount uint64
	Size             uint64
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
		fmt.Fprintf(writer, "collection:\"%s\"\tvolumeCount:%d\tsize:%d\tfileCount:%d\tdeletedBytes:%d\tdeletion:%d\n", c, cif.VolumeCount, cif.Size, cif.FileCount, cif.DeletedByteCount, cif.DeleteCount)
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
	cif.Size += vif.Size
	cif.DeleteCount += vif.DeleteCount
	cif.FileCount += vif.FileCount
	cif.DeletedByteCount += vif.DeletedByteCount
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
