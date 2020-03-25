package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandVolumeFsck{})
}

type commandVolumeFsck struct {
	env *CommandEnv
}

func (c *commandVolumeFsck) Name() string {
	return "volume.fsck"
}

func (c *commandVolumeFsck) Help() string {
	return `check all volumes to find entries not used by the filer

	Important assumption!!!
		the system is all used by one filer.

	This command works this way:
	1. collect all file ids from all volumes, as set A
	2. collect all file ids from the filer, as set B
	3. find out the set A subtract B

`
}

func (c *commandVolumeFsck) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	applyPurging := fsckCommand.Bool("reallyDeleteFromVolume", false, "<expert only> delete data not referenced by the filer")
	if err = fsckCommand.Parse(args); err != nil {
		return nil
	}

	c.env = commandEnv

	// create a temp folder
	tempFolder, err := ioutil.TempDir("", "sw_fsck")
	if err != nil {
		return fmt.Errorf("failed to create temp folder: %v", err)
	}
	if *verbose {
		fmt.Fprintf(writer, "working directory: %s\n", tempFolder)
	}
	defer os.RemoveAll(tempFolder)

	// collect all volume id locations
	volumeIdToServer, err := c.collectVolumeIds(*verbose, writer)
	if err != nil {
		return fmt.Errorf("failed to collect all volume locations: %v", err)
	}

	// collect each volume file ids
	for volumeId, vinfo := range volumeIdToServer {
		err = c.collectOneVolumeFileIds(tempFolder, volumeId, vinfo, *verbose, writer)
		if err != nil {
			return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, err)
		}
	}

	// collect all filer file ids
	if err = c.collectFilerFileIds(tempFolder, volumeIdToServer, *verbose, writer); err != nil {
		return fmt.Errorf("failed to collect file ids from filer: %v", err)
	}

	// volume file ids substract filer file ids
	var totalInUseCount, totalOrphanChunkCount, totalOrphanDataSize uint64
	for volumeId, vinfo := range volumeIdToServer {
		inUseCount, orphanChunkCount, orphanDataSize, checkErr := c.oneVolumeFileIdsSubtractFilerFileIds(tempFolder, volumeId, writer, *verbose)
		if checkErr != nil {
			return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, checkErr)
		}
		totalInUseCount += inUseCount
		totalOrphanChunkCount += orphanChunkCount
		totalOrphanDataSize += orphanDataSize
	}

	if totalOrphanChunkCount == 0 {
		fmt.Fprintf(writer, "no orphan data\n")
	}

	pct := float64(totalOrphanChunkCount*100) / (float64(totalOrphanChunkCount + totalInUseCount))
	fmt.Fprintf(writer, "\nTotal\t\tentries:%d\torphan:%d\t%.2f%%\t%dB\n",
		totalOrphanChunkCount+totalInUseCount, totalOrphanChunkCount, pct, totalOrphanDataSize)

	fmt.Fprintf(writer, "This could be normal if multiple filers or no filers are used.\n")

	if *applyPurging {
		fmt.Fprintf(writer, "\nstarting to destroy your data ...\n")
		time.Sleep(30 * time.Second)
		fmt.Fprintf(writer, "just kidding. Not implemented yet.\n")
	}

	return nil
}

func (c *commandVolumeFsck) collectOneVolumeFileIds(tempFolder string, volumeId uint32, vinfo VInfo, verbose bool, writer io.Writer) error {

	if verbose {
		fmt.Fprintf(writer, "collecting volume %d file ids from %s ...\n", volumeId, vinfo.server)
	}

	return operation.WithVolumeServerClient(vinfo.server, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Ext:                      ".idx",
			CompactionRevision:       math.MaxUint32,
			StopOffset:               math.MaxInt64,
			Collection:               vinfo.collection,
			IsEcVolume:               vinfo.isEcVolume,
			IgnoreSourceFileNotFound: false,
		})
		if err != nil {
			return fmt.Errorf("failed to start copying volume %d.idx: %v", volumeId, err)
		}

		err = writeToFile(copyFileClient, getVolumeFileIdFile(tempFolder, volumeId))
		if err != nil {
			return fmt.Errorf("failed to copy %d.idx from %s: %v", volumeId, vinfo.server, err)
		}

		return nil

	})

}

func (c *commandVolumeFsck) collectFilerFileIds(tempFolder string, volumeIdToServer map[uint32]VInfo, verbose bool, writer io.Writer) error {

	if verbose {
		fmt.Fprintf(writer, "collecting file ids from filer ...\n")
	}

	files := make(map[uint32]*os.File)
	for vid := range volumeIdToServer {
		dst, openErr := os.OpenFile(getFilerFileIdFile(tempFolder, vid), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if openErr != nil {
			return fmt.Errorf("failed to create file %s: %v", getFilerFileIdFile(tempFolder, vid), openErr)
		}
		files[vid] = dst
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	type Item struct {
		vid     uint32
		fileKey uint64
	}
	return doTraverseBfsAndSaving(c.env, nil, "/", false, func(outputChan chan interface{}) {
		buffer := make([]byte, 8)
		for item := range outputChan {
			i := item.(*Item)
			util.Uint64toBytes(buffer, i.fileKey)
			files[i.vid].Write(buffer)
		}
	}, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		for _, chunk := range entry.Entry.Chunks {
			outputChan <- &Item{
				vid:     chunk.Fid.VolumeId,
				fileKey: chunk.Fid.FileKey,
			}
		}
		return nil
	})
}

func (c *commandVolumeFsck) oneVolumeFileIdsSubtractFilerFileIds(tempFolder string, volumeId uint32, writer io.Writer, verbose bool) (inUseCount, orphanChunkCount, orphanDataSize uint64, err error) {

	db := needle_map.NewMemDb()
	defer db.Close()

	if err = db.LoadFromIdx(getVolumeFileIdFile(tempFolder, volumeId)); err != nil {
		return
	}

	filerFileIdsData, err := ioutil.ReadFile(getFilerFileIdFile(tempFolder, volumeId))
	if err != nil {
		return
	}

	dataLen := len(filerFileIdsData)
	if dataLen%8 != 0 {
		return 0, 0, 0, fmt.Errorf("filer data is corrupted")
	}

	for i := 0; i < len(filerFileIdsData); i += 8 {
		fileKey := util.BytesToUint64(filerFileIdsData[i : i+8])
		db.Delete(types.NeedleId(fileKey))
		inUseCount++
	}

	db.AscendingVisit(func(n needle_map.NeedleValue) error {
		// fmt.Printf("%d,%x\n", volumeId, n.Key)
		orphanChunkCount++
		orphanDataSize += uint64(n.Size)
		return nil
	})

	if orphanChunkCount > 0 {
		pct := float64(orphanChunkCount*100) / (float64(orphanChunkCount + inUseCount))
		fmt.Fprintf(writer, "volume:%d\tentries:%d\torphan:%d\t%.2f%%\t%dB\n",
			volumeId, orphanChunkCount+inUseCount, orphanChunkCount, pct, orphanDataSize)
	}

	return

}

type VInfo struct {
	server     string
	collection string
	isEcVolume bool
}

func (c *commandVolumeFsck) collectVolumeIds(verbose bool, writer io.Writer) (volumeIdToServer map[uint32]VInfo, err error) {

	if verbose {
		fmt.Fprintf(writer, "collecting volume id and locations from master ...\n")
	}

	volumeIdToServer = make(map[uint32]VInfo)
	var resp *master_pb.VolumeListResponse
	err = c.env.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return
	}

	eachDataNode(resp.TopologyInfo, func(dc string, rack RackId, t *master_pb.DataNodeInfo) {
		for _, vi := range t.VolumeInfos {
			volumeIdToServer[vi.Id] = VInfo{
				server:     t.Id,
				collection: vi.Collection,
				isEcVolume: false,
			}
		}
		for _, ecShardInfo := range t.EcShardInfos {
			volumeIdToServer[ecShardInfo.Id] = VInfo{
				server:     t.Id,
				collection: ecShardInfo.Collection,
				isEcVolume: true,
			}
		}
	})

	if verbose {
		fmt.Fprintf(writer, "collected %d volumes and locations.\n", len(volumeIdToServer))
	}
	return
}

func getVolumeFileIdFile(tempFolder string, vid uint32) string {
	return filepath.Join(tempFolder, fmt.Sprintf("%d.idx", vid))
}

func getFilerFileIdFile(tempFolder string, vid uint32) string {
	return filepath.Join(tempFolder, fmt.Sprintf("%d.fid", vid))
}

func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	dst, err := os.OpenFile(fileName, flags, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receiving %s: %v", fileName, receiveErr)
		}
		dst.Write(resp.FileContent)
	}
	return nil
}
