package shell

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
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

	If -findMissingChunksInFiler is enabled, this works
	in a reverse way:
	1. collect all file ids from all volumes, as set A
	2. collect all file ids from the filer, as set B
	3. find out the set B subtract A

`
}

func (c *commandVolumeFsck) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	findMissingChunksInFiler := fsckCommand.Bool("findMissingChunksInFiler", false, "see \"help volume.fsck\"")
	findMissingChunksInFilerPath := fsckCommand.String("findMissingChunksInFilerPath", "/", "used together with findMissingChunksInFiler")
	applyPurging := fsckCommand.Bool("reallyDeleteFromVolume", false, "<expert only> delete data not referenced by the filer")
	if err = fsckCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.env = commandEnv

	// create a temp folder
	tempFolder, err := os.MkdirTemp("", "sw_fsck")
	if err != nil {
		return fmt.Errorf("failed to create temp folder: %v", err)
	}
	if *verbose {
		fmt.Fprintf(writer, "working directory: %s\n", tempFolder)
	}
	defer os.RemoveAll(tempFolder)

	// collect all volume id locations
	volumeIdToVInfo, err := c.collectVolumeIds(commandEnv, *verbose, writer)
	if err != nil {
		return fmt.Errorf("failed to collect all volume locations: %v", err)
	}

	// collect each volume file ids
	for volumeId, vinfo := range volumeIdToVInfo {
		err = c.collectOneVolumeFileIds(tempFolder, volumeId, vinfo, *verbose, writer)
		if err != nil {
			return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, err)
		}
	}

	if *findMissingChunksInFiler {
		// collect all filer file ids and paths
		if err = c.collectFilerFileIdAndPaths(volumeIdToVInfo, tempFolder, writer, *findMissingChunksInFilerPath, *verbose, applyPurging); err != nil {
			return fmt.Errorf("collectFilerFileIdAndPaths: %v", err)
		}
		// for each volume, check filer file ids
		if err = c.findFilerChunksMissingInVolumeServers(volumeIdToVInfo, tempFolder, writer, *verbose, applyPurging); err != nil {
			return fmt.Errorf("findFilerChunksMissingInVolumeServers: %v", err)
		}
	} else {
		// collect all filer file ids
		if err = c.collectFilerFileIds(tempFolder, volumeIdToVInfo, *verbose, writer); err != nil {
			return fmt.Errorf("failed to collect file ids from filer: %v", err)
		}
		// volume file ids substract filer file ids
		if err = c.findExtraChunksInVolumeServers(volumeIdToVInfo, tempFolder, writer, *verbose, applyPurging); err != nil {
			return fmt.Errorf("findExtraChunksInVolumeServers: %v", err)
		}
	}

	return nil
}

func (c *commandVolumeFsck) collectFilerFileIdAndPaths(volumeIdToServer map[uint32]VInfo, tempFolder string, writer io.Writer, filerPath string, verbose bool, applyPurging *bool) error {

	if verbose {
		fmt.Fprintf(writer, "checking each file from filer ...\n")
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
		cookie  uint32
		path    util.FullPath
	}
	return doTraverseBfsAndSaving(c.env, nil, filerPath, false, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		if verbose && entry.Entry.IsDirectory {
			fmt.Fprintf(writer, "checking directory %s\n", util.NewFullPath(entry.Dir, entry.Entry.Name))
		}
		dChunks, mChunks, resolveErr := filer.ResolveChunkManifest(filer.LookupFn(c.env), entry.Entry.Chunks, 0, math.MaxInt64)
		if resolveErr != nil {
			return nil
		}
		dChunks = append(dChunks, mChunks...)
		for _, chunk := range dChunks {
			outputChan <- &Item{
				vid:     chunk.Fid.VolumeId,
				fileKey: chunk.Fid.FileKey,
				cookie:  chunk.Fid.Cookie,
				path:    util.NewFullPath(entry.Dir, entry.Entry.Name),
			}
		}
		return nil
	}, func(outputChan chan interface{}) {
		buffer := make([]byte, 16)
		for item := range outputChan {
			i := item.(*Item)
			if f, ok := files[i.vid]; ok {
				util.Uint64toBytes(buffer, i.fileKey)
				util.Uint32toBytes(buffer[8:], i.cookie)
				util.Uint32toBytes(buffer[12:], uint32(len(i.path)))
				f.Write(buffer)
				f.Write([]byte(i.path))
				// fmt.Fprintf(writer, "%d,%x%08x %d %s\n", i.vid, i.fileKey, i.cookie, len(i.path), i.path)
			} else {
				fmt.Fprintf(writer, "%d,%x%08x %s volume not found\n", i.vid, i.fileKey, i.cookie, i.path)
			}
		}
	})

}

func (c *commandVolumeFsck) findFilerChunksMissingInVolumeServers(volumeIdToVInfo map[uint32]VInfo, tempFolder string, writer io.Writer, verbose bool, applyPurging *bool) error {

	for volumeId, vinfo := range volumeIdToVInfo {
		checkErr := c.oneVolumeFileIdsCheckOneVolume(tempFolder, volumeId, writer, verbose)
		if checkErr != nil {
			return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, checkErr)
		}
	}
	return nil
}

func (c *commandVolumeFsck) findExtraChunksInVolumeServers(volumeIdToVInfo map[uint32]VInfo, tempFolder string, writer io.Writer, verbose bool, applyPurging *bool) error {
	var totalInUseCount, totalOrphanChunkCount, totalOrphanDataSize uint64
	for volumeId, vinfo := range volumeIdToVInfo {
		inUseCount, orphanFileIds, orphanDataSize, checkErr := c.oneVolumeFileIdsSubtractFilerFileIds(tempFolder, volumeId, writer, verbose)
		if checkErr != nil {
			return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, checkErr)
		}
		totalInUseCount += inUseCount
		totalOrphanChunkCount += uint64(len(orphanFileIds))
		totalOrphanDataSize += orphanDataSize

		if verbose {
			for _, fid := range orphanFileIds {
				fmt.Fprintf(writer, "%s\n", fid)
			}
		}

		if *applyPurging && len(orphanFileIds) > 0 {
			if vinfo.isEcVolume {
				fmt.Fprintf(writer, "Skip purging for Erasure Coded volume %d.\n", volumeId)
				continue
			}
			if vinfo.isReadOnly {
				fmt.Fprintf(writer, "Skip purging for read only volume %d.\n", volumeId)
				continue
			}
			if inUseCount == 0 {
				if err := deleteVolume(c.env.option.GrpcDialOption, needle.VolumeId(volumeId), vinfo.server); err != nil {
					return fmt.Errorf("delete volume %d: %v", volumeId, err)
				}
			} else {
				if err := c.purgeFileIdsForOneVolume(volumeId, orphanFileIds, writer); err != nil {
					return fmt.Errorf("purge for volume %d: %v", volumeId, err)
				}
			}
		}
	}

	if totalOrphanChunkCount == 0 {
		fmt.Fprintf(writer, "no orphan data\n")
		return nil
	}

	if !*applyPurging {
		pct := float64(totalOrphanChunkCount*100) / (float64(totalOrphanChunkCount + totalInUseCount))
		fmt.Fprintf(writer, "\nTotal\t\tentries:%d\torphan:%d\t%.2f%%\t%dB\n",
			totalOrphanChunkCount+totalInUseCount, totalOrphanChunkCount, pct, totalOrphanDataSize)

		fmt.Fprintf(writer, "This could be normal if multiple filers or no filers are used.\n")
	}
	return nil
}

func (c *commandVolumeFsck) collectOneVolumeFileIds(tempFolder string, volumeId uint32, vinfo VInfo, verbose bool, writer io.Writer) error {

	if verbose {
		fmt.Fprintf(writer, "collecting volume %d file ids from %s ...\n", volumeId, vinfo.server)
	}

	return operation.WithVolumeServerClient(false, vinfo.server, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		ext := ".idx"
		if vinfo.isEcVolume {
			ext = ".ecx"
		}

		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Ext:                      ext,
			CompactionRevision:       math.MaxUint32,
			StopOffset:               math.MaxInt64,
			Collection:               vinfo.collection,
			IsEcVolume:               vinfo.isEcVolume,
			IgnoreSourceFileNotFound: false,
		})
		if err != nil {
			return fmt.Errorf("failed to start copying volume %d%s: %v", volumeId, ext, err)
		}

		err = writeToFile(copyFileClient, getVolumeFileIdFile(tempFolder, volumeId))
		if err != nil {
			return fmt.Errorf("failed to copy %d%s from %s: %v", volumeId, ext, vinfo.server, err)
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
	return doTraverseBfsAndSaving(c.env, nil, "/", false, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		dChunks, mChunks, resolveErr := filer.ResolveChunkManifest(filer.LookupFn(c.env), entry.Entry.Chunks, 0, math.MaxInt64)
		if resolveErr != nil {
			if verbose {
				fmt.Fprintf(writer, "resolving manifest chunks in %s: %v\n", util.NewFullPath(entry.Dir, entry.Entry.Name), resolveErr)
			}
			return nil
		}
		dChunks = append(dChunks, mChunks...)
		for _, chunk := range dChunks {
			outputChan <- &Item{
				vid:     chunk.Fid.VolumeId,
				fileKey: chunk.Fid.FileKey,
			}
		}
		return nil
	}, func(outputChan chan interface{}) {
		buffer := make([]byte, 8)
		for item := range outputChan {
			i := item.(*Item)
			util.Uint64toBytes(buffer, i.fileKey)
			files[i.vid].Write(buffer)
		}
	})
}

func (c *commandVolumeFsck) oneVolumeFileIdsCheckOneVolume(tempFolder string, volumeId uint32, writer io.Writer, verbose bool) (err error) {

	if verbose {
		fmt.Fprintf(writer, "find missing file chuns in volume %d ...\n", volumeId)
	}

	db := needle_map.NewMemDb()
	defer db.Close()

	if err = db.LoadFromIdx(getVolumeFileIdFile(tempFolder, volumeId)); err != nil {
		return
	}

	file := getFilerFileIdFile(tempFolder, volumeId)
	fp, err := os.Open(file)
	if err != nil {
		return
	}
	defer fp.Close()

	type Item struct {
		fileKey uint64
		cookie  uint32
		path    util.FullPath
	}

	br := bufio.NewReader(fp)
	buffer := make([]byte, 16)
	item := &Item{}
	var readSize int
	for {
		readSize, err = io.ReadFull(br, buffer)
		if err != nil || readSize != 16 {
			if err == io.EOF {
				return nil
			} else {
				break
			}
		}

		item.fileKey = util.BytesToUint64(buffer[:8])
		item.cookie = util.BytesToUint32(buffer[8:12])
		pathSize := util.BytesToUint32(buffer[12:16])
		pathBytes := make([]byte, int(pathSize))
		n, err := io.ReadFull(br, pathBytes)
		if err != nil {
			fmt.Fprintf(writer, "%d,%x%08x in unexpected error: %v\n", volumeId, item.fileKey, item.cookie, err)
		}
		if n != int(pathSize) {
			fmt.Fprintf(writer, "%d,%x%08x %d unexpected file name size %d\n", volumeId, item.fileKey, item.cookie, pathSize, n)
		}
		item.path = util.FullPath(string(pathBytes))

		if _, found := db.Get(types.NeedleId(item.fileKey)); !found {
			fmt.Fprintf(writer, "%d,%x%08x in %s %d not found\n", volumeId, item.fileKey, item.cookie, item.path, pathSize)
		}

	}

	return

}

func (c *commandVolumeFsck) oneVolumeFileIdsSubtractFilerFileIds(tempFolder string, volumeId uint32, writer io.Writer, verbose bool) (inUseCount uint64, orphanFileIds []string, orphanDataSize uint64, err error) {

	db := needle_map.NewMemDb()
	defer db.Close()

	if err = db.LoadFromIdx(getVolumeFileIdFile(tempFolder, volumeId)); err != nil {
		return
	}

	filerFileIdsData, err := os.ReadFile(getFilerFileIdFile(tempFolder, volumeId))
	if err != nil {
		return
	}

	dataLen := len(filerFileIdsData)
	if dataLen%8 != 0 {
		return 0, nil, 0, fmt.Errorf("filer data is corrupted")
	}

	for i := 0; i < len(filerFileIdsData); i += 8 {
		fileKey := util.BytesToUint64(filerFileIdsData[i : i+8])
		db.Delete(types.NeedleId(fileKey))
		inUseCount++
	}

	var orphanFileCount uint64
	db.AscendingVisit(func(n needle_map.NeedleValue) error {
		// fmt.Printf("%d,%x\n", volumeId, n.Key)
		orphanFileIds = append(orphanFileIds, fmt.Sprintf("%d,%s00000000", volumeId, n.Key.String()))
		orphanFileCount++
		orphanDataSize += uint64(n.Size)
		return nil
	})

	if orphanFileCount > 0 {
		pct := float64(orphanFileCount*100) / (float64(orphanFileCount + inUseCount))
		fmt.Fprintf(writer, "volume:%d\tentries:%d\torphan:%d\t%.2f%%\t%dB\n",
			volumeId, orphanFileCount+inUseCount, orphanFileCount, pct, orphanDataSize)
	}

	return

}

type VInfo struct {
	server     pb.ServerAddress
	collection string
	isEcVolume bool
	isReadOnly bool
}

func (c *commandVolumeFsck) collectVolumeIds(commandEnv *CommandEnv, verbose bool, writer io.Writer) (volumeIdToServer map[uint32]VInfo, err error) {

	if verbose {
		fmt.Fprintf(writer, "collecting volume id and locations from master ...\n")
	}

	volumeIdToServer = make(map[uint32]VInfo)
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	eachDataNode(topologyInfo, func(dc string, rack RackId, t *master_pb.DataNodeInfo) {
		for _, diskInfo := range t.DiskInfos {
			for _, vi := range diskInfo.VolumeInfos {
				volumeIdToServer[vi.Id] = VInfo{
					server:     pb.NewServerAddressFromDataNode(t),
					collection: vi.Collection,
					isEcVolume: false,
					isReadOnly: vi.ReadOnly,
				}
			}
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				volumeIdToServer[ecShardInfo.Id] = VInfo{
					server:     pb.NewServerAddressFromDataNode(t),
					collection: ecShardInfo.Collection,
					isEcVolume: true,
					isReadOnly: true,
				}
			}
		}
	})

	if verbose {
		fmt.Fprintf(writer, "collected %d volumes and locations.\n", len(volumeIdToServer))
	}
	return
}

func (c *commandVolumeFsck) purgeFileIdsForOneVolume(volumeId uint32, fileIds []string, writer io.Writer) (err error) {
	fmt.Fprintf(writer, "purging orphan data for volume %d...\n", volumeId)
	locations, found := c.env.MasterClient.GetLocations(volumeId)
	if !found {
		return fmt.Errorf("failed to find volume %d locations", volumeId)
	}

	resultChan := make(chan []*volume_server_pb.DeleteResult, len(locations))
	var wg sync.WaitGroup
	for _, location := range locations {
		wg.Add(1)
		go func(server pb.ServerAddress, fidList []string) {
			defer wg.Done()

			if deleteResults, deleteErr := operation.DeleteFilesAtOneVolumeServer(server, c.env.option.GrpcDialOption, fidList, false); deleteErr != nil {
				err = deleteErr
			} else if deleteResults != nil {
				resultChan <- deleteResults
			}

		}(location.ServerAddress(), fileIds)
	}
	wg.Wait()
	close(resultChan)

	for results := range resultChan {
		for _, result := range results {
			if result.Error != "" {
				fmt.Fprintf(writer, "purge error: %s\n", result.Error)
			}
		}
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
