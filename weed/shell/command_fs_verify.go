package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"math"
	"strings"
	"time"
)

func init() {
	Commands = append(Commands, &commandFsVerify{})
}

type commandFsVerify struct {
	env                  *CommandEnv
	volumeServers        []pb.ServerAddress
	volumeIds            map[uint32][]pb.ServerAddress
	verbose              *bool
	modifyTimeAgoAtSec   int64
	writer               io.Writer
	volumeServerFileIdCh map[string]chan *filer_pb.FileId
}

func (c *commandFsVerify) Name() string {
	return "fs.verify"
}

func (c *commandFsVerify) Help() string {
	return `recursively verify all files under a directory

	fs.verify [-v] [-modifyTimeAgo 1h] /buckets/dir

`
}

func (c *commandFsVerify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	c.env = commandEnv
	c.writer = writer

	fsVerifyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.verbose = fsVerifyCommand.Bool("v", false, "print out each processed files")
	modifyTimeAgo := fsVerifyCommand.Duration("modifyTimeAgo", 0, "only include files after this modify time to verify")
	parallelLimitByVolumeServer := fsVerifyCommand.Int("parallelLimitByVolumeServer", 1, "number of parallel verification per volume server")

	if err = fsVerifyCommand.Parse(args); err != nil {
		return err
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsVerifyCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	c.modifyTimeAgoAtSec = int64(modifyTimeAgo.Seconds())

	if err := c.collectVolumeIds(); err != nil {
		return parseErr
	}

	c.volumeServerFileIdCh = make(map[string]chan *filer_pb.FileId)
	for _, volumeServer := range c.volumeServers {
		fileIdCh := make(chan *filer_pb.FileId, *parallelLimitByVolumeServer)
		errorCh := make(chan error, *parallelLimitByVolumeServer)
		go c.verifyEntry(&volumeServer, fileIdCh, errorCh)
		c.volumeServerFileIdCh[string(volumeServer)] = fileIdCh
		defer close(fileIdCh)
		defer close(errorCh)
	}

	fCount, eConut, terr := c.verifyTraverseBfs(path)
	if terr == nil {
		fmt.Fprintf(writer, "verified %d files, error %d files \n", fCount, eConut)
	}

	return terr

}

func (c *commandFsVerify) collectVolumeIds() error {
	c.volumeIds = make(map[uint32][]pb.ServerAddress)
	topologyInfo, _, err := collectTopologyInfo(c.env, 0)
	if err != nil {
		return err
	}
	eachDataNode(topologyInfo, func(dc string, rack RackId, nodeInfo *master_pb.DataNodeInfo) {
		for _, diskInfo := range nodeInfo.DiskInfos {
			for _, vi := range diskInfo.VolumeInfos {
				c.volumeIds[vi.Id] = append(c.volumeIds[vi.Id], pb.NewServerAddressFromDataNode(nodeInfo))
			}
		}
	})
	return nil
}

func (c *commandFsVerify) verifyEntry(volumeServer *pb.ServerAddress, fileIdCh <-chan *filer_pb.FileId, errorCh chan<- error) {
	for fileId := range fileIdCh {
		err := operation.WithVolumeServerClient(false, *volumeServer, c.env.option.GrpcDialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				_, err := client.VolumeNeedleStatus(context.Background(),
					&volume_server_pb.VolumeNeedleStatusRequest{
						VolumeId: fileId.VolumeId,
						NeedleId: fileId.FileKey})
				return err
			},
		)
		if err != nil && !strings.Contains(err.Error(), storage.ErrorDeleted.Error()) {
			errorCh <- err
		}
		errorCh <- nil
	}
}

type ItemEntry struct {
	chunks []*filer_pb.FileChunk
	path   util.FullPath
}

func (c *commandFsVerify) verifyTraverseBfs(path string) (fileCount int64, errCount int64, err error) {
	timeNowAtSec := time.Now().Unix()
	return fileCount, errCount, doTraverseBfsAndSaving(c.env, c.writer, path, false,
		func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
			if c.modifyTimeAgoAtSec > 0 {
				if entry.Entry.Attributes != nil && c.modifyTimeAgoAtSec < timeNowAtSec-entry.Entry.Attributes.Mtime {
					return nil
				}
			}
			dataChunks, manifestChunks, resolveErr := filer.ResolveChunkManifest(filer.LookupFn(c.env), entry.Entry.GetChunks(), 0, math.MaxInt64)
			if resolveErr != nil {
				return fmt.Errorf("failed to ResolveChunkManifest: %+v", resolveErr)
			}
			dataChunks = append(dataChunks, manifestChunks...)
			if len(dataChunks) > 0 {
				outputChan <- &ItemEntry{
					chunks: dataChunks,
					path:   util.NewFullPath(entry.Dir, entry.Entry.Name),
				}
			}
			return nil
		},
		func(outputChan chan interface{}) {
			for itemEntry := range outputChan {
				i := itemEntry.(*ItemEntry)
				fileMsg := fmt.Sprintf("file:%s needle status ", i.path)
				if *c.verbose {
					fmt.Fprintf(c.writer, fileMsg)
					fileMsg = ""
				}
				for _, chunk := range i.chunks {
					if volumeIds, ok := c.volumeIds[chunk.Fid.VolumeId]; ok {
						for _, volumeServer := range volumeIds {
							if fileIdCh, ok := c.volumeServerFileIdCh[string(volumeServer)]; ok {
								fileIdCh <- chunk.Fid
							} else {
								fmt.Fprintf(c.writer, "%sfailed to get channel for %d:%d: %+v\n",
									fileMsg, chunk.Fid.VolumeId, chunk.Fid.FileKey, err)
							}
							//if err = c.verifyEntry(chunk.Fid, &volumeServer); err != nil {
							//	fmt.Fprintf(c.writer, "%sfailed verify %d:%d: %+v\n",
							//		fileMsg, chunk.Fid.VolumeId, chunk.Fid.FileKey, err)
							//	break
							//}
						}
					} else {
						err = fmt.Errorf("volumeId %d not found", chunk.Fid.VolumeId)
						fmt.Fprintf(c.writer, "%sfailed verify chunk %d:%d: %+v\n",
							fileMsg, chunk.Fid.VolumeId, chunk.Fid.FileKey, err)
						break
					}
				}

				if err != nil {
					errCount++
					continue
				}

				if *c.verbose {
					fmt.Fprintf(c.writer, " verifed\n")
				}
				fileCount++
			}
		})

}
