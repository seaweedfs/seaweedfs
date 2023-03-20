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
	"golang.org/x/exp/slices"
	"io"
	"math"
	"strings"
	"sync"
	"time"
)

func init() {
	Commands = append(Commands, &commandFsVerify{})
}

type commandFsVerify struct {
	env                *CommandEnv
	volumeServers      []pb.ServerAddress
	volumeIds          map[uint32][]pb.ServerAddress
	verbose            *bool
	concurrency        *int
	modifyTimeAgoAtSec int64
	writer             io.Writer
	waitChan           map[string]chan struct{}
	waitChanLock       sync.RWMutex
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
	c.concurrency = fsVerifyCommand.Int("concurrency", 0, "number of parallel verification per volume server")

	if err = fsVerifyCommand.Parse(args); err != nil {
		return err
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsVerifyCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	c.modifyTimeAgoAtSec = int64(modifyTimeAgo.Seconds())
	c.volumeIds = make(map[uint32][]pb.ServerAddress)
	c.waitChan = make(map[string]chan struct{})
	c.volumeServers = []pb.ServerAddress{}

	if err := c.collectVolumeIds(); err != nil {
		return parseErr
	}

	if *c.concurrency > 0 {
		for _, volumeServer := range c.volumeServers {
			volumeServerStr := string(volumeServer)
			c.waitChan[volumeServerStr] = make(chan struct{}, *c.concurrency)
			defer close(c.waitChan[volumeServerStr])
		}
	}

	fCount, eConut, terr := c.verifyTraverseBfs(path)
	if terr == nil {
		fmt.Fprintf(writer, "verified %d files, error %d files \n", fCount, eConut)
	}

	return terr

}

func (c *commandFsVerify) collectVolumeIds() error {
	topologyInfo, _, err := collectTopologyInfo(c.env, 0)
	if err != nil {
		return err
	}
	eachDataNode(topologyInfo, func(dc string, rack RackId, nodeInfo *master_pb.DataNodeInfo) {
		for _, diskInfo := range nodeInfo.DiskInfos {
			for _, vi := range diskInfo.VolumeInfos {
				volumeServer := pb.NewServerAddressFromDataNode(nodeInfo)
				c.volumeIds[vi.Id] = append(c.volumeIds[vi.Id], volumeServer)
				if !slices.Contains(c.volumeServers, volumeServer) {
					c.volumeServers = append(c.volumeServers, volumeServer)
				}
			}
		}
	})
	return nil
}

func (c *commandFsVerify) verifyEntry(volumeServer pb.ServerAddress, fileId *filer_pb.FileId) error {
	err := operation.WithVolumeServerClient(false, volumeServer, c.env.option.GrpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeNeedleStatus(context.Background(),
				&volume_server_pb.VolumeNeedleStatusRequest{
					VolumeId: fileId.VolumeId,
					NeedleId: fileId.FileKey})
			return err
		},
	)
	if err != nil && !strings.Contains(err.Error(), storage.ErrorDeleted.Error()) {
		return err
	}
	return nil
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
				itemPath := string(i.path)
				fileMsg := fmt.Sprintf("file:%s", itemPath)
				errItem := make(map[string]error)
				errItemLock := sync.RWMutex{}
				for _, chunk := range i.chunks {
					if volumeIds, ok := c.volumeIds[chunk.Fid.VolumeId]; ok {
						for _, volumeServer := range volumeIds {
							if *c.concurrency == 0 {
								if err = c.verifyEntry(volumeServer, chunk.Fid); err != nil {
									fmt.Fprintf(c.writer, "%s failed verify needle %d:%d: %+v\n",
										fileMsg, chunk.Fid.VolumeId, chunk.Fid.FileKey, err)
								}
								continue
							}
							c.waitChanLock.RLock()
							waitChan, ok := c.waitChan[string(volumeServer)]
							c.waitChanLock.RUnlock()
							if !ok {
								fmt.Fprintf(c.writer, "%s failed to get channel for %s chunk: %d:%d: %+v\n",
									string(volumeServer), fileMsg, chunk.Fid.VolumeId, chunk.Fid.FileKey, err)
								continue
							}
							waitChan <- struct{}{}
							go func(fId *filer_pb.FileId, path string, volumeServer pb.ServerAddress, msg string) {
								if err = c.verifyEntry(volumeServer, fId); err != nil {
									errItemLock.Lock()
									errItem[path] = err
									fmt.Fprintf(c.writer, "%s failed verify needle %d:%d: %+v\n",
										msg, fId.VolumeId, fId.FileKey, err)
									errItemLock.Unlock()
								}
								<-waitChan
							}(chunk.Fid, itemPath, volumeServer, fileMsg)
						}
					} else {
						err = fmt.Errorf("volumeId %d not found", chunk.Fid.VolumeId)
						fmt.Fprintf(c.writer, "%s  %d:%d: %+v\n",
							fileMsg, chunk.Fid.VolumeId, chunk.Fid.FileKey, err)
						break
					}
				}
				errItemLock.RLock()
				err, _ = errItem[itemPath]
				errItemLock.RUnlock()

				if err != nil {
					errCount++
					continue
				}

				if *c.verbose {
					fmt.Fprintf(c.writer, "%s needles:%d verifed\n", fileMsg, len(i.chunks))
				}
				fileCount++
			}
		})

}
