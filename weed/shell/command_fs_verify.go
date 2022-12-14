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
	env                *CommandEnv
	volumeIds          map[uint32][]pb.ServerAddress
	verbose            *bool
	modifyTimeAgoAtSec int64
	writer             io.Writer
}

func (c *commandFsVerify) Name() string {
	return "fs.verify"
}

func (c *commandFsVerify) Help() string {
	return `recursively verify all files under a directory

	fs.verify /some/dir

`
}

func (c *commandFsVerify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	c.env = commandEnv
	c.writer = writer

	fsVerifyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.verbose = fsVerifyCommand.Bool("v", false, "print out each processed files")
	modifyTimeAgo := fsVerifyCommand.Duration("modifyTimeAgo", 0, "only include files after this modify time to verify")

	if modifyTimeAgo.Milliseconds() > 0 {
		c.modifyTimeAgoAtSec = int64(modifyTimeAgo.Seconds())
	}
	if err = fsVerifyCommand.Parse(args); err != nil {
		return err
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsVerifyCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	fCount, terr := c.verifyTraverseBfs(path)

	if terr == nil {
		fmt.Fprintf(writer, "verified %d files\n", fCount)
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

func (c *commandFsVerify) verifyEntry(chunk *Item, volumeServer *pb.ServerAddress) {
	err := operation.WithVolumeServerClient(false, *volumeServer, c.env.option.GrpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeNeedleStatus(context.Background(),
				&volume_server_pb.VolumeNeedleStatusRequest{
					VolumeId: chunk.vid,
					NeedleId: chunk.fileKey})
			return err
		},
	)
	if err != nil && !strings.Contains(err.Error(), storage.ErrorDeleted.Error()) {
		fmt.Fprintf(c.writer, "failed to read %d needle status of file %s: %+v\n", chunk.fileKey, chunk.path, err)
		return
	}
	if *c.verbose {
		fmt.Fprintf(c.writer, "verifed %d needle status of file %s\n", chunk.fileKey, chunk.path)
	}
}

func (c *commandFsVerify) verifyTraverseBfs(path string) (fileCount int64, err error) {
	timeNowAtSec := time.Now().Unix()
	return fileCount, doTraverseBfsAndSaving(c.env, nil, path, false,
		func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
			if c.modifyTimeAgoAtSec > 0 && entry.Entry.Attributes != nil && c.modifyTimeAgoAtSec > timeNowAtSec-entry.Entry.Attributes.Mtime {
				return nil
			}
			dataChunks, manifestChunks, resolveErr := filer.ResolveChunkManifest(filer.LookupFn(c.env), entry.Entry.GetChunks(), 0, math.MaxInt64)
			if resolveErr != nil {
				return fmt.Errorf("failed to ResolveChunkManifest: %+v", resolveErr)
			}
			dataChunks = append(dataChunks, manifestChunks...)
			for _, chunk := range dataChunks {
				outputChan <- &Item{
					vid:     chunk.Fid.VolumeId,
					fileKey: chunk.Fid.FileKey,
					cookie:  chunk.Fid.Cookie,
					path:    util.NewFullPath(entry.Dir, entry.Entry.Name),
				}
			}
			fileCount++
			return nil
		},
		func(outputChan chan interface{}) {
			for item := range outputChan {
				i := item.(*Item)
				for _, volumeServer := range c.volumeIds[i.vid] {
					c.verifyEntry(i, &volumeServer)
				}
			}
		})
}
