package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"strconv"
	"strings"
)

func init() {
	Commands = append(Commands, &commandFsMetaChangeVolumeId{})
}

type commandFsMetaChangeVolumeId struct {
}

func (c *commandFsMetaChangeVolumeId) Name() string {
	return "fs.meta.changeVolumeId"
}

func (c *commandFsMetaChangeVolumeId) Help() string {
	return `change volume id in existing metadata.

	fs.meta.changeVolumeId -dir=/path/to/a/dir -fromVolumeId=x -toVolumeId=y -force
	fs.meta.changeVolumeId -dir=/path/to/a/dir -mapping=/path/to/mapping/file -force

	The mapping file should have these lines, each line is: [fromVolumeId]=>[toVolumeId]
	e.g.
		1 => 2
		3 => 4

`
}

func (c *commandFsMetaChangeVolumeId) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaChangeVolumeId) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMetaChangeVolumeIdCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	dir := fsMetaChangeVolumeIdCommand.String("dir", "/", "fix all metadata under this folder")
	mappingFileName := fsMetaChangeVolumeIdCommand.String("mapping", "", "a file with multiple volume id changes, with each line as x=>y")
	fromVolumeId := fsMetaChangeVolumeIdCommand.Uint("fromVolumeId", 0, "change metadata with this volume id")
	toVolumeId := fsMetaChangeVolumeIdCommand.Uint("toVolumeId", 0, "change metadata to this volume id")
	isForce := fsMetaChangeVolumeIdCommand.Bool("force", false, "applying the metadata changes")
	if err = fsMetaChangeVolumeIdCommand.Parse(args); err != nil {
		return err
	}

	// load the mapping
	mapping := make(map[needle.VolumeId]needle.VolumeId)
	if *mappingFileName != "" {
		readMappingFromFile(*mappingFileName, mapping)
	} else {
		if *fromVolumeId == *toVolumeId {
			return fmt.Errorf("no volume id changes")
		}
		if *fromVolumeId == 0 || *toVolumeId == 0 {
			return fmt.Errorf("volume id can not be zero")
		}
		mapping[needle.VolumeId(*fromVolumeId)] = needle.VolumeId(*toVolumeId)
	}

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.TraverseBfs(commandEnv, util.FullPath(*dir), func(parentPath util.FullPath, entry *filer_pb.Entry) {
			if !entry.IsDirectory {
				var hasChanges bool
				for _, chunk := range entry.Chunks {
					if chunk.IsChunkManifest {
						fmt.Printf("Change volume id for large file is not implemented yet: %s/%s\n", parentPath, entry.Name)
						return
					}
					chunkVolumeId := chunk.Fid.VolumeId
					if toVolumeId, found := mapping[needle.VolumeId(chunkVolumeId)]; found {
						hasChanges = true
						chunk.Fid.VolumeId = uint32(toVolumeId)
						chunk.FileId = ""
					}
				}
				if hasChanges {
					println("Updating", parentPath, entry.Name)
					if *isForce {
						if updateErr := filer_pb.UpdateEntry(context.Background(), client, &filer_pb.UpdateEntryRequest{
							Directory: string(parentPath),
							Entry:     entry,
						}); updateErr != nil {
							fmt.Printf("failed to update %s/%s: %v\n", parentPath, entry.Name, updateErr)
						}
					}
				}
			}
		})
	})
}

func readMappingFromFile(filename string, mapping map[needle.VolumeId]needle.VolumeId) error {
	mappingFile, openErr := os.Open(filename)
	if openErr != nil {
		return fmt.Errorf("failed to open file %s: %v", filename, openErr)
	}
	defer mappingFile.Close()
	mappingContent, readErr := io.ReadAll(mappingFile)
	if readErr != nil {
		return fmt.Errorf("failed to read file %s: %v", filename, readErr)
	}
	for _, line := range strings.Split(string(mappingContent), "\n") {
		parts := strings.Split(line, "=>")
		if len(parts) != 2 {
			println("unrecognized line:", line)
			continue
		}
		x, errX := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if errX != nil {
			return errX
		}
		y, errY := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
		if errY != nil {
			return errY
		}
		mapping[needle.VolumeId(x)] = needle.VolumeId(y)
	}
	return nil
}
