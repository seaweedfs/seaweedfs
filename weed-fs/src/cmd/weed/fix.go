package main

import (
	"log"
	"os"
	"path"
	"pkg/storage"
	"strconv"
)

func init() {
	cmdFix.Run = runFix // break init cycle
	cmdFix.IsDebug = cmdFix.Flag.Bool("debug", false, "enable debug mode")
}

var cmdFix = &Command{
	UsageLine: "fix -dir=/tmp -volumeId=234",
	Short:     "run weed tool fix on index file if corrupted",
	Long: `Fix runs the WeedFS fix command to re-create the index .idx file.

  `,
}

var (
	fixVolumePath = cmdFix.Flag.String("dir", "/tmp", "data directory to store files")
	fixVolumeId   = cmdFix.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func runFix(cmd *Command, args []string) bool {

	if *fixVolumeId == -1 {
		return false
	}

	fileName := strconv.Itoa(*fixVolumeId)
	dataFile, e := os.OpenFile(path.Join(*fixVolumePath, fileName+".dat"), os.O_RDONLY, 0644)
	if e != nil {
		log.Fatalf("Read Volume [ERROR] %s\n", e)
	}
	defer dataFile.Close()
	indexFile, ie := os.OpenFile(path.Join(*fixVolumePath, fileName+".idx"), os.O_WRONLY|os.O_CREATE, 0644)
	if ie != nil {
		log.Fatalf("Create Volume Index [ERROR] %s\n", ie)
	}
	defer indexFile.Close()

	dataFile.Seek(0, 0)
	header := make([]byte, storage.SuperBlockSize)
	if _, e := dataFile.Read(header); e != nil {
		log.Fatalf("cannot read superblock: %s", e)
	}

	ver, _, _ := storage.ParseSuperBlock(header)

	n, rest := storage.ReadNeedleHeader(dataFile, ver)
	dataFile.Seek(int64(rest), 1)
	nm := storage.NewNeedleMap(indexFile)
	offset := uint32(storage.SuperBlockSize)
	for n != nil {
		debug("key", n.Id, "volume offset", offset, "data_size", n.Size, "rest", rest)
		if n.Size > 0 {
			count, pe := nm.Put(n.Id, offset/8, n.Size)
			debug("saved", count, "with error", pe)
		}
		offset += rest + 16
		n, rest = storage.ReadNeedleHeader(dataFile, ver)
		dataFile.Seek(int64(rest), 1)
	}
	return true
}
