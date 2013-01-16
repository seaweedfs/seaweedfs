package main

import (
	"errors"
	"log"
	"os"
	"path"
	"pkg/storage"
	"strconv"
)

func init() {
	cmdFix.Run = runFix // break init cycle
	IsDebug = cmdFix.Flag.Bool("debug", false, "enable debug mode")
}

var cmdFix = &Command{
	UsageLine: "fix -dir=/tmp -volumeId=234 -debug=1",
	Short:     "run weed tool fix on index file if corrupted",
	Long: `Fix runs the WeedFS fix command to re-create the index .idx file.

  `,
}

var (
	dir      = cmdFix.Flag.String("dir", "/tmp", "data directory to store files")
	volumeId = cmdFix.Flag.Int("volumeId", -1, "a non-negative volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func runFix(cmd *Command, args []string) bool {

	if *volumeId == -1 {
		return false
	}

	fileName := strconv.Itoa(*volumeId)

	if err := createIndexFile(path.Join(*dir, fileName+".dat")); err != nil {
		log.Fatalf("[ERROR] " + err.Error())
	}
	return true
}

func createIndexFile(datafn string) error {
	dataFile, e := os.OpenFile(datafn, os.O_RDONLY, 0644)
	if e != nil {
		return errors.New("Read Volume " + e.Error())
	}
	defer dataFile.Close()
	// log.Printf("dataFile=%s", dataFile)
	indexFile, ie := os.OpenFile(datafn[:len(datafn)-4]+".idx", os.O_WRONLY|os.O_CREATE, 0644)
	if ie != nil {
		return errors.New("Create Volume Index " + ie.Error())
	}
	defer indexFile.Close()

	dataFile.Seek(0, 0)
	header := make([]byte, storage.SuperBlockSize)
	if _, e := dataFile.Read(header); e != nil {
		return errors.New("cannot read superblock: " + e.Error())
	}

	ver, _, e := storage.ParseSuperBlock(header)
	if e != nil {
		return errors.New("cannot parse superblock: " + e.Error())
	}

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
	return nil
}
