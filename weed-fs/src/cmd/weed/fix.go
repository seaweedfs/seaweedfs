package main

import (
  "pkg/storage"
  "log"
  "os"
  "path"
  "strconv"
)

func init() {
  cmdFix.Run = runFix // break init cycle
  IsDebug  = cmdFix.Flag.Bool("debug", false, "enable debug mode")
}

var cmdFix = &Command{
  UsageLine: "fix -dir=/tmp -volumeId=234 -debug=1",
  Short:     "run weed tool fix on data file if corrupted",
  Long: `Fix runs the WeedFS fix command on the .dat volume file.

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
  dataFile, e := os.OpenFile(path.Join(*dir, fileName+".dat"), os.O_RDONLY, 0644)
  if e != nil {
    log.Fatalf("Read Volume [ERROR] %s\n", e)
  }
  defer dataFile.Close()
  indexFile, ie := os.OpenFile(path.Join(*dir, fileName+".idx"), os.O_WRONLY|os.O_CREATE, 0644)
  if ie != nil {
    log.Fatalf("Create Volume Index [ERROR] %s\n", ie)
  }
  defer indexFile.Close()

  //skip the volume super block
  dataFile.Seek(storage.SuperBlockSize, 0)

  n, length := storage.ReadNeedle(dataFile)
  nm := storage.NewNeedleMap(indexFile)
  offset := uint32(storage.SuperBlockSize)
  for n != nil {
    if *IsDebug {
      log.Println("key", n.Key, "volume offset", offset, "data_size", n.Size, "length", length)
    }
    if n.Size > 0 {
      count, pe := nm.Put(n.Key, offset/8, n.Size)
      if *IsDebug {
        log.Println("saved", count, "with error", pe)
      }
    }
    offset += length
    n, length = storage.ReadNeedle(dataFile)
  }
  return true
}
