package main

import (
	"archive/tar"
	"fmt"
	"log"
	"os"
	"path"
	"pkg/directory"
	"pkg/storage"
	"strconv"
	"strings"
	"time"
)

func init() {
	cmdExport.Run = runExport // break init cycle
	cmdExport.IsDebug = cmdExport.Flag.Bool("debug", false, "enable debug mode")
}

var cmdExport = &Command{
	UsageLine: "export -dir=/tmp -volumeId=234 -o=/dir/name.tar",
	Short:     "export files out of one volume",
	Long: `export all files in a volume

  `,
}

var (
	exportVolumePath = cmdExport.Flag.String("dir", "/tmp", "input data directory to store volume data files")
	exportVolumeId   = cmdExport.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
	dest             = cmdExport.Flag.String("o", "", "output tar file name")
	tarFh            *tar.Writer
	tarHeader        tar.Header
)

func runExport(cmd *Command, args []string) bool {

	if *exportVolumeId == -1 {
		return false
	}

	var err error
	if strings.HasSuffix(*dest, ".tar") {
		var fh *os.File
		if *dest == "" {
			fh = os.Stdout
		} else {
			if fh, err = os.Create(*dest); err != nil {
				log.Fatalf("cannot open output tar %s: %s", *dest, err)
			}
		}
		defer fh.Close()
		tarFh = tar.NewWriter(fh)
		defer tarFh.Close()
		t := time.Now()
		tarHeader = tar.Header{Mode: 0644,
			ModTime: t, Uid: os.Getuid(), Gid: os.Getgid(),
			Typeflag:   tar.TypeReg,
			AccessTime: t, ChangeTime: t}
	}

	fileName := strconv.Itoa(*exportVolumeId)
	vid := storage.VolumeId(*exportVolumeId)
	indexFile, err := os.OpenFile(path.Join(*exportVolumePath, fileName+".idx"), os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	nm := storage.LoadNeedleMap(indexFile)

	err = storage.ScanVolumeFile(*exportVolumePath, vid, func(superBlock storage.SuperBlock) error {
		return nil
	}, func(n *storage.Needle, offset uint32) error {
		debug("key", n.Id, "offset", offset, "size", n.Size, "disk_size", n.DiskSize(), "gzip", n.IsGzipped())
		nv, ok := nm.Get(n.Id)
		if ok && nv.Size > 0 {
			return walker(vid, n)
		} else {
			if !ok {
				debug("This seems deleted", n.Id)
			} else {
				debug("Id", n.Id, "size", n.Size)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Export Volume File [ERROR] %s\n", err)
	}
	return true
}

func walker(vid storage.VolumeId, n *storage.Needle) (err error) {
	nm := fmt.Sprintf("%s/%d#%s", n.Mime, n.Id, n.Name)
	if n.IsGzipped() && path.Ext(nm) != ".gz" {
		nm = nm + ".gz"
	}
	if tarFh != nil {
		tarHeader.Name, tarHeader.Size = nm, int64(len(n.Data))
		if err = tarFh.WriteHeader(&tarHeader); err != nil {
			return err
		}
		_, err = tarFh.Write(n.Data)
	} else {
		fmt.Printf("key=%s Name=%s Size=%d gzip=%t mime=%s\n",
			directory.NewFileId(vid, n.Id, n.Cookie).String(),
			n.Name,
			n.DataSize,
			n.IsGzipped(),
			n.Mime,
		)
	}
	return
}
