// Copyright Tamás Gulácsi 2013 All rights reserved
// Use of this source is governed by the same rules as the weed-fs library.
// If this would be ambigous, than Apache License 2.0 has to be used.
//
// dump dumps the files of a volume to tar or unique files.
// Each file will have id#mimetype#original_name file format

package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	// "io"
	"log"
	"os"
	"pkg/storage"
	"strings"
	"time"
)

var (
	volumePath = flag.String("dir", "/tmp", "volume directory")
	volumeId   = flag.Int("id", 0, "volume Id")
	dest       = flag.String("out", "-", "output path. Produces tar if path ends with .tar; creates files otherwise.")
	tarFh      *tar.Writer
	tarHeader  tar.Header
	counter    int
)

func main() {
	var err error

	flag.Parse()

	if *dest == "-" {
		*dest = ""
	}
	if *dest == "" || strings.HasSuffix(*dest, ".tar") {
		var fh *os.File
		if *dest == "" {
			fh = os.Stdout
		} else {
			if fh, err = os.Create(*dest); err != nil {
				log.Printf("cannot open output tar %s: %s", *dest, err)
				return
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

	v, err := storage.NewVolume(*volumePath, storage.VolumeId(*volumeId), storage.CopyNil)
	if v == nil || v.Version() == 0 || err != nil {
		log.Printf("cannot load volume %d from %s (%s): %s", *volumeId, *volumePath, v, err)
		return
	}
	log.Printf("volume: %s (ver. %d)", v, v.Version())
	if err := v.WalkValues(walker); err != nil {
		log.Printf("error while walking: %s", err)
		return
	}

	log.Printf("%d files written.", counter)
}

func walker(n *storage.Needle) (err error) {
	// log.Printf("Id=%d Size=%d Name=%s mime=%s", n.Id, n.Size, n.Name, n.Mime)
	nm := fmt.Sprintf("%d#%s#%s", n.Id, bytes.Replace(n.Mime, []byte{'/'}, []byte{'_'}, -1), n.Name)
	// log.Print(nm)
	if tarFh != nil {
		tarHeader.Name, tarHeader.Size = nm, int64(len(n.Data))
		if err = tarFh.WriteHeader(&tarHeader); err != nil {
			return err
		}
		_, err = tarFh.Write(n.Data)
	} else {
		if fh, e := os.Create(*dest + "/" + nm); e != nil {
			return e
		} else {
			defer fh.Close()
			_, err = fh.Write(n.Data)
		}
	}
	if err == nil {
		counter++
	}
	return
}
