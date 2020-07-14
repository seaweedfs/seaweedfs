package main

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type VolumeFileScanner4SeeDat struct {
	version needle.Version
}

func (scanner *VolumeFileScanner4SeeDat) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil
}

func (scanner *VolumeFileScanner4SeeDat) ReadNeedleBody() bool {
	return true
}

var (
	files     = int64(0)
	filebytes = int64(0)
	diffbytes = int64(0)
)

func Compresssion(data []byte) float64 {
	if len(data) <= 128 {
		return 100.0
	}
	compressed, _ := util.GzipData(data[0:128])
	return float64(len(compressed)*10) / 1280.0
}

func (scanner *VolumeFileScanner4SeeDat) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	t := time.Unix(int64(n.AppendAtNs)/int64(time.Second), int64(n.AppendAtNs)%int64(time.Second))
	glog.V(0).Info("----------------------------------------------------------------------------------")
	glog.V(0).Infof("%d,%s%x offset %d size %d(%s) cookie %x appendedAt %v  hasmime[%t] mime[%s] (len: %d)",
		*volumeId, n.Id, n.Cookie, offset, n.Size, util.BytesToHumanReadable(uint64(n.Size)), n.Cookie, t, n.HasMime(), string(n.Mime), len(n.Mime))
	r, err := gzip.NewReader(bytes.NewReader(n.Data))
	if err == nil {
		buf := bytes.Buffer{}
		h := md5.New()
		c, _ := io.Copy(&buf, r)
		d := buf.Bytes()
		io.Copy(h, bytes.NewReader(d))
		diff := (int64(n.DataSize) - int64(c))
		diffbytes += diff
		glog.V(0).Infof("was gzip! stored_size: %d orig_size: %d diff: %d(%d) mime:%s compression-of-128: %.2f md5: %x", n.DataSize, c, diff, diffbytes, http.DetectContentType(d), Compresssion(d), h.Sum(nil))
	} else {
		glog.V(0).Infof("no gzip!")
	}
	return nil
}

var (
	_                = ioutil.ReadAll
	volumePath       = flag.String("dir", "/tmp", "data directory to store files")
	volumeCollection = flag.String("collection", "", "the volume collection name")
	volumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func main() {
	flag.Parse()
	vid := needle.VolumeId(*volumeId)
	glog.V(0).Info("Starting")
	scanner := &VolumeFileScanner4SeeDat{}
	err := storage.ScanVolumeFile(*volumePath, *volumeCollection, vid, storage.NeedleMapInMemory, scanner)
	if err != nil {
		glog.Fatalf("Reading Volume File [ERROR] %s\n", err)
	}
}
