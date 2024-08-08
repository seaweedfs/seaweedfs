package main

import (
	"flag"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"log"
	"time"
	"context"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	util2 "github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/tools/godoc/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	master         = flag.String("master", "localhost:9333", "master server host and port")
	volumeId       = flag.Int("volumeId", -1, "a volume id")
	rewindDuration = flag.Duration("rewind", -1, "rewind back in time. -1 means from the first entry. 0 means from now.")
	timeoutSeconds = flag.Int("timeoutSeconds", 0, "disconnect if no activity after these seconds")
	showTextFile   = flag.Bool("showTextFile", false, "display textual file content")
)

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	util2.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util2.GetViper(), "grpc.client")

	vid := needle.VolumeId(*volumeId)

	var sinceTimeNs int64
	if *rewindDuration == 0 {
		sinceTimeNs = time.Now().UnixNano()
	} else if *rewindDuration == -1 {
		sinceTimeNs = 0
	} else if *rewindDuration > 0 {
		sinceTimeNs = time.Now().Add(-*rewindDuration).UnixNano()
	}

	err := operation.TailVolume(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*master) }, grpcDialOption, vid, uint64(sinceTimeNs), *timeoutSeconds, func(n *needle.Needle) (err error) {
		if n.Size == 0 {
			println("-", n.String())
			return nil
		} else {
			println("+", n.String())
		}

		if *showTextFile {

			data := n.Data
			if n.IsCompressed() {
				if data, err = util2.DecompressData(data); err != nil {
					return err
				}
			}
			if util.IsText(data) {
				println(string(data))
			}

			println("-", n.String(), "compressed", n.IsCompressed(), "original size", len(data))
		}
		return nil
	})

	if err != nil {
		log.Printf("Error VolumeTailSender volume %d: %v", vid, err)
	}

}
