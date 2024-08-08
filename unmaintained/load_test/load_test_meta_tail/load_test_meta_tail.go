package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strconv"
	"strings"
	"time"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	dir           = flag.String("dir", "/tmp", "directory to create files")
	n             = flag.Int("n", 100, "the number of metadata")
	tailFiler     = flag.String("filer", "localhost:8888", "the filer address")
	isWrite       = flag.Bool("write", false, "only write")
	writeInterval = flag.Duration("writeInterval", 0, "write interval, e.g., 1s")
)

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	if *isWrite {
		startGenerateMetadata()
		return
	}

	expected := 0
	startSubscribeMetadata(func(event *filer_pb.SubscribeMetadataResponse) error {
		if event.Directory != *dir {
			return nil
		}
		name := event.EventNotification.NewEntry.Name
		glog.V(0).Infof("=> %s ts:%+v", name, time.Unix(0, event.TsNs))
		id := name[4:]
		if x, err := strconv.Atoi(id); err == nil {
			if x != expected {
				return fmt.Errorf("Expected file%d Actual %s\n", expected, name)
			}
			expected++
		} else {
			return err
		}
		time.Sleep(10 * time.Millisecond)
		return nil
	})

}

func startGenerateMetadata() {
	pb.WithFilerClient(false, util.RandomInt32(), pb.ServerAddress(*tailFiler), grpc.WithTransportCredentials(insecure.NewCredentials()), func(client filer_pb.SeaweedFilerClient) error {

		for i := 0; i < *n; i++ {
			name := fmt.Sprintf("file%d", i)
			glog.V(0).Infof("write %s/%s", *dir, name)
			if err := filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
				Directory: *dir,
				Entry: &filer_pb.Entry{
					Name: name,
				},
			}); err != nil {
				fmt.Printf("create entry %s: %v\n", name, err)
				return err
			}
			if *writeInterval > 0 {
				time.Sleep(*writeInterval)
			}
		}

		return nil

	})
}

func startSubscribeMetadata(eachEntryFunc func(event *filer_pb.SubscribeMetadataResponse) error) {

	prefix := *dir
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "tail",
		ClientId:               0,
		ClientEpoch:            0,
		SelfSignature:          0,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: nil,
		DirectoriesToWatch:     nil,
		StartTsNs:              0,
		StopTsNs:               0,
		EventErrorType:         pb.TrivialOnError,
	}
	tailErr := pb.FollowMetadata(pb.ServerAddress(*tailFiler), grpc.WithTransportCredentials(insecure.NewCredentials()), metadataFollowOption, eachEntryFunc)

	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}
}
