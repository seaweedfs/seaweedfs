package main

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"strconv"
	"time"
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
	pb.WithFilerClient(false, pb.ServerAddress(*tailFiler), grpc.WithInsecure(), func(client filer_pb.SeaweedFilerClient) error {

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

	tailErr := pb.FollowMetadata(pb.ServerAddress(*tailFiler), grpc.WithInsecure(), "tail", 0, *dir, nil, 0, 0, eachEntryFunc, false)

	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}
}
